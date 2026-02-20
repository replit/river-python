import { Codex, type ThreadEvent } from "@openai/codex-sdk";
import { execSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { buildInitialPrompt, buildRetryPrompt } from "./prompts.js";
import type { CodegenOptions } from "./types.js";
import { VERIFY_SCRIPT } from "./verify-script.js";

// Re-export for library consumers
export type { CodegenOptions } from "./types.js";

/**
 * Run the LLM-based code generation pipeline.
 *
 * 1. Validates prerequisites (Python, pydantic).
 * 2. Sets up a workspace directory with the schema + verification script.
 * 3. Launches a Codex agent session with access to the TypeScript server
 *    source and (optionally) the existing Python client.
 * 4. The agent reads the TypeScript source, generates Pydantic models, and
 *    runs the verification script.
 * 5. If verification fails, feeds errors back and retries (up to maxAttempts).
 * 6. On success, copies the generated package to the output directory.
 */
export async function runCodegen(opts: CodegenOptions): Promise<void> {
  // -----------------------------------------------------------------------
  // 1. Validate
  // -----------------------------------------------------------------------
  validatePrerequisites(opts);

  // -----------------------------------------------------------------------
  // 2. Set up workspace
  // -----------------------------------------------------------------------
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "codegen-llm-"));
  log(opts, `Workspace: ${workDir}`);

  try {
    setupWorkspace(workDir, opts);

    // -----------------------------------------------------------------------
    // 3. Start Codex session
    // -----------------------------------------------------------------------
    const codex = new Codex(opts.apiKey ? { apiKey: opts.apiKey } : {});

    // The agent gets its own workspace as the working directory, with
    // additional read access to the server source (and existing client if
    // provided) so it can inspect the TypeScript definitions.
    const additionalDirs: string[] = [opts.serverSrcPath];
    if (opts.existingClientPath) {
      additionalDirs.push(opts.existingClientPath);
    }

    const thread = codex.startThread({
      model: opts.model,
      sandboxMode: "workspace-write",
      approvalPolicy: "never",
      modelReasoningEffort: opts.effort,
      workingDirectory: workDir,
      skipGitRepoCheck: true,
      additionalDirectories: additionalDirs,
    });

    // -----------------------------------------------------------------------
    // 4. Run generation loop
    // -----------------------------------------------------------------------
    const initialPrompt = buildInitialPrompt(opts);
    let lastVerifyOutput = "";
    let passed = false;

    for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
      log(opts, `\n--- Attempt ${attempt}/${opts.maxAttempts} ---`);

      const prompt =
        attempt === 1 ? initialPrompt : buildRetryPrompt(lastVerifyOutput);

      if (opts.verbose) {
        log(opts, `Prompt length: ${prompt.length} chars`);
      }

      const { events } = await thread.runStreamed(prompt);

      for await (const event of events) {
        printEvent(event, opts);
      }

      // Run verification ourselves to confirm state.
      const verifyResult = runVerification(workDir);
      if (verifyResult.passed) {
        log(opts, "\nVerification PASSED.");
        passed = true;
        break;
      }

      lastVerifyOutput = verifyResult.output;
      log(
        opts,
        `Verification failed (${verifyResult.errorCount} errors). ${
          attempt < opts.maxAttempts ? "Retrying..." : "No more attempts."
        }`,
      );
    }

    if (!passed) {
      console.error(
        `\nCode generation did not pass verification after ${opts.maxAttempts} attempts.`,
      );
      console.error("Partial output preserved for inspection.");
    }

    // -----------------------------------------------------------------------
    // 5. Copy output
    // -----------------------------------------------------------------------
    copyOutput(workDir, opts);
    log(opts, `\nGenerated package written to: ${opts.outputPath}`);
  } finally {
    log(opts, `Workspace preserved at: ${workDir}`);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function validatePrerequisites(opts: CodegenOptions): void {
  // API key is optional — if omitted the Codex CLI will use its locally
  // stored session (from `codex` → "Sign in with ChatGPT").
  if (!opts.apiKey) {
    log(
      opts,
      "No API key provided; relying on local Codex CLI authentication.",
    );
  }

  if (!fs.existsSync(opts.schemaPath)) {
    throw new Error(`Schema file not found: ${opts.schemaPath}`);
  }

  if (!fs.existsSync(opts.serverSrcPath)) {
    throw new Error(
      `Server source directory not found: ${opts.serverSrcPath}`,
    );
  }

  if (
    opts.existingClientPath &&
    !fs.existsSync(opts.existingClientPath)
  ) {
    throw new Error(
      `Existing client directory not found: ${opts.existingClientPath}`,
    );
  }

  // uv must be available (used to manage the workspace venv).
  try {
    execSync("uv --version", {
      encoding: "utf8",
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch {
    throw new Error("uv is required and was not found on PATH.");
  }
}

// ---------------------------------------------------------------------------
// Naming hints extraction
// ---------------------------------------------------------------------------

interface NamingHints {
  /** Maps error code literals (e.g. "NOT_FOUND") to PascalCase class names (e.g. "NotFoundError"). */
  errorCodeToClassName: Record<string, string>;
  /** The four standard River error codes that appear in every procedure. */
  standardErrorCodes: string[];
  /** Maps $kind literal values to suggested variant class name prefixes. */
  kindValueToClassName: Record<string, string>;
}

/**
 * Walk schema.json and extract naming hints the agent should use.
 *
 * This lets us compute correct PascalCase names for error codes and $kind
 * variants mechanically, so the agent doesn't need to implement its own
 * (broken) conversion.
 */
function extractNamingHints(schemaPath: string): NamingHints {
  const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  const errorCodes = new Set<string>();
  const kindValues = new Set<string>();

  // Walk all procedures to find error codes and $kind values
  for (const svc of Object.values(schema.services ?? {})) {
    const service = svc as { procedures: Record<string, Record<string, unknown>> };
    for (const proc of Object.values(service.procedures ?? {})) {
      // Collect error codes
      collectErrorCodes(proc.errors, errorCodes);
      // Collect $kind values from input, output, init
      for (const facet of ["input", "output", "init"]) {
        collectKindValues(proc[facet], kindValues);
      }
    }
  }

  // Build error code → class name map
  const errorCodeToClassName: Record<string, string> = {};
  for (const code of errorCodes) {
    errorCodeToClassName[code] = errorCodeToPascal(code);
  }

  // Build $kind value → class name map
  const kindValueToClassName: Record<string, string> = {};
  for (const kind of kindValues) {
    kindValueToClassName[kind] = kindValueToPascal(kind);
  }

  // Standard error codes (appear in virtually every procedure)
  const standardErrorCodes = [
    "UNCAUGHT_ERROR",
    "UNEXPECTED_DISCONNECT",
    "INVALID_REQUEST",
    "CANCEL",
  ];

  return { errorCodeToClassName, standardErrorCodes, kindValueToClassName };
}

/** Recursively find all `const` values under `properties.code` in error schemas. */
function collectErrorCodes(node: unknown, codes: Set<string>): void {
  if (!node || typeof node !== "object") return;
  const obj = node as Record<string, unknown>;

  // If this is an error variant with properties.code.const
  if (obj.properties && typeof obj.properties === "object") {
    const props = obj.properties as Record<string, unknown>;
    if (props.code && typeof props.code === "object") {
      const codeSchema = props.code as Record<string, unknown>;
      if (typeof codeSchema.const === "string") {
        codes.add(codeSchema.const);
      }
    }
  }

  // Recurse into anyOf / oneOf / allOf
  for (const key of ["anyOf", "oneOf", "allOf"]) {
    if (Array.isArray(obj[key])) {
      for (const item of obj[key] as unknown[]) {
        collectErrorCodes(item, codes);
      }
    }
  }
}

/** Recursively find all `$kind` literal values in schemas. */
function collectKindValues(node: unknown, kinds: Set<string>): void {
  if (!node || typeof node !== "object") return;
  const obj = node as Record<string, unknown>;

  if (obj.properties && typeof obj.properties === "object") {
    const props = obj.properties as Record<string, unknown>;
    if (props["$kind"] && typeof props["$kind"] === "object") {
      const kindSchema = props["$kind"] as Record<string, unknown>;
      if (typeof kindSchema.const === "string") {
        kinds.add(kindSchema.const);
      }
    }
  }

  for (const key of ["anyOf", "oneOf", "allOf", "items"]) {
    const val = obj[key];
    if (Array.isArray(val)) {
      for (const item of val) {
        collectKindValues(item, kinds);
      }
    } else if (val && typeof val === "object") {
      collectKindValues(val, kinds);
    }
  }
}

/**
 * Convert an UPPER_SNAKE_CASE error code to a PascalCase class name.
 * e.g. "NOT_FOUND" → "NotFoundError", "CGROUP_CLEANUP_ERROR" → "CgroupCleanupError"
 */
function errorCodeToPascal(code: string): string {
  // If the code already ends with _ERROR, don't double-suffix
  const stripped = code.replace(/_ERROR$/, "");
  const pascal = stripped
    .split("_")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join("");
  return `${pascal}Error`;
}

/**
 * Convert a $kind value to a PascalCase class name prefix.
 * e.g. "finished" → "Finished", "finalOutput" → "FinalOutput"
 */
function kindValueToPascal(kind: string): string {
  // Already camelCase → just capitalise first letter
  return kind.charAt(0).toUpperCase() + kind.slice(1);
}

function setupWorkspace(workDir: string, opts: CodegenOptions): void {
  // Copy the serialised schema
  fs.copyFileSync(opts.schemaPath, path.join(workDir, "schema.json"));

  // Pre-compute naming hints from the schema so the agent doesn't need to
  // implement PascalCase conversion (which it keeps getting wrong).
  log(opts, "Extracting naming hints from schema...");
  const namingHints = extractNamingHints(opts.schemaPath);
  fs.writeFileSync(
    path.join(workDir, "naming_hints.json"),
    JSON.stringify(namingHints, null, 2),
  );
  log(
    opts,
    `Extracted ${Object.keys(namingHints.errorCodeToClassName).length} error names, ` +
      `${Object.keys(namingHints.kindValueToClassName).length} $kind variant names`,
  );

  // Write the verification script OUTSIDE the workspace so the agent
  // cannot read its source (workspace-write sandbox restricts reads to
  // the workspace + additionalDirectories).  We place a thin shell
  // wrapper inside the workspace that the agent can call.
  const verifyDir = fs.mkdtempSync(path.join(os.tmpdir(), "codegen-verify-"));
  const verifyScriptPath = path.join(verifyDir, "verify_schema.py");
  fs.writeFileSync(verifyScriptPath, VERIFY_SCRIPT, { mode: 0o755 });

  // Shell wrapper inside workspace — agent calls this but can't see the
  // Python source.
  const wrapper = [
    "#!/usr/bin/env bash",
    `exec .venv/bin/python "${verifyScriptPath}" "$@"`,
  ].join("\n");
  fs.writeFileSync(path.join(workDir, "verify"), wrapper, { mode: 0o755 });

  // Create the output directory the agent writes into
  fs.mkdirSync(path.join(workDir, "generated"), { recursive: true });

  // Create a venv with pydantic via uv so that both the agent and our
  // host-side verification have a working Python regardless of whether
  // the system Python is Nix-managed / immutable.
  log(opts, "Creating Python venv with pydantic...");
  execSync("uv venv .venv", { cwd: workDir, stdio: "pipe" });
  execSync("uv pip install 'pydantic>=2.9.0'", {
    cwd: workDir,
    stdio: "pipe",
    timeout: 120_000,
    env: { ...process.env, VIRTUAL_ENV: `${workDir}/.venv` },
  });
  const version = execSync(
    '.venv/bin/python -c "import pydantic; print(pydantic.VERSION)"',
    { cwd: workDir, encoding: "utf8", stdio: ["pipe", "pipe", "pipe"] },
  ).trim();
  log(opts, `Installed pydantic ${version} in workspace venv`);

  // Minimal git init so Codex is happy (belt-and-suspenders alongside
  // skipGitRepoCheck)
  try {
    execSync("git init && git add -A && git commit -m init --allow-empty", {
      cwd: workDir,
      stdio: "pipe",
    });
  } catch {
    // Non-fatal
  }
}

interface VerifyResult {
  passed: boolean;
  output: string;
  errorCount: number;
}

function runVerification(workDir: string): VerifyResult {
  const schemaMapPath = path.join(workDir, "generated", "_schema_map.py");

  if (!fs.existsSync(schemaMapPath)) {
    return {
      passed: false,
      output: "generated/_schema_map.py does not exist yet.",
      errorCount: 1,
    };
  }

  // Use the wrapper script (which calls the external verify_schema.py)
  try {
    const output = execSync(
      "./verify schema.json generated",
      {
        cwd: workDir,
        encoding: "utf8",
        stdio: ["pipe", "pipe", "pipe"],
        timeout: 120_000,
      },
    );
    return { passed: true, output, errorCount: 0 };
  } catch (err: unknown) {
    const e = err as { stdout?: string; stderr?: string };
    const output = [e.stdout ?? "", e.stderr ?? ""].join("\n").trim();
    const errorMatch = output.match(/(\d+) error/);
    const errorCount = errorMatch ? parseInt(errorMatch[1]!, 10) : 1;
    return { passed: false, output, errorCount };
  }
}

function copyOutput(workDir: string, opts: CodegenOptions): void {
  const src = path.join(workDir, "generated");
  if (!fs.existsSync(src)) {
    console.warn("Warning: no generated directory found in workspace.");
    return;
  }

  if (fs.existsSync(opts.outputPath)) {
    fs.rmSync(opts.outputPath, { recursive: true });
  }
  fs.mkdirSync(opts.outputPath, { recursive: true });
  fs.cpSync(src, opts.outputPath, { recursive: true });
}

function log(opts: CodegenOptions, msg: string): void {
  // Always print in verbose mode; otherwise only print non-indented lines
  if (opts.verbose || !msg.startsWith("  ")) {
    console.log(msg);
  }
}

function printEvent(event: ThreadEvent, opts: CodegenOptions): void {
  switch (event.type) {
    case "item.completed": {
      const item = event.item;
      if (item.type === "agent_message") {
        console.log(`\n[agent] ${item.text}\n`);
      } else if (item.type === "command_execution") {
        const cmd = "command" in item ? String(item.command) : "";
        const exit = "exit_code" in item ? item.exit_code : undefined;
        console.log(`[exec] ${cmd}  (exit ${exit ?? "?"})`);
      } else if (item.type === "file_change") {
        const file = "path" in item ? String(item.path) : "";
        console.log(`[file] ${file}`);
      } else if (item.type === "reasoning" && opts.verbose) {
        const text = "text" in item ? String(item.text) : "";
        console.log(`[think] ${text.slice(0, 200)}`);
      }
      break;
    }
    case "turn.completed":
      if (event.usage) {
        console.log(
          `[usage] input: ${event.usage.input_tokens}, output: ${event.usage.output_tokens}`,
        );
      }
      break;
    case "turn.failed":
      console.error(`[error] ${event.error.message}`);
      break;
    default:
      break;
  }
}
