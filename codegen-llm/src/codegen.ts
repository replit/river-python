import { Codex, type ThreadEvent } from "@openai/codex-sdk";
import { execSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  buildInitialPrompt,
  buildRetryPrompt,
  buildPass2InitialPrompt,
  buildPass2RetryPrompt,
} from "./prompts.js";
import type { CodegenOptions } from "./types.js";
import { VERIFY_SCRIPT } from "./verify-script.js";

// Re-export for library consumers
export type { CodegenOptions } from "./types.js";

/**
 * Run the LLM-based code generation pipeline.
 *
 * Two-pass architecture:
 *
 * **Pass 1 — Correctness:**
 *   Generate Pydantic models that pass schema verification.  Names may be
 *   mechanical.  This is the hard structural problem.
 *
 * **Pass 2 — Quality:**
 *   Aggressively refactor the Pass 1 output: rename classes using TypeScript
 *   source names, deduplicate shared errors, clean up imports.  Must still
 *   pass verification after refactoring.
 *
 * Use `--pass1-only` to stop after Pass 1, or `--pass1-dir` to skip Pass 1
 * and refactor an existing output.
 */
export async function runCodegen(opts: CodegenOptions): Promise<void> {
  // -----------------------------------------------------------------------
  // 1. Validate
  // -----------------------------------------------------------------------
  validatePrerequisites(opts);

  // -----------------------------------------------------------------------
  // 2. Pass 1 — Correctness
  // -----------------------------------------------------------------------
  let pass1WorkDir: string;

  if (opts.pass1Dir) {
    // Skip Pass 1 — use existing output
    log(opts, `Skipping Pass 1 — using existing output: ${opts.pass1Dir}`);
    pass1WorkDir = opts.pass1Dir;
  } else {
    pass1WorkDir = fs.mkdtempSync(path.join(os.tmpdir(), "codegen-llm-"));
    log(opts, `Pass 1 workspace: ${pass1WorkDir}`);

    setupWorkspace(pass1WorkDir, opts);

    const pass1Passed = await runPass1(pass1WorkDir, opts);
    if (!pass1Passed) {
      console.error(
        `\nPass 1 did not pass verification after ${opts.maxAttempts} attempts.`,
      );
      console.error(`Partial output preserved at: ${pass1WorkDir}`);
      // Still copy whatever we have
      copyOutput(pass1WorkDir, opts);
      return;
    }

    // Copy Pass 1 output for reference
    const pass1OutputDir = opts.outputPath + "-pass1";
    copyOutput(pass1WorkDir, { ...opts, outputPath: pass1OutputDir });
    log(opts, `Pass 1 output saved to: ${pass1OutputDir}`);
  }

  if (opts.pass1Only) {
    copyOutput(pass1WorkDir, opts);
    log(opts, `\nPass 1 output written to: ${opts.outputPath} (Pass 2 skipped)`);
    return;
  }

  // -----------------------------------------------------------------------
  // 3. Pass 2 — Quality refactoring
  // -----------------------------------------------------------------------
  const pass2WorkDir = fs.mkdtempSync(path.join(os.tmpdir(), "codegen-llm-p2-"));
  log(opts, `\nPass 2 workspace: ${pass2WorkDir}`);

  try {
    setupPass2Workspace(pass2WorkDir, pass1WorkDir, opts);

    const pass2Passed = await runPass2(pass2WorkDir, opts);

    if (!pass2Passed) {
      console.error(
        `\nPass 2 did not pass verification after ${opts.pass2MaxAttempts ?? opts.maxAttempts} attempts.`,
      );
      console.error("Falling back to Pass 1 output.");
      // Fall back to Pass 1 output
      if (!opts.pass1Dir) {
        copyOutput(pass1WorkDir, opts);
      }
    } else {
      copyOutput(pass2WorkDir, opts);
    }

    log(opts, `\nGenerated package written to: ${opts.outputPath}`);
  } finally {
    log(opts, `Pass 2 workspace preserved at: ${pass2WorkDir}`);
    if (!opts.pass1Dir) {
      log(opts, `Pass 1 workspace preserved at: ${pass1WorkDir}`);
    }
  }
}


/**
 * Run Pass 1 — correctness-focused generation.
 * Returns true if verification passed.
 */
async function runPass1(workDir: string, opts: CodegenOptions): Promise<boolean> {
  log(opts, "\n========== PASS 1: Correctness ==========\n");

  const codex = new Codex(opts.apiKey ? { apiKey: opts.apiKey } : {});

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

  const initialPrompt = buildInitialPrompt(opts);
  let lastVerifyOutput = "";

  for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
    log(opts, `\n--- Pass 1 Attempt ${attempt}/${opts.maxAttempts} ---`);

    const prompt =
      attempt === 1 ? initialPrompt : buildRetryPrompt(lastVerifyOutput);

    if (opts.verbose) {
      log(opts, `Prompt length: ${prompt.length} chars`);
    }

    const { events } = await thread.runStreamed(prompt);

    for await (const event of events) {
      printEvent(event, opts);
    }

    const verifyResult = runVerification(workDir);
    if (verifyResult.passed) {
      log(opts, "\nPass 1 verification PASSED.");
      return true;
    }

    lastVerifyOutput = verifyResult.output;
    log(
      opts,
      `Pass 1 verification failed (${verifyResult.errorCount} errors). ${
        attempt < opts.maxAttempts ? "Retrying..." : "No more attempts."
      }`,
    );
  }

  return false;
}


/**
 * Set up the Pass 2 workspace by copying Pass 1 output + schema + verify.
 */
function setupPass2Workspace(
  pass2WorkDir: string,
  pass1WorkDir: string,
  opts: CodegenOptions,
): void {
  // Copy schema.json — try pass1WorkDir first, fall back to opts.schemaPath
  const pass1Schema = path.join(pass1WorkDir, "schema.json");
  if (fs.existsSync(pass1Schema)) {
    fs.copyFileSync(pass1Schema, path.join(pass2WorkDir, "schema.json"));
  } else {
    // --pass1-dir points to a standalone package; schema comes from CLI arg
    fs.copyFileSync(opts.schemaPath, path.join(pass2WorkDir, "schema.json"));
  }

  // Copy naming_hints.json — try pass1WorkDir, else regenerate
  const hintsPath = path.join(pass1WorkDir, "naming_hints.json");
  if (fs.existsSync(hintsPath)) {
    fs.copyFileSync(hintsPath, path.join(pass2WorkDir, "naming_hints.json"));
  } else {
    // Regenerate from schema
    const namingHints = extractNamingHints(opts.schemaPath);
    fs.writeFileSync(
      path.join(pass2WorkDir, "naming_hints.json"),
      JSON.stringify(namingHints, null, 2),
    );
  }

  // Copy the verify wrapper (it points to the external venv/script, so
  // we need to copy it verbatim)
  const verifyPath = path.join(pass1WorkDir, "verify");
  if (fs.existsSync(verifyPath)) {
    fs.copyFileSync(verifyPath, path.join(pass2WorkDir, "verify"));
    fs.chmodSync(path.join(pass2WorkDir, "verify"), 0o755);
  } else {
    // --pass1-dir was user-supplied; set up fresh verify infrastructure
    setupVerifyInfra(pass2WorkDir, opts);
  }

  // Copy the Pass 1 generated package into generated/
  const pass1Generated = path.join(pass1WorkDir, "generated");
  const pass1OutputFallback = pass1WorkDir; // --pass1-dir might point directly at the package
  const srcDir = fs.existsSync(pass1Generated) ? pass1Generated : pass1OutputFallback;

  const destDir = path.join(pass2WorkDir, "generated");
  fs.cpSync(srcDir, destDir, { recursive: true });

  // Clean up __pycache__ dirs
  cleanPycache(destDir);

  // Git init for Codex
  try {
    execSync("git init && git add -A && git commit -m init --allow-empty", {
      cwd: pass2WorkDir,
      stdio: "pipe",
    });
  } catch {
    // Non-fatal
  }
}


/**
 * Set up verify infrastructure (venv + wrapper) when not inherited from Pass 1.
 */
function setupVerifyInfra(workDir: string, opts: CodegenOptions): void {
  const verifyDir = fs.mkdtempSync(path.join(os.tmpdir(), "codegen-verify-"));
  const verifyScriptPath = path.join(verifyDir, "verify_schema.py");
  fs.writeFileSync(verifyScriptPath, VERIFY_SCRIPT, { mode: 0o755 });

  log(opts, "Creating Python venv with pydantic for verification...");
  const venvDir = path.join(verifyDir, ".venv");
  execSync(`uv venv "${venvDir}"`, { cwd: verifyDir, stdio: "pipe" });
  execSync("uv pip install 'pydantic>=2.9.0'", {
    cwd: verifyDir,
    stdio: "pipe",
    timeout: 120_000,
    env: { ...process.env, VIRTUAL_ENV: venvDir },
  });
  const venvPython = path.join(venvDir, "bin", "python");

  const wrapper = [
    "#!/usr/bin/env bash",
    `exec "${venvPython}" "${verifyScriptPath}" "$@"`,
  ].join("\n");
  fs.writeFileSync(path.join(workDir, "verify"), wrapper, { mode: 0o755 });
}


/** Recursively remove __pycache__ directories. */
function cleanPycache(dir: string): void {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "__pycache__") {
        fs.rmSync(full, { recursive: true });
      } else {
        cleanPycache(full);
      }
    }
  }
}


/**
 * Run Pass 2 — quality-focused refactoring.
 * Returns true if verification passed after refactoring.
 */
async function runPass2(workDir: string, opts: CodegenOptions): Promise<boolean> {
  log(opts, "\n========== PASS 2: Quality Refactoring ==========\n");

  const codex = new Codex(opts.apiKey ? { apiKey: opts.apiKey } : {});
  const maxAttempts = opts.pass2MaxAttempts ?? opts.maxAttempts;

  const thread = codex.startThread({
    model: opts.model,
    sandboxMode: "workspace-write",
    approvalPolicy: "never",
    modelReasoningEffort: opts.effort,
    workingDirectory: workDir,
    skipGitRepoCheck: true,
    additionalDirectories: [opts.serverSrcPath],
  });

  const initialPrompt = buildPass2InitialPrompt({ serverSrcPath: opts.serverSrcPath });
  let lastVerifyOutput = "";

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    log(opts, `\n--- Pass 2 Attempt ${attempt}/${maxAttempts} ---`);

    const prompt =
      attempt === 1 ? initialPrompt : buildPass2RetryPrompt(lastVerifyOutput);

    if (opts.verbose) {
      log(opts, `Prompt length: ${prompt.length} chars`);
    }

    const { events } = await thread.runStreamed(prompt);

    for await (const event of events) {
      printEvent(event, opts);
    }

    const verifyResult = runVerification(workDir);
    if (verifyResult.passed) {
      log(opts, "\nPass 2 verification PASSED.");
      return true;
    }

    lastVerifyOutput = verifyResult.output;
    log(
      opts,
      `Pass 2 verification failed (${verifyResult.errorCount} errors). ${
        attempt < maxAttempts ? "Retrying..." : "No more attempts."
      }`,
    );
  }

  return false;
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

  // Create a venv with pydantic OUTSIDE the workspace so the agent
  // cannot use it to run scaffolding scripts.  Only the verify wrapper
  // knows the path to this Python.
  log(opts, "Creating Python venv with pydantic...");
  const venvDir = path.join(verifyDir, ".venv");
  execSync(`uv venv "${venvDir}"`, { cwd: verifyDir, stdio: "pipe" });
  execSync("uv pip install 'pydantic>=2.9.0'", {
    cwd: verifyDir,
    stdio: "pipe",
    timeout: 120_000,
    env: { ...process.env, VIRTUAL_ENV: venvDir },
  });
  const venvPython = path.join(venvDir, "bin", "python");
  const version = execSync(
    `"${venvPython}" -c "import pydantic; print(pydantic.VERSION)"`,
    { cwd: verifyDir, encoding: "utf8", stdio: ["pipe", "pipe", "pipe"] },
  ).trim();
  log(opts, `Installed pydantic ${version} in verify venv`);

  // Shell wrapper inside workspace — agent calls this but can't see the
  // Python source or the venv.
  const wrapper = [
    "#!/usr/bin/env bash",
    `exec "${venvPython}" "${verifyScriptPath}" "$@"`,
  ].join("\n");
  fs.writeFileSync(path.join(workDir, "verify"), wrapper, { mode: 0o755 });

  // Create the output directory the agent writes into
  fs.mkdirSync(path.join(workDir, "generated"), { recursive: true });

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
