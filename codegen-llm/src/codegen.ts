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
      sandboxMode: "danger-full-access",
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

function setupWorkspace(workDir: string, opts: CodegenOptions): void {
  // Copy the serialised schema
  fs.copyFileSync(opts.schemaPath, path.join(workDir, "schema.json"));

  // Write the verification script
  fs.writeFileSync(path.join(workDir, "verify_schema.py"), VERIFY_SCRIPT, {
    mode: 0o755,
  });

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

  try {
    const output = execSync(
      ".venv/bin/python verify_schema.py schema.json generated",
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
