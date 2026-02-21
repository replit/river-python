#!/usr/bin/env node

import { Command } from "commander";
import * as path from "node:path";
import { runCodegen } from "./codegen.js";
import type { CodegenOptions } from "./types.js";

const program = new Command();

program
  .name("codegen-llm")
  .description(
    "LLM-based Pydantic type generation from River TypeScript server schemas.\n\n" +
      "Points a Codex agent at a TypeScript River server's source code to read\n" +
      "how types are named and organised, then generates matching Pydantic v2\n" +
      "models.  Verifies correctness by comparing generated JSON schemas\n" +
      "against the serialised River schema.",
  )
  .version("0.1.0");

program
  .command("generate")
  .description(
    "Generate a Python Pydantic client from a TypeScript River server.\n\n" +
      "Runs a two-pass pipeline:\n" +
      "  Pass 1: Generate schema-correct models (names may be mechanical)\n" +
      "  Pass 2: Aggressively refactor for quality (TS-derived names, dedup)\n\n" +
      "Use --pass1-only to stop after Pass 1, or --pass1-dir to skip Pass 1\n" +
      "and refactor an existing output.",
  )
  .requiredOption(
    "--server-src <path>",
    "Path to the TypeScript River server source (directory containing service definitions)",
  )
  .requiredOption(
    "--schema <path>",
    "Path to the serialised River JSON schema (output of serializeSchema())",
  )
  .requiredOption(
    "--output <path>",
    "Output directory for the generated Python package",
  )
  .option(
    "--existing-client <path>",
    "Path to an existing generated Python client to reference/replace",
  )
  .option(
    "--client-name <name>",
    "Name for the generated top-level client class",
    "RiverClient",
  )
  .option(
    "--model <name>",
    "Codex model to use",
    "gpt-5.3-codex-api-preview",
  )
  .option(
    "--effort <level>",
    "Model reasoning effort (low, medium, high, xhigh)",
    "xhigh",
  )
  .option(
    "--max-attempts <n>",
    "Maximum generation + verification attempts (Pass 1)",
    "3",
  )
  .option(
    "--pass2-max-attempts <n>",
    "Maximum verification attempts for Pass 2 (defaults to --max-attempts)",
  )
  .option(
    "--pass1-dir <path>",
    "Skip Pass 1 — use this directory as pre-generated Pass 1 output",
  )
  .option(
    "--pass1-only",
    "Only run Pass 1 (correctness), skip Pass 2 (quality refactoring)",
    false,
  )
  .option(
    "--api-key <key>",
    "OpenAI API key (or set OPENAI_API_KEY / CODEX_API_KEY env var)",
  )
  .option("--verbose", "Enable verbose output", false)
  .action(async (rawOpts) => {
    const opts: CodegenOptions = {
      serverSrcPath: path.resolve(rawOpts.serverSrc),
      schemaPath: path.resolve(rawOpts.schema),
      outputPath: path.resolve(rawOpts.output),
      existingClientPath: rawOpts.existingClient
        ? path.resolve(rawOpts.existingClient)
        : undefined,
      clientName: rawOpts.clientName,
      model: rawOpts.model,
      effort: rawOpts.effort,
      maxAttempts: parseInt(rawOpts.maxAttempts, 10),
      pass2MaxAttempts: rawOpts.pass2MaxAttempts
        ? parseInt(rawOpts.pass2MaxAttempts, 10)
        : undefined,
      pass1Dir: rawOpts.pass1Dir
        ? path.resolve(rawOpts.pass1Dir)
        : undefined,
      pass1Only: rawOpts.pass1Only,
      apiKey:
        rawOpts.apiKey ||
        process.env.OPENAI_API_KEY ||
        process.env.CODEX_API_KEY ||
        undefined,
      verbose: rawOpts.verbose,
    };

    try {
      await runCodegen(opts);
    } catch (err) {
      console.error(
        "Error:",
        err instanceof Error ? err.message : String(err),
      );
      process.exit(1);
    }
  });

program.parse();
