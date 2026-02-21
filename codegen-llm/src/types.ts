export interface CodegenOptions {
  /** Path to the TypeScript River server source directory (contains service definitions) */
  serverSrcPath: string;
  /** Path to the serialised River JSON schema (produced by serializeSchema()) */
  schemaPath: string;
  /** Output directory for the generated Python package */
  outputPath: string;
  /** Optional path to an existing generated Python client to update/replace */
  existingClientPath?: string;
  /** Client class name for the generated top-level wrapper (e.g. "MyServiceClient") */
  clientName: string;
  /** Model name to use */
  model: string;
  /** Model reasoning effort level */
  effort: "low" | "medium" | "high" | "xhigh";
  /** Maximum number of generation + verification attempts */
  maxAttempts: number;
  /** OpenAI API key (optional — if omitted, uses local Codex CLI auth) */
  apiKey?: string;
  /** Print verbose output */
  verbose: boolean;
  /**
   * If set, skip Pass 1 and use this directory as the Pass 1 output.
   * The directory should contain a previously-generated Python package
   * that already passes verification.
   */
  pass1Dir?: string;
  /** Maximum attempts for Pass 2 (quality refactoring). Defaults to maxAttempts. */
  pass2MaxAttempts?: number;
  /** Skip Pass 2 entirely — only run Pass 1. */
  pass1Only?: boolean;
}

/** Options for the Pass 2 quality refactoring prompt builder. */
export interface Pass2Options {
  /** Path to the TypeScript server source (for naming context) */
  serverSrcPath: string;
}
