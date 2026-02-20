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
  /** OpenAI API key */
  apiKey: string;
  /** Print verbose output */
  verbose: boolean;
}
