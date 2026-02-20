import type { CodegenOptions } from "./types.js";

/**
 * Build the initial prompt for the Codex agent.
 *
 * The agent has filesystem access to:
 *   - The TypeScript River server source (service definitions in TypeBox)
 *   - The serialised JSON schema (ground truth for verification)
 *   - Optionally an existing generated Python client
 *   - A workspace with the verification script
 */
export function buildInitialPrompt(opts: CodegenOptions): string {
  const existingSection = opts.existingClientPath
    ? `
### Existing Python client (reference / replacement target)

An existing auto-generated Python client lives at:

    ${opts.existingClientPath}

Look at it to understand:
- What services and procedures exist
- How the replit_river Client is used (send_rpc, send_subscription, etc.)
- What needs improvement (naming, type reuse, structure)

Your generated code should be a **clean replacement** for this client.
`
    : "";

  return `
# Task: Generate Pydantic models from a River RPC server schema

You are generating a Python Pydantic v2 client package for a **River** RPC
server.  The server is written in TypeScript using \`@replit/river\` with
\`@sinclair/typebox\` for schema definitions.  Your job is to produce clean,
well-structured Pydantic BaseModel classes that mirror the TypeScript type
naming and composition.

## Source of truth

### TypeScript server source (how types are named and organised)

The TypeScript service definitions live at:

    ${opts.serverSrcPath}

Each service is typically in its own subdirectory.  Inside you'll find TypeBox
schemas using \`Type.Object\`, \`Type.Union\`, \`Type.Literal\`, etc., and
procedure definitions using \`Procedure.rpc\`, \`Procedure.subscription\`,
\`Procedure.upload\`, \`Procedure.stream\`.

**Read these files** to understand:
- How types are named (use the same names in Python)
- What shared/reusable types exist
- How types compose together
- How services and procedures are structured

Look for an \`index.ts\` that registers all services — this gives you the
complete list of service names and their mapping.

### Serialised JSON schema (ground truth for verification)

    schema.json  (in the workspace, copied from ${opts.schemaPath})

This is the JSON output of River's \`serializeSchema(services)\`.  Structure:
\`\`\`json
{
  "handshakeSchema": { ... },
  "services": {
    "<serviceName>": {
      "procedures": {
        "<procName>": {
          "input": { ...JSON Schema... },
          "output": { ...JSON Schema... },
          "errors": { "anyOf": [...] },
          "type": "rpc" | "subscription" | "upload" | "stream"
        }
      }
    }
  }
}
\`\`\`
${existingSection}
## What to generate

Write a complete Python package into:

    generated/

### Package structure

\`\`\`
generated/
  __init__.py              # ${opts.clientName} client class + top-level re-exports
  _handshake.py            # HandshakeSchema model (if handshakeSchema is present)
  _errors.py               # Shared River error types (UNCAUGHT_ERROR, UNEXPECTED_DISCONNECT,
                           #   INVALID_REQUEST, CANCEL — these appear in every procedure)
  _common.py               # Shared domain types used across multiple services
  <service_name>/
    __init__.py             # Service class with typed RPC methods
    <procedure_name>.py     # Input, Output, Errors types + TypeAdapters for that procedure
  _schema_map.py            # Verification mapping (see below)
\`\`\`

### Design principles

1. **Mirror the TypeScript naming.**  Read the TypeBox definitions and use the
   same names for your Pydantic models.  If TypeScript has a schema called
   \`CreateOptionsSchema\` with a field typed \`ServiceInputSchema\`, your
   Python should have \`CreateOptions\` with field \`services: list[ServiceInput]\`.

2. **Reuse shared types.**  The four standard River errors (\`UNCAUGHT_ERROR\`,
   \`UNEXPECTED_DISCONNECT\`, \`INVALID_REQUEST\`, \`CANCEL\`) appear in every
   procedure's error union.  Define them once in \`_errors.py\` and import.
   Same for any domain type used across services — put it in \`_common.py\`.

3. **Use Pydantic v2 BaseModel everywhere.**  No TypedDict.  Use
   \`Field(alias=...)\` for fields with special characters (e.g. \`$kind\`).
   Use \`model_config = ConfigDict(populate_by_name=True)\` so both the alias
   and the Python name work.

4. **Discriminated unions.**  Use
   \`Annotated[Union[A, B], Field(discriminator="field")]\` where possible.
   For the \`$kind\` pattern common in River schemas, use
   \`Field(alias="$kind")\` on each variant's literal field.

5. **TypeAdapters.**  For each procedure, create TypeAdapters:
   \`\`\`python
   InputAdapter: TypeAdapter[InputModel] = TypeAdapter(InputModel)
   OutputAdapter: TypeAdapter[OutputModel] = TypeAdapter(OutputModel)
   ErrorsAdapter: TypeAdapter[ErrorsUnion] = TypeAdapter(ErrorsUnion)
   \`\`\`

6. **Service classes.**  Each service class wraps \`replit_river.Client\` and
   exposes typed async methods.  Pattern:
   \`\`\`python
   async def my_method(self, input: MyInput, timeout: timedelta) -> MyOutput:
       return await self.client.send_rpc(
           "serviceName", "methodName", input,
           lambda x: InputAdapter.dump_python(x, by_alias=True, exclude_none=True),
           lambda x: OutputAdapter.validate_python(x),
           lambda x: ErrorsAdapter.validate_python(x),
           timeout,
       )
   \`\`\`

7. **Enums and literals.**  For string enums with known values, use
   \`Literal["a", "b", "c"]\` or \`Enum\` as appropriate.  Output types should
   handle unknown values gracefully for forward compatibility.

### The _schema_map.py module

This is **critical for verification**.  It must export \`SCHEMA_MAP\`:

\`\`\`python
from pydantic import TypeAdapter
# import all your generated types...

SCHEMA_MAP: dict = {
    "<serviceName>": {
        "procedures": {
            "<procName>": {
                "input": TypeAdapter(<InputModel>),
                "output": TypeAdapter(<OutputModel>),
                "errors": TypeAdapter(<ErrorsUnion>),   # or None
                "type": "rpc",
            }
        }
    },
    # ... every service and every procedure
}
\`\`\`

Every service and every procedure from schema.json must be represented.  The
TypeAdapter wrappers let the verification script call \`.json_schema()\` and
compare against the original.

## Verification

After generating all files, run:

    python verify_schema.py schema.json generated

This script:
1. Loads the original schema.json
2. Imports your \`_schema_map.py\`
3. For each procedure, compares \`TypeAdapter(...).json_schema()\` output
   against the original, after normalising both sides (stripping
   title/description/$id, inlining $ref/$defs, normalising nullable types,
   sorting unions).
4. Exits 0 on success, 1 with detailed diff output on failure.

**If it fails, read the errors carefully and fix the mismatches.  Then re-run.**

## Process

1. Start by reading the service registry in the TypeScript source to get the
   full list of services.
2. Read the TypeScript source for a few representative services to understand
   patterns (pick ones with varied procedure types: rpc, subscription, stream).
3. Read the serialised schema.json to see the exact JSON Schema structure
   (use \`head\`, \`jq\`, etc. — the file may be large).
4. Identify shared types that appear across services.
5. Generate \`_errors.py\` and \`_common.py\` first.
6. Generate each service's types, reading the corresponding TypeScript source
   for naming guidance.
7. Generate \`_schema_map.py\`.
8. Generate the top-level \`__init__.py\` with the \`${opts.clientName}\` client class.
9. Run \`python verify_schema.py schema.json generated\`
10. Fix any errors and re-run until verification passes.

## Important notes

- The JSON schema file may be very large.  Don't try to read it all at once.
  Use \`jq\`, \`head\`, \`grep\`, or read specific services.
- Use \`jq '.services | keys' schema.json\` to list all service names.
- Use \`jq '.services.<serviceName>' schema.json\` to inspect a specific service.
- When the verification script reports mismatches, look at both the original
  JSON Schema (from schema.json) and what your Pydantic model produces
  (\`TypeAdapter(Model).json_schema()\`) to understand the difference.
- Pydantic adds \`title\` fields — the verification script strips these.
- Pydantic uses \`$defs\`/\`$ref\` — the verification script inlines these.
- For \`Optional[X]\`, Pydantic produces
  \`{"anyOf": [{...X...}, {"type": "null"}]}\` — the verification script
  normalises this.
`.trim();
}

/**
 * Build a retry prompt when verification failed.
 */
export function buildRetryPrompt(verificationOutput: string): string {
  return `
The verification script failed.  Here is its output:

\`\`\`
${verificationOutput}
\`\`\`

Please read the error messages carefully, fix the generated Pydantic models
to resolve every mismatch, and then re-run:

    python verify_schema.py schema.json generated

Keep fixing and re-running until verification passes.
`.trim();
}
