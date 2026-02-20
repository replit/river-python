import type { CodegenOptions } from "./types.js";

/**
 * Build the initial prompt for the Codex agent.
 *
 * The agent has scoped filesystem access to:
 *   - Its workspace (working directory) — schema.json, verify script, venv
 *   - The TypeScript River server source (via additionalDirectories)
 *   - Optionally an existing generated Python client (via additionalDirectories)
 *
 * It does NOT have access to the wider filesystem.
 */
export function buildInitialPrompt(opts: CodegenOptions): string {
  const existingSection = opts.existingClientPath
    ? `
3. **Existing Python client** (reference / replacement target):

       ${opts.existingClientPath}

   Look at it to understand what services and procedures exist, how the
   replit_river Client is used, and what needs improvement. Your generated
   code should be a **clean replacement** for this client.
`
    : "";

  return `
# Task: Generate clean Pydantic v2 types from a River RPC server schema

## Quality standard — READ THIS FIRST

Your output will be evaluated on whether a Python developer can look at the
generated code and immediately understand every type, every field, and every
union variant. The generated types must be as clean and readable as if a
senior engineer hand-wrote them.

**If the output is not clean, readable, and the types are not easily understood
and used by consumers, the output will be DISCARDED and the generation will be
retried from scratch.**

The whole reason we use an LLM instead of a mechanical codegen script is to
produce output that reads like human-written code. A developer should be able
to open any generated file and instantly see:
- What fields a request takes (with their types)
- What a response looks like (with named variants in unions)
- What errors can occur (with their codes and payloads)

If your output consists of opaque wrappers around raw JSON Schema dicts, or
dynamic class factories, or anything that hides the actual type structure from
the developer — it has completely failed its purpose and will be thrown away.


## BANNED patterns — automatic rejection

The verification script scans for these patterns BEFORE comparing schemas.
If ANY are found, verification fails immediately and you must rewrite.

**Banned constructs:**
- \`RootModel\` or \`RootModel[Any]\` — no root models wrapping raw schemas
- \`__get_pydantic_json_schema__\` — no overriding Pydantic's schema generation
- Any function like \`make_schema_model()\` that dynamically creates model
  classes from dicts at runtime
- Embedding raw JSON Schema dicts as Python dict literals in your code
  (e.g. \`INPUT_SCHEMA = {"type": "object", ...}\`)
- \`ClassVar[dict]\` storing JSON schemas on model classes
- \`SchemaAdapter\` or \`make_schema_adapter()\` wrapper classes
- \`create_model()\` from pydantic — no dynamic model creation
- Any "helper" or "utility" that builds models from schema dicts at runtime

**Required:**
- Every Input, Output, and Error type MUST be a concrete \`BaseModel\` subclass
  with explicitly declared, typed fields
- \`TypeAdapter(MyModel).json_schema()\` must produce correct schemas through
  Pydantic's own schema generation — NOT through a hardcoded override


## File access scope

You have access to these locations and ONLY these locations:

1. **Your workspace** (current working directory) — contains \`schema.json\`,
   \`verify_schema.py\`, \`generated/\`, and \`.venv/\`
2. **TypeScript server source**: \`${opts.serverSrcPath}\`
${existingSection}
**Do NOT attempt to browse, read, or access any other directories on the
filesystem.** There is nothing useful elsewhere — everything you need is in
the paths listed above.


## Source of truth

### TypeScript server source (how types are named and organised)

The TypeScript service definitions live at:

    ${opts.serverSrcPath}

Each service is typically in its own subdirectory. Inside you will find
TypeBox schemas using \`Type.Object\`, \`Type.Union\`, \`Type.Literal\`, etc.,
and procedure definitions using \`Procedure.rpc\`, \`Procedure.subscription\`,
\`Procedure.upload\`, \`Procedure.stream\`.

**Read these files** to understand:
- How types are named — use the same names in Python
- What shared/reusable types exist — look for \`schemas.ts\` files, and for
  shared schemas in sibling \`lib/\` directories
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


## Concrete translation examples

Study these patterns carefully — they cover the common cases you will encounter.

### Example 1: Type.Object → BaseModel

TypeScript (TypeBox):
\`\`\`typescript
const ResponseSchema = Type.Object({
  sessionId: Type.String(),
  count: Type.Number(),
  metadata: Type.Object({
    id: Type.Number(),
    username: Type.String(),
  }),
});
\`\`\`

Python (Pydantic):
\`\`\`python
class ResponseMetadata(BaseModel):
    id: float
    username: str

class Response(BaseModel):
    sessionId: str
    count: float
    metadata: ResponseMetadata
\`\`\`

Key points:
- \`Type.Number()\` → \`float\`,  \`Type.Integer()\` → \`int\`,  \`Type.String()\` → \`str\`
- Nested \`Type.Object\` → a separate BaseModel class
- Field names match the TypeScript property names (camelCase is fine in Python
  for JSON interop — do NOT rename to snake_case unless using Field(alias=...))


### Example 2: \`$kind\` discriminated union

TypeScript:
\`\`\`typescript
const ExitInfo = Type.Object({
  $kind: Type.Literal('finished'),
  exitCode: Type.Integer(),
  reason: Type.Union([
    Type.Literal('Errored'),
    Type.Literal('Exited'),
    Type.Literal('Stopped'),
  ]),
});

const OutputChunk = Type.Object({
  $kind: Type.Literal('output'),
  output: Type.Uint8Array(),
});

const ResponseSchema = Type.Union([ExitInfo, OutputChunk]);
\`\`\`

Python:
\`\`\`python
class ExitInfo(BaseModel):
    kind: Literal['finished'] = Field(alias='$kind')
    exitCode: int
    reason: Literal['Errored'] | Literal['Exited'] | Literal['Stopped']
    model_config = ConfigDict(populate_by_name=True)

class OutputChunk(BaseModel):
    kind: Literal['output'] = Field(alias='$kind')
    output: bytes
    model_config = ConfigDict(populate_by_name=True)

Response = Annotated[
    ExitInfo | OutputChunk,
    Field(discriminator='kind'),
]
\`\`\`

Key points:
- \`$kind\` cannot be a Python identifier — use \`Field(alias='$kind')\` with
  a Python name like \`kind\`
- \`model_config = ConfigDict(populate_by_name=True)\` so both names work
- The \`discriminator\` value is the **Python field name**, not the alias
- \`Type.Uint8Array()\` → \`bytes\` in Python
- For a union of Type.Literal string values, use individual Literals joined
  with \`|\`:  \`Literal['a'] | Literal['b'] | Literal['c']\`
  (this produces \`anyOf\` with \`const\` in JSON Schema)


### Example 3: Error union discriminated by \`code\`

TypeScript:
\`\`\`typescript
const FilesystemError = Type.Union([
  Type.Object(
    { code: Type.Literal('NOT_FOUND'), message: Type.String() },
    { description: "File or directory wasn't found." },
  ),
  Type.Object(
    { code: Type.Literal('PERMISSION_DENIED'), message: Type.String() },
  ),
]);
\`\`\`

Python:
\`\`\`python
class NotFoundError(BaseModel):
    code: Literal['NOT_FOUND']
    message: str

class PermissionDeniedError(BaseModel):
    code: Literal['PERMISSION_DENIED']
    message: str

FilesystemError = Annotated[
    NotFoundError | PermissionDeniedError,
    Field(discriminator='code'),
]
\`\`\`

Key points:
- Each error variant gets its own BaseModel with \`code: Literal['...']\`
- Descriptions on TypeBox schemas are metadata — they don't affect structure
- The standard River errors (UNCAUGHT_ERROR, UNEXPECTED_DISCONNECT,
  INVALID_REQUEST, CANCEL) appear in **every** procedure's error union.
  Define them ONCE in \`_errors.py\` and import everywhere.


### Example 4: Optional fields, records, arrays

TypeScript:
\`\`\`typescript
const ConfigSchema = Type.Object({
  name: Type.String(),
  tags: Type.Optional(Type.Array(Type.String())),
  env: Type.Optional(Type.Record(Type.String(), Type.String())),
  port: Type.Optional(Type.Integer()),
  enabled: Type.Boolean(),
});
\`\`\`

Python:
\`\`\`python
class Config(BaseModel):
    name: str
    tags: list[str] | None = None
    env: dict[str, str] | None = None
    port: int | None = None
    enabled: bool
\`\`\`

Key points:
- \`Type.Optional(X)\` → \`X | None = None\`
- \`Type.Array(X)\` → \`list[X]\`
- \`Type.Record(Type.String(), X)\` → \`dict[str, X]\`
- \`Type.Boolean()\` → \`bool\`


### Example 5: Recursive types

TypeScript:
\`\`\`typescript
const SkillSchema = Type.Recursive(
  (This) => Type.Object({
    name: Type.String(),
    description: Type.String(),
    children: Type.Array(This),
  }),
  { $id: 'Skill' },
);
\`\`\`

Python:
\`\`\`python
class Skill(BaseModel):
    name: str
    description: str
    children: list['Skill']

Skill.model_rebuild()   # Required for self-referencing models
\`\`\`


### Example 6: Type.Intersect (allOf) — flatten into one model

TypeScript:
\`\`\`typescript
const BaseSchema = Type.Object({ name: Type.String() });
const ExtendedSchema = Type.Intersect([
  BaseSchema,
  Type.Object({ port: Type.Integer() }),
]);
\`\`\`

Python — merge all properties into a single model:
\`\`\`python
class Extended(BaseModel):
    name: str
    port: int
\`\`\`

TypeBox \`Type.Intersect\` produces \`allOf\` in JSON Schema. In Python, just
merge all properties into one BaseModel. Do NOT use allOf patterns in Pydantic.


## What to generate

Write a complete Python package into: \`generated/\`

### Package structure

\`\`\`
generated/
  __init__.py              # ${opts.clientName} client class + top-level re-exports
  _handshake.py            # HandshakeSchema model (from handshakeSchema in schema.json)
  _errors.py               # Shared River error types: UncaughtError, UnexpectedDisconnectError,
                           #   InvalidRequestError, CancelError, and the StandardRiverError union
  _common.py               # Shared domain types (BaseModel classes) used across multiple services
  <service_name>/
    __init__.py             # Service class with typed async methods
    <procedure_name>.py     # Input, Output, Errors types + TypeAdapters for that procedure
  _schema_map.py            # Verification mapping (see below)
\`\`\`

**Important:** \`_common.py\` must contain ONLY shared BaseModel classes — NOT
utility functions, schema helpers, or dynamic class factories.


### Design principles

1. **Mirror TypeScript naming.** Read the TypeBox definitions and use the same
   names. If TypeScript has \`CreateOptionsSchema\`, your Python should have
   \`CreateOptions\`. If a field references \`ServiceInputSchema\`, name your
   model \`ServiceInput\`.

2. **Reuse shared types.** The four standard River errors appear in every
   procedure's error union. Define them ONCE in \`_errors.py\` and import them.
   Same for any domain type used across services — put it in \`_common.py\`.

3. **BaseModel everywhere.** Every type must be a \`BaseModel\` subclass with
   explicitly typed fields. No TypedDict. No RootModel. No dynamic creation.

4. **Discriminated unions.** Use
   \`Annotated[A | B, Field(discriminator='field')]\` where possible.
   For the \`$kind\` pattern, use \`Field(alias='$kind')\` on each variant.

5. **TypeAdapters.** Each procedure module must export typed adapters:
   \`\`\`python
   InputAdapter: TypeAdapter[InputModel] = TypeAdapter(InputModel)
   OutputAdapter: TypeAdapter[OutputModel] = TypeAdapter(OutputModel)
   ErrorsAdapter: TypeAdapter[ErrorsUnion] = TypeAdapter(ErrorsUnion)
   \`\`\`

6. **Service classes.** Each service wraps a River client and exposes typed
   async methods:
   \`\`\`python
   class MyService:
       def __init__(self, client: Any):
           self.client = client

       async def my_rpc(self, input: MyInput, timeout: timedelta) -> MyOutput:
           return await self.client.send_rpc(
               "serviceName", "procName", input,
               lambda x: InputAdapter.dump_python(
                   InputAdapter.validate_python(x), by_alias=True, exclude_none=True),
               lambda x: OutputAdapter.validate_python(x),
               lambda x: ErrorsAdapter.validate_python(x),
               timeout,
           )
   \`\`\`
   For subscriptions (no timeout param), streams and uploads (init + data
   stream params), adapt the method signatures accordingly.

7. **String literal unions.** For TypeBox
   \`Type.Union([Type.Literal('a'), Type.Literal('b')])\`, use individual
   Literals joined with \`|\`:  \`Literal['a'] | Literal['b']\`.
   This produces \`anyOf\` with \`const\` entries in the JSON Schema.
   Do NOT use \`Literal['a', 'b']\` — that produces \`enum\` instead of
   \`anyOf\`, which may not match.

8. **Intersections.** For \`Type.Intersect([A, B])\`, flatten all properties
   into a single BaseModel. Do NOT try to represent \`allOf\` in Pydantic.


### The _schema_map.py module

This is **critical for verification**. It must export \`SCHEMA_MAP\`:

\`\`\`python
from pydantic import TypeAdapter
# Import adapters from each procedure module...

SCHEMA_MAP: dict = {
    "<serviceName>": {
        "procedures": {
            "<procName>": {
                "input": TypeAdapter(<InputModel>),
                "output": TypeAdapter(<OutputModel>),
                "errors": TypeAdapter(<ErrorsUnion>),
                "type": "rpc",
            }
        }
    },
    # ... every service and every procedure
}
\`\`\`

Every service and every procedure from schema.json must be represented.
The TypeAdapter wrappers let the verification script call \`.json_schema()\`
and compare against the original.


## Verification

After generating all files, run:

    .venv/bin/python verify_schema.py schema.json generated

A Python venv with pydantic is already set up at \`.venv/\`.
Always use \`.venv/bin/python\` to run Python.

The verification script runs two checks:

1. **Code quality check**: Scans all generated \`.py\` files for banned
   patterns (RootModel, make_schema_model, schema overrides, raw JSON Schema
   dicts, etc.). If any are found, it fails immediately with detailed messages
   telling you which files contain which banned patterns.

2. **Schema comparison**: Loads schema.json and compares it against
   \`TypeAdapter(Model).json_schema()\` output for every procedure, after
   normalising both sides (stripping metadata, resolving refs, normalising
   nullable types, handling Uint8Array, sorting unions).

Exit codes: 0 = success, 1 = mismatches, 2 = import error or banned patterns.

**If it fails, read the errors carefully, fix the models, and re-run.
Repeat until verification passes.**

### Known schema quirks the verifier handles

- Pydantic adds \`title\` fields — verifier strips them
- Pydantic uses \`$ref\`/\`$defs\` — verifier inlines them
- Pydantic's \`X | None\` produces \`anyOf: [X, null]\` — verifier strips the
  null variant (TypeBox Optional just means "not required", not nullable)
- TypeBox includes \`"type": "string"\` alongside \`"const": "foo"\` — verifier
  strips \`type\` when \`const\` is present
- TypeBox uses \`"type": "Uint8Array"\` — verifier normalises to \`"string"\`
- Pydantic discriminated unions emit a \`discriminator\` key — verifier strips it
- TypeBox \`additionalProperties\` — verifier strips it
- Pydantic \`Literal['a', 'b']\` produces \`enum\` — verifier normalises
  \`enum\` to \`anyOf\` with individual \`const\` entries

Because of these normalisations, use \`bytes\` for Uint8Array fields and your
preferred Literal style for string unions. The verifier handles the rest.


## How to approach this

**Take your time.** There are many services and many procedures. Work through
them methodically, one service at a time. This is a LARGE task and it is
expected to take a long time. Quality matters more than speed.

Do NOT try to be clever:
- Do NOT write a meta-generator or codegen script
- Do NOT write a utility that reads JSON and produces classes dynamically
- Do NOT create "helper functions" that build models from schema dicts
- Do NOT look for existing codegen tools or utilities on the filesystem
- DO read each service's TypeScript source, understand the types, and write
  clean Pydantic BaseModel classes with explicit typed fields

You ARE the code generator. Read the TypeScript. Write the Python. Every model
must have real fields that a developer can see and understand.


## Step-by-step process

### Phase 1: Understand the landscape

1. Read the service registry in the TypeScript source to get the full list
   of services.
2. Read 3–4 representative services (pick ones with varied procedure types:
   rpc, subscription, upload, stream) to learn the patterns.
3. Inspect a few services in schema.json via \`jq\` to see the JSON Schema
   structure the verifier compares against.

### Phase 2: Shared types

4. Write \`_errors.py\` with the four standard River error models
   (UncaughtError, UnexpectedDisconnectError, InvalidRequestError, CancelError)
   and the \`StandardRiverError\` discriminated union.
5. Scan the TypeScript source for domain types reused across multiple
   services. Write them as BaseModel classes in \`_common.py\`.

### Phase 3: Service-by-service generation

6. For **each** service, one at a time:
   a. Read the TypeScript source for that service (\`index.ts\`,
      \`schemas.ts\`, etc. in its directory).
   b. Read the corresponding JSON Schema via
      \`jq '.services.<serviceName>' schema.json\`.
   c. Write Pydantic models for each procedure in that service,
      naming them after the TypeScript schema definitions.
   d. Write the service class (\`__init__.py\`) with typed methods.

   Do this for every single service. Do not skip any.

### Phase 4: Assembly and verification

7. Write \`_schema_map.py\` covering every service and procedure.
8. Write the top-level \`__init__.py\` with the \`${opts.clientName}\` client class.
9. Run \`.venv/bin/python verify_schema.py schema.json generated\`
10. If it fails, read the errors, fix the models, and re-run.
    Repeat until verification passes.


## Important notes

- schema.json is large. Don't try to read it all at once.
  Use \`jq\`, \`head\`, \`grep\`, or read specific services.
- \`jq '.services | keys' schema.json\` lists all service names.
- \`jq '.services.<serviceName>' schema.json\` inspects a specific service.
- When the verifier reports mismatches, look at both the original JSON Schema
  (from schema.json) and what your Pydantic model produces
  (\`TypeAdapter(Model).json_schema()\`) to understand the difference.
`.trim();
}

/**
 * Build a retry prompt when verification failed.
 */
export function buildRetryPrompt(verificationOutput: string): string {
  return `
The verification script failed. Here is its output:

\`\`\`
${verificationOutput}
\`\`\`

Read the error messages carefully and fix every issue:

- **If there are "BANNED PATTERN" errors:** you used a shortcut that is not
  allowed. You must rewrite those files with concrete BaseModel subclasses
  that declare typed fields. No RootModel, no dynamic class creation, no raw
  JSON Schema dicts embedded in Python code.

- **If there are schema mismatches:** compare the original JSON Schema
  (from schema.json via \`jq\`) against what your Pydantic model's
  \`TypeAdapter.json_schema()\` produces. Common fixes:
  - Flatten \`Type.Intersect\` / \`allOf\` into a single BaseModel
  - Use \`bytes\` for Uint8Array fields
  - Check that required vs optional fields match the original
  - Check that field types match (float vs int, str vs bool, etc.)

After fixing, re-run:

    .venv/bin/python verify_schema.py schema.json generated

Keep fixing and re-running until verification passes.

Remember: if the output is not clean, readable, and the types are not easily
understood and used by Python consumers, the output will be discarded and the
entire generation retried from scratch.
`.trim();
}
