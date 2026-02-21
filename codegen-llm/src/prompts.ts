import type { CodegenOptions, Pass2Options } from "./types.js";

/**
 * Build the initial prompt for the Codex agent.
 *
 * The agent has scoped filesystem access to:
 *   - Its workspace (working directory) — schema.json, naming_hints.json,
 *     verify wrapper, venv
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

You are generating a Python client library from a TypeScript River RPC server.
The generated code must read as if a senior Python engineer hand-wrote it.

## WHY we use an LLM for this (not a codegen script)

A mechanical codegen script walks JSON Schema and produces class names from
JSON paths: \`CreateInputArtifactServicesItemProductionHealthLiveness\`,
\`NOTFOUNDError\`, \`ReadErrorsVariant1\`.  Nobody can read that.

**Your job is to read the TypeScript source**, see that the developers named
a schema \`ExitInfo\`, and use that same name in Python.  See that
\`schemas.ts\` exports \`MonitorResponse\`, and call your class \`MonitorResponse\`.
See that \`lib/fs/errors.ts\` defines a \`FilesystemError\` union, and reuse it.

If you produce output that a codegen script could have produced, you have
failed.  The output will be discarded and regenerated.


## File access scope

You have access to these locations and ONLY these locations:

1. **Your workspace** (current working directory):
   - \`schema.json\` — serialised River JSON schema (ground truth for verification)
   - \`naming_hints.json\` — pre-computed error and $kind class names (USE THESE)
   - \`./verify\` — verification tool (only way to run Python)
   - \`generated/\` — output directory

   **NOTE:** There is NO Python interpreter in the workspace.  You cannot run
   Python scripts directly.  The ONLY way to execute Python is \`./verify\`.
   Write each service file by hand — do not attempt to create or run scripts.

2. **TypeScript server source** (READ-ONLY):
   \`${opts.serverSrcPath}\`
${existingSection}
**Do NOT attempt to browse or access any other directories.**


## naming_hints.json — USE THIS FILE

This file in your workspace contains pre-computed class names for every error
code and every \`$kind\` value found in schema.json:

\`\`\`json
{
  "errorCodeToClassName": {
    "NOT_FOUND": "NotFoundError",
    "CGROUP_CLEANUP_ERROR": "CgroupCleanupError",
    "PROCESS_IS_NOT_RUNNING": "ProcessIsNotRunningError",
    "DISK_QUOTA_EXCEEDED": "DiskQuotaExceededError",
    ...
  },
  "standardErrorCodes": ["UNCAUGHT_ERROR", "UNEXPECTED_DISCONNECT", "INVALID_REQUEST", "CANCEL"],
  "kindValueToClassName": {
    "finished": "Finished",
    "output": "Output",
    "result": "Result",
    ...
  },
  "tsExportNames": {
    "shellExec": ["ExitInfo", "OutputChunk", "MonitorResponse", "ShellOutput", ...],
    "artifact": ["Artifact", "CreateArtifactOptions", ...],
    ...
  },
  "tsSharedExportNames": {
    "lib/fs": ["FilesystemError"],
    "lib/shell": ["ShellOutput", "ShellError"],
    ...
  }
}
\`\`\`

**You MUST use the exact class names from this file** for error classes and
\`$kind\` variants.  Do NOT invent your own PascalCase conversion.

**For data types**, check \`tsExportNames\` and \`tsSharedExportNames\`.  These
are names extracted from the TypeScript source (with the "Schema" suffix
removed).  When you see a TypeBox schema in a service's \`schemas.ts\` named
e.g. \`ExitInfo\`, the matching entry will appear in
\`tsExportNames["shellExec"]\` as \`"ExitInfo"\`.  **Use these names for your
Python classes** instead of mechanical path-derived names.

The verifier rejects class names with 4+ consecutive uppercase letters
(e.g. \`NOTFOUNDError\`, \`CGROUPCLEANUPERRORError\` — these are BANNED).


## BANNED patterns — automatic rejection

The verifier scans BEFORE comparing schemas.  If ANY banned pattern is found,
verification fails immediately.

**Banned constructs:**
- \`RootModel\`, \`__get_pydantic_json_schema__\`, \`create_model()\`
- \`SchemaAdapter\`, \`make_schema_model\`, \`make_schema_adapter\`
- \`RiverTypeAdapter\` or any custom TypeAdapter subclass
- \`JsonAdapter\` or any custom adapter/wrapper class
- Custom \`json_schema()\` methods — only Pydantic's built-in is allowed
- \`schema_override_json\`, \`schema_override\`, \`_schema_json\`
- \`SimpleNamespace\`, \`_make_adapter\`, or any fake adapter objects
- \`WithJsonSchema\` — do NOT attach raw JSON schemas to \`Any\` via annotations
- \`json.loads(\` — do NOT embed raw JSON schema strings in generated code
- Loading \`schema.json\` at runtime in \`_schema_map.py\` or any module
- Monkey-patching \`.json_schema\` on TypeAdapter instances (\`.json_schema = \`)
- \`_bind_reference\`, \`_load_reference\`, \`frozen_schema\` — do NOT replace
  adapter schemas with reference data from schema.json
- Raw JSON Schema dicts embedded as Python dict literals or JSON strings
- Any helper/utility that builds models from schema dicts at runtime

**Banned naming patterns (regex-enforced):**
- \`Duplicate\` or \`Triplicate\` in a class name — reuse the same class instead
- \`Variant\\d+\` anywhere in a class name — ALL banned
- \`Input2\`, \`Output2\`, \`Errors3\` — numbered suffixes
- 4+ consecutive uppercase letters: \`NOTFOUNDError\`, \`PTYERRORError\` — banned
- Class names > 60 characters — indicates mechanical path-derived naming
- Chained \`Literal[x] | Literal[y] | Literal[z]\` — use \`Literal[x, y, z]\` instead

**Banned redefinitions:**
- Redefining \`UncaughtError\`, \`UnexpectedDisconnectError\`,
  \`InvalidRequestError\`, or \`CancelError\` outside \`_errors.py\`

**Banned workflow:**
- Do NOT write a standalone Python generator/scaffolding script (e.g.
  \`build_generated.py\`, \`scaffold.py\`, \`generate.py\`).  You must write
  each service file directly.

**Required:**
- Every type MUST be a concrete \`BaseModel\` subclass with typed fields
- \`TypeAdapter(Model).json_schema()\` must produce correct schemas through
  Pydantic's own native generation — no overrides or raw JSON embedding
- Error class names must come from \`naming_hints.json\`
- Standard River errors must be imported from \`_errors.py\`, not redefined


## Source of truth

### TypeScript server source — read this for naming

The TypeScript service definitions live at:

    ${opts.serverSrcPath}

Each service is in its own subdirectory with \`index.ts\` (procedure
definitions) and often \`schemas.ts\` (named TypeBox schemas).  Shared types
live in \`lib/\` subdirectories.

**You MUST read these files** for each service to learn:
- What the developers named their schemas (use those names in Python)
- Which types are shared across procedures
- How unions and intersections are structured

The top-level \`index.ts\` registers all services.

### schema.json — ground truth for verification

    schema.json  (in workspace, copied from ${opts.schemaPath})

Structure:
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
- Field names match TypeScript property names (camelCase is fine)


### Example 2: \`$kind\` discriminated union — use TS names

TypeScript:
\`\`\`typescript
// In schemas.ts:
const ExitInfo = Type.Object({
  $kind: Type.Literal('finished'),
  exitCode: Type.Integer(),
  reason: Type.Union([Type.Literal('Errored'), Type.Literal('Exited'), Type.Literal('Stopped')]),
});

const OutputChunk = Type.Object({
  $kind: Type.Literal('output'),
  output: Type.Uint8Array(),
});

const MonitorResponse = Type.Union([ExitInfo, OutputChunk]);
\`\`\`

Python — use the **TypeScript names** (\`ExitInfo\`, \`OutputChunk\`):
\`\`\`python
class ExitInfo(BaseModel):
    kind: Literal['finished'] = Field(alias='$kind')
    exitCode: int
    reason: Literal['Errored', 'Exited', 'Stopped']
    model_config = ConfigDict(populate_by_name=True)

class OutputChunk(BaseModel):
    kind: Literal['output'] = Field(alias='$kind')
    output: bytes
    model_config = ConfigDict(populate_by_name=True)

MonitorResponse = Annotated[ExitInfo | OutputChunk, Field(discriminator='kind')]
\`\`\`

Note: \`ExitInfo\` comes from the TS variable name, NOT from the $kind value.
If you hadn't read the TS source, you'd call it \`FinishedOutput\` — which is
worse.  **This is why reading the TypeScript source matters.**


### Example 3: Error union — use naming_hints.json

TypeScript:
\`\`\`typescript
const FilesystemError = Type.Union([
  Type.Object({ code: Type.Literal('NOT_FOUND'), message: Type.String() }),
  Type.Object({ code: Type.Literal('PERMISSION_DENIED'), message: Type.String() }),
]);
\`\`\`

Python — look up names in naming_hints.json:
\`\`\`python
# naming_hints.json says: "NOT_FOUND" → "NotFoundError"
class NotFoundError(BaseModel):
    code: Literal['NOT_FOUND']
    message: str

# naming_hints.json says: "PERMISSION_DENIED" → "PermissionDeniedError"
class PermissionDeniedError(BaseModel):
    code: Literal['PERMISSION_DENIED']
    message: str

FilesystemError = Annotated[
    NotFoundError | PermissionDeniedError,
    Field(discriminator='code'),
]
\`\`\`


### Example 4: Optional fields, records, arrays

\`\`\`python
class Config(BaseModel):
    name: str
    tags: list[str] | None = None          # Type.Optional(Type.Array(...))
    env: dict[str, str] | None = None      # Type.Optional(Type.Record(...))
    port: int | None = None                # Type.Optional(Type.Integer())
    enabled: bool
\`\`\`


### Example 5: Recursive types

\`\`\`python
class Skill(BaseModel):
    name: str
    description: str
    children: list['Skill']

Skill.model_rebuild()   # Required for self-referencing models
\`\`\`


### Example 6: Type.Intersect (allOf) — flatten into one model

TypeBox \`Type.Intersect([A, B])\` produces \`allOf\` in JSON Schema.
In Python, merge all properties into one BaseModel.  The verifier flattens
\`allOf\` during normalisation, so a flat model matches correctly.


### Example 7: Shared \`$kind\` with sub-discriminator

Sometimes two variants share the same \`$kind\` value but differ by another
field (e.g. \`status\`).  You CANNOT use \`Field(discriminator='kind')\` when
two variants have the same \`kind\` value — Pydantic will raise an error.

\`\`\`json
{"anyOf": [
  {"properties": {"$kind": {"const": "toolResult"}, "status": {"const": "ok"}, "result": {}}, ...},
  {"properties": {"$kind": {"const": "toolResult"}, "status": {"const": "error"}, "error": {...}}, ...},
  {"properties": {"$kind": {"const": "output"}, ...}, ...}
]}
\`\`\`

Solution: nest the sub-discriminated variants into a union, then use the
outer discriminator only on unique \`$kind\` values:

\`\`\`python
class ToolResultOk(BaseModel):
    kind: Literal['toolResult'] = Field(alias='$kind')
    status: Literal['ok']
    invocationId: str
    result: Any
    model_config = ConfigDict(populate_by_name=True)

class ToolResultError(BaseModel):
    kind: Literal['toolResult'] = Field(alias='$kind')
    status: Literal['error']
    invocationId: str
    error: ToolResultErrorDetail
    model_config = ConfigDict(populate_by_name=True)

# Sub-discriminate by status (unique within this group)
ToolResult = Annotated[ToolResultOk | ToolResultError, Field(discriminator='status')]

# For the outer union, do NOT use discriminator='kind' since
# toolResult appears twice.  Use a plain union instead:
EvaluateInput = ToolResultOk | ToolResultError | OutputMessage | ...
\`\`\`

If ALL \`$kind\` values are unique, you CAN use \`Field(discriminator='kind')\`.
If ANY \`$kind\` value appears more than once, you MUST NOT use a \`kind\`
discriminator — use a plain union instead.


## What to generate

Write a complete Python package into: \`generated/\`

### Package structure

\`\`\`
generated/
  __init__.py              # ${opts.clientName} client class + top-level re-exports
  _handshake.py            # HandshakeSchema model
  _errors.py               # Standard River errors (UncaughtError, etc.)
  _common.py               # Shared domain types used across multiple services
  <service_name>.py        # ALL types + service class for that service
  _schema_map.py           # Verification mapping
\`\`\`

**One file per service** — each \`<service_name>.py\` contains:
- All BaseModel classes for every procedure (named from TS source + naming_hints.json)
- Service-level shared error types (defined once, reused across procedures)
- The service class with typed async methods
- TypeAdapter instances for each procedure

### Design principles

1. **Mirror TypeScript naming.** Read the TypeBox definitions and use the
   same names.  \`CreateOptionsSchema\` → \`CreateOptions\`.

2. **Reuse shared types.** Standard River errors: import from \`_errors.py\`.
   Same error type across procedures within a service: define once, reuse.

3. **BaseModel everywhere.** No TypedDict, no RootModel, no dynamic creation.

4. **Discriminated unions.** \`Annotated[A | B, Field(discriminator='field')]\`.
   For \`$kind\`: use \`Field(alias='$kind')\` on each variant.

5. **TypeAdapters** for each procedure in each service module.

6. **Service classes** wrapping a River client with typed async methods.

7. **String/int literal unions.** Use multi-value Literal: \`Literal['a', 'b', 'c']\`
   or \`Literal[0, 1, 2]\`.  Do NOT chain single-value Literals with \`|\`.

8. **Intersections.** Flatten all properties into a single BaseModel.

9. **Error deduplication.** Same \`code\` literal + same fields = same class.
   Define once at the top of the service file.  The verifier deduplicates
   structurally identical \`anyOf\` variants, so if the same error appears
   multiple times in a composed union, **reuse the same class** — do NOT
   create \`FooDuplicate\` or \`FooTriplicate\` copies.


### The _schema_map.py module

Exports \`SCHEMA_MAP\` — a nested dict of service → procedure → adapters:

\`\`\`python
SCHEMA_MAP: dict = {
    "<serviceName>": {
        "procedures": {
            "<procName>": {
                "input": <ProcNameInputAdapter>,
                "output": <ProcNameOutputAdapter>,
                "errors": <ProcNameErrorsAdapter>,
                "type": "rpc",
            }
        }
    },
}
\`\`\`

Every service and procedure from schema.json must be represented.

**CRITICAL:** \`_schema_map.py\` must ONLY import TypeAdapter instances from
the service modules and assemble them into the dict.  It must NOT:
- Load \`schema.json\` at runtime
- Monkey-patch \`.json_schema\` methods on adapters
- Use \`copy.deepcopy\` to cache reference schemas
- Contain any function named \`_bind_reference_schemas\` or \`_load_reference_services\`

The adapters' \`.json_schema()\` output must come entirely from the Pydantic
models they wrap.  If verification fails, fix the **models**, not the schema map.


## Verification

After generating all files, run:

    ./verify schema.json generated

Exit codes: 0 = success, 1 = mismatches, 2 = import error or banned patterns.

**If it fails, read the errors, fix the models, and re-run.**

### What the verifier normalises (so you don't need to worry about these)

- \`title\` fields — stripped
- \`$ref\`/\`$defs\` — inlined
- \`X | None\` → strips null variant (TypeBox Optional = not required, not nullable)
- \`const\` + \`type\` → strips \`type\` (redundant when \`const\` present)
- \`Uint8Array\` → normalised to \`string\`
- \`discriminator\` metadata — stripped
- \`additionalProperties\` — stripped
- \`enum\` → normalised to \`anyOf\` with \`const\` entries
- \`allOf\` → flattened into merged object
- \`anyOf\` / \`oneOf\` → structurally identical variants deduplicated

The verifier deduplicates \`anyOf\` variants, so if the same error type
(e.g. \`UncaughtError\`) appears multiple times in a composed error union,
**reuse the same Python class** — do NOT create \`UncaughtErrorDuplicate\`
or similar copies.  Use a plain union (not discriminated) when the same
discriminator value appears more than once.

Use \`bytes\` for Uint8Array fields.  The verifier handles the rest.


## Step-by-step process

### Phase 1: Read the TypeScript source (MANDATORY)

This phase is about READING, not writing.  Do not write any Python yet.

1. Read \`${opts.serverSrcPath}/index.ts\` to get the full list of services.

2. Read \`naming_hints.json\` to load the error code → class name mapping.

3. For **every** service directory, read the TypeScript files (\`index.ts\`,
   \`schemas.ts\`, etc.) and note:
   - What named schemas are exported (e.g. \`MonitorResponse\`, \`ExitInfo\`,
     \`FilesystemError\`, \`CreateArtifactOptionsSchema\`)
   - What shared types are imported from \`lib/\` directories
   - How procedures are defined and what types they reference

   You don't need to memorise everything — you'll re-read individual services
   when generating them.  But you need a high-level map of what names exist.

4. Also scan schema.json to understand the structure:
   \`jq '.services | keys' schema.json\` — list services
   \`jq '.services.healthCheck' schema.json\` — sample service

### Phase 2: Shared types

5. Write \`_errors.py\` with the four standard River error models and the
   \`StandardRiverError\` discriminated union.

6. Write \`_common.py\` with any domain types reused across multiple services
   (based on what you found in Phase 1).

### Phase 3: Service-by-service generation

For **each** service:

7. **Re-read** the TypeScript source for that service (\`index.ts\`,
   \`schemas.ts\`).  Note the exported schema names.

8. Read the JSON Schema: \`jq '.services.<serviceName>' schema.json\`

9. Write \`<service_name>.py\` containing:
   - All Pydantic models named after the TypeScript schemas
   - Error classes named from \`naming_hints.json\`
   - Shared service-level errors defined once
   - Service class with typed async methods
   - TypeAdapter instances

   When a TypeScript file exports \`const FooSchema = Type.Object({...})\`,
   your Python class should be called \`Foo\` (drop the "Schema" suffix).

   Do this for **every** service.  Do not skip any.

### Phase 4: Assembly and verification

10. Write \`_schema_map.py\` covering every service and procedure.
11. Write \`__init__.py\` with the \`${opts.clientName}\` client class.
12. Run \`./verify schema.json generated\`
13. Fix failures and re-run until verification passes.


## Previous failures — learn from these

Previous generation attempts failed in specific ways.  Do NOT repeat them:

### Failure 1: Writing a scaffolding script that walks schema.json

The agent wrote \`build_generated.py\` — a Python script that walked
schema.json and generated all files mechanically.  It never read the
TypeScript source.  Result: every error class was named by mangling JSON
paths (\`ReadErrorsVariant1\` through \`ReadErrorsVariant16\`), every type
had a mechanical name (\`CreateInputArtifactServicesItemDevelopmentRunVariant1\`).

**Do NOT write a generator script.**  Write each service file directly.

### Failure 2: Broken PascalCase conversion for error codes

The agent's scaffolding script converted \`NOT_FOUND\` to \`NOTFOUNDError\`
(just stripped underscores) instead of \`NotFoundError\` (proper PascalCase).
Result: \`CGROUPCLEANUPERRORError\`, \`PTYERRORError\`, \`DISKQUOTAEXCEEDEDError\`.

**Use the names from naming_hints.json.** They are already correct.

### Failure 3: Overriding TypeAdapter.json_schema() to cheat verification

The agent created \`RiverTypeAdapter\` with \`schema_override_json\` that
returned raw JSON Schema instead of letting Pydantic generate it.
Verification "passed" but the models were wrong.

**Use plain TypeAdapter only.**  Fix the models until they produce correct
schemas natively.

### Failure 4: Redefining standard errors in every file

\`UncaughtError\`, \`UnexpectedDisconnectError\`, \`InvalidRequestError\`,
\`CancelError\` were copy-pasted into all 56 service files instead of
importing from \`_errors.py\`.

**Import from _errors.py.**

### Failure 5: Writing scaffolding scripts outside the workspace

The agent wrote \`/tmp/gen_models.py\` — a scaffolding script placed OUTSIDE
the workspace to dodge the ban.  It used \`.venv/bin/python\` to execute it.
Result: same terrible mechanical names, stuttered class names 200+ chars long
(\`ListInstalledPackagesOutputPackagesValueAllItemListInstalled...\`).

**There is no Python interpreter available.  Write files directly.**

### Failure 6: Custom JsonAdapter class to manipulate json_schema() output

The agent defined a \`JsonAdapter\` class that wraps \`TypeAdapter\` and strips
\`$defs\` keys to game verification.  This is banned — use plain \`TypeAdapter\`.

### Failure 7: Chained Literal[x] | Literal[y] | Literal[z]

The agent produced \`Literal[0] | Literal[1] | Literal[2] | ... | Literal[8]\`
instead of \`Literal[0, 1, 2, 3, 4, 5, 6, 7, 8]\`.  The chained form is banned.
Use multi-value \`Literal[...]\` for cleaner, more readable code.

### Failure 8: Fake adapters in _schema_map.py (SimpleNamespace cheat)

The agent made \`_schema_map.py\` load \`schema.json\` at runtime and create
\`SimpleNamespace\` objects with a \`json_schema()\` method that returns the raw
schema directly.  This bypasses verification entirely — the actual Pydantic
models are never tested.

**The verifier checks that every adapter is a real \`TypeAdapter\` instance.**
\`_schema_map.py\` MUST import TypeAdapter instances from service modules.
Any fake adapter will be rejected.

### Failure 9: WithJsonSchema + json.loads to bypass model verification

The agent used \`TypeAdapter(Annotated[Any, WithJsonSchema(json.loads(...))])\`
on EVERY adapter.  The raw JSON schema was embedded as a string and passed
through \`WithJsonSchema\`.  The Pydantic models were decorative — never tested.

**\`WithJsonSchema\` and \`json.loads\` are both banned.**  Every TypeAdapter
must be \`TypeAdapter(ModelClass)\` where \`ModelClass\` is a BaseModel subclass
or a union of BaseModel subclasses.  The adapter's \`.json_schema()\` must
derive entirely from the Pydantic model structure.


## Important notes

- **No Python in workspace.** You cannot run \`.venv/bin/python\`, \`python3\`,
  or any Python scripts.  The only executable is \`./verify\`.  Do NOT create
  venvs, install packages, or write Python scripts to run.
- schema.json is large.  Use \`jq\` to read specific services.
- \`jq '.services | keys' schema.json\` — list all service names
- \`jq '.services.<name>' schema.json\` — inspect a specific service
- When the verifier reports mismatches, read the error message carefully and
  fix the model by hand.
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
  allowed. Rewrite those files with concrete BaseModel subclasses.

- **If there are "BANNED NAME" errors:** your class names are wrong.
  - For error classes: use the names from \`naming_hints.json\`
  - For other types: read the TypeScript source for the correct name
  - Names with 4+ consecutive uppercase letters (e.g. NOTFOUNDError) are
    banned — use proper PascalCase (NotFoundError)

- **If there are schema mismatches:** compare original JSON Schema
  (\`jq '.services.<svc>.procedures.<proc>.<facet>' schema.json\`)
  against \`TypeAdapter(Model).json_schema()\`.  Common fixes:
  - Flatten \`allOf\` into a single BaseModel
  - Use \`bytes\` for Uint8Array fields
  - Check required vs optional fields match
  - Check field types (float vs int, str vs bool)

After fixing, re-run: \`./verify schema.json generated\`

Keep fixing until verification passes.
`.trim();
}


// ===========================================================================
// Pass 2: Quality refactoring prompts
// ===========================================================================

/**
 * Build the initial prompt for Pass 2 — aggressive quality refactoring.
 *
 * The agent's workspace contains:
 *   - `generated/`  — Pass 1 output (functionally correct, badly named)
 *   - `schema.json`  — the original River schema
 *   - `naming_hints.json` — pre-computed error/kind name mappings
 *   - `./verify`     — the verification tool
 *
 * The agent has read-only access to the TS server source.
 */
export function buildPass2InitialPrompt(opts: Pass2Options): string {
  return `
# Task: Aggressively refactor generated Pydantic models for production quality

You have a Python package in \`generated/\` that is **functionally correct** —
every procedure passes schema verification.  But the code quality is poor:
mechanical names, massive duplication, no relationship to the TypeScript source
names.  Your job is to refactor it to production quality.

**The bar: a senior Python engineer should look at the output and believe a
human wrote it.**  If the output still looks machine-generated after your
refactoring, it will be discarded.


## What's wrong with the current code

The current code was generated by an LLM that walked \`schema.json\` mechanically.
It never read the TypeScript source for naming.  Specific problems:

### 1. Meaningless alphabetic suffix names
Union variants get \`A\`, \`B\`, \`C\` suffixes instead of descriptive names:

- \`MonitorOutputA\` (state="NotStarted"), \`MonitorOutputB\` (state="Running"),
  \`MonitorOutputC\` (state="Finished") — should be \`NotStartedProcess\`,
  \`RunningProcess\`, \`FinishedProcess\` or whatever the TS source calls them
- \`ComputerUseInputActionA\` through \`ComputerUseInputActionP\` (16 actions!)
  — should be \`ScreenshotAction\`, \`LeftClickAction\`, \`TypeAction\`, etc.
- \`ToolResultA\` (error variant) — should be \`ToolResultError\`
- \`RequestTakeoverOutputA/B/C\` — should be named after their \`status\` value

### 2. Deep path-derived class names
Names like \`CreateInputArtifactServicesItemProductionHealthLiveness\` or
\`GetDeploymentArtifactsOutputArtifactsItemServicesItem\`.  These are the same
terrible names a mechanical codegen produces.  Use concise, meaningful names
derived from what the type actually represents.

### 3. Massive error class duplication
\`NotFoundError\`, \`AlreadyExistError\`, \`IsDirectoryError\`, etc. (the
filesystem error set) are redefined identically in ~20+ files.  They should
be defined once and imported.  Same for \`InvalidRequestErrorExtras\` and
\`InvalidRequestErrorExtrasFirstValidationErrorsItem\` — duplicated in 56
files when they belong in \`_errors.py\`.

### 4. Self-assignment aliases
Lines like \`SpawnInput = SpawnInput\`, \`ReadInput = ReadInput\` are pointless
noise.  Remove them.

### 5. _schema_map.py uses monkey-patched TypeAdapters
The current \`_schema_map.py\` creates TypeAdapter instances and replaces their
\`.json_schema\` method with a lambda returning raw dicts.  This means the
actual models are never tested through the schema map.  After refactoring,
\`_schema_map.py\` MUST import TypeAdapters from the service modules.

### 6. Unused imports
Many files import \`StringConstraints\`, \`ConfigDict\`, etc. without using them.
Clean up.

### 7. Duplicate/Triplicate error classes
Classes like \`UncaughtErrorDuplicate\`, \`NotFoundErrorDuplicate\`,
\`UncaughtErrorTriplicate\` exist because the same error code appears
multiple times in composed \`anyOf\` unions, and Pydantic discriminated
unions require unique types.  The verifier now **deduplicates structurally
identical anyOf variants**, so you can reuse the same error class.
**Delete all \`*Duplicate\` and \`*Triplicate\` classes.**  Use the original
class in a plain union (without \`Field(discriminator=...)\`) when the same
discriminator value appears more than once.

### 8. Duplicate field declarations
Some classes have the same field declared twice (e.g. \`extras: Foo\` on two
consecutive lines).  Python silently shadows the first.  The verifier now
catches this.  **Remove duplicate field declarations.**

### 9. Data type names not from TypeScript source
Top-level schema names like \`ExitInfo\`, \`MonitorResponse\`, \`ShellOutput\`
from the TypeScript source don't appear — everything has path-derived names.
Check \`naming_hints.json\` → \`tsExportNames\` for pre-extracted names per
service, and \`tsSharedExportNames\` for shared lib/ types.


## File access scope

1. **Your workspace** (current working directory):
   - \`generated/\` — the code to refactor (read-write)
   - \`schema.json\` — original River schema (read-only reference)
   - \`naming_hints.json\` — error/kind name hints (read-only reference)
   - \`./verify\` — verification tool

2. **TypeScript server source** (READ-ONLY — this is the naming authority):
   \`${opts.serverSrcPath}\`

**There is NO Python interpreter in the workspace.**  The ONLY way to run
Python is \`./verify\`.  Do NOT write or run Python scripts.


## How to approach the refactoring

### Phase 1: Study naming_hints.json and TypeScript source

Before changing any Python code:

1. **Read \`naming_hints.json\`** — it contains:
   - \`errorCodeToClassName\`: error code → Python class name mapping
   - \`kindValueToClassName\`: \`$kind\` value → class name prefix
   - \`tsExportNames\`: per-service list of TypeBox schema names from TS source
     (e.g. \`"shellExec": ["ExitInfo", "OutputChunk", "MonitorResponse"]\`)
   - \`tsSharedExportNames\`: shared lib/ TypeBox schema names
     (e.g. \`"lib/fs": ["FilesystemError"]\`)

   **Use these names** to rename Python classes.  The \`tsExportNames\` entries
   are the authoritative TypeScript names with "Schema" suffix already removed.

2. Read \`${opts.serverSrcPath}/index.ts\` to get the service list.

3. For each service, read its \`schemas.ts\` and \`index.ts\`.  Cross-reference
   with \`tsExportNames[serviceName]\` to confirm which TS names map to which
   Python classes.  Look for:
   - **Named exports**: match to entries in \`tsExportNames\`
   - **Union variant names**: if TS has \`const MonitorResponse = Type.Union([
     NotStartedState, RunningState, FinishedState])\`, those variant names
     should appear in Python
   - **Shared types in lib/**: match to entries in \`tsSharedExportNames\`

4. Read the shared \`lib/\` directories:
   - \`${opts.serverSrcPath}/../lib/\` or \`${opts.serverSrcPath}/lib/\` if it exists
   - Cross-reference with \`tsSharedExportNames\`

### Phase 2: Refactor shared types

4. **Expand \`_errors.py\`** — move ALL error classes that appear in 3+ service
   files into \`_errors.py\`.  This includes the filesystem error set
   (\`NotFoundError\`, \`AlreadyExistError\`, \`NotDirectoryError\`, etc.) and
   any other frequently-duplicated error classes.

5. **Expand \`_common.py\`** — if any non-error types are shared across
   services, define them here.

6. Remove \`InvalidRequestErrorExtras\` and
   \`InvalidRequestErrorExtrasFirstValidationErrorsItem\` from every service
   file — they belong in \`_errors.py\` (as part of \`InvalidRequestError\`'s
   definition, which is already there).

### Phase 3: Service-by-service refactoring

For **each** service file:

7. **Read the TypeScript source** for that service.

8. **Rename classes** to match TypeScript names:
   - If TS exports \`const FooSchema = Type.Object({...})\`, rename the Python
     class from whatever mechanical name it has to \`Foo\`
   - If TS has a named union variant, use that name
   - For union variants distinguished by a field value (like \`state\` or
     \`action\`), name the class after the value:
     \`MonitorOutputA\` with state="NotStarted" → \`NotStartedProcess\`
     \`ComputerUseInputActionB\` with action="left_click" → \`LeftClickAction\`
   - Drop "Schema" suffixes from TS names (\`ExitInfoSchema\` → \`ExitInfo\`)

9. **Shorten deep path names**:
   - \`CreateInputArtifactServicesItemProductionHealthLiveness\` →
     \`ProductionHealthLiveness\` or \`HealthProbe\` (whatever the TS calls it)
   - \`GetDeploymentArtifactsOutputArtifactsItemServicesItem\` →
     \`DeploymentService\` or similar

10. **Replace duplicated error classes** with imports from \`_errors.py\`.

11. **Remove self-assignment aliases** (\`SpawnInput = SpawnInput\`).

12. **Clean up imports** — remove unused ones.

13. **Update TypeAdapter references** if you renamed any classes.

### Phase 4: Rebuild _schema_map.py and verify

14. **Rewrite \`_schema_map.py\`** to be a simple import-and-assemble module:
    - Import TypeAdapter instances from each service module
    - Assemble them into the \`SCHEMA_MAP\` dict
    - Do NOT load \`schema.json\` at runtime
    - Do NOT use AST parsing, \`inspect\`, or runtime introspection
    - Do NOT monkey-patch \`.json_schema\` on any adapter
    - Do NOT use \`_bind_reference_schemas\`, \`_load_reference_services\`,
      \`frozen_schema\`, \`copy.deepcopy\`, or any schema override mechanism
    - The \`_schema_map.py\` should be ~200 lines of straightforward imports

15. **Update \`__init__.py\`** if any service class names changed.

16. **Run \`./verify schema.json generated\`** and fix any regressions.


## Critical rules

- **Verification must still pass.**  Every rename, every moved type, every
  import change — the models must still produce identical JSON schemas.
  Run \`./verify\` after each batch of changes.

- **Do not change field names or types.**  Only rename classes and move
  definitions.  The field-level structure must remain identical.

- **Do not change the TypeAdapter type arguments.**  If a TypeAdapter wraps
  a union \`A | B | C\`, and you rename A to FooBar, update the union to
  \`FooBar | B | C\`.  Do not change what the adapter wraps.

- **Error class names from naming_hints.json are correct.**  Don't rename
  \`NotFoundError\` to \`FileNotFoundError\` — the naming_hints names match
  the error code literals and the verifier expects them.

- **When in doubt about a name, check the TS source.**  The TS source is
  the naming authority.  If it doesn't export a name for something, pick a
  concise descriptive name based on the type's structure and usage.

- **Run verification frequently** — after refactoring each service or each
  batch of 3-5 services.  Don't refactor everything then verify once at the
  end.


## Verification

    ./verify schema.json generated

Exit 0 = pass.  Must pass when you're done.


## Previous failure patterns to avoid

- Don't write a scaffolding script to do the refactoring.  Edit files directly.
- Don't use \`sed\` for complex renames — it breaks Python syntax.  Read the
  file, understand the structure, rewrite it.
- Don't skip services.  Every service file must be reviewed and improved.
- Don't break imports.  If you move a class from \`service.py\` to
  \`_errors.py\`, update every file that used the local definition.

### Run 10 failures (most recent):

- **\`*Duplicate\` / \`*Triplicate\` error classes** — \`UncaughtErrorDuplicate\`,
  \`NotFoundErrorDuplicate\`, etc.  These are BANNED.  The verifier now
  deduplicates \`anyOf\` variants, so just reuse the same class.  If the same
  discriminator value appears more than once in a union, use a plain union
  (no \`Field(discriminator=...)\`).

- **Monkey-patched \`_schema_map.py\`** — \`_bind_reference_schemas()\` loaded
  \`schema.json\` and replaced each adapter's \`.json_schema\` with a lambda.
  This is BANNED.  Write a simple import-and-assemble schema map.

- **Duplicate field declarations** — \`ParseError\` had \`extras: ParseErrorExtras\`
  on two consecutive lines.  This is now caught by the verifier.

- **TS export names still missing** — Classes were still named with path-derived
  mechanical names instead of \`ExitInfo\`, \`MonitorResponse\`, etc.  Use
  \`naming_hints.json\` → \`tsExportNames\` for the correct names.
`.trim();
}

/**
 * Build a retry prompt for Pass 2 when verification fails after refactoring.
 */
export function buildPass2RetryPrompt(verificationOutput: string): string {
  return `
Your refactoring broke verification.  Here is the output:

\`\`\`
${verificationOutput}
\`\`\`

This is likely caused by:
- A renamed class that wasn't updated in all references
- A moved type whose import wasn't updated in all files
- A field type that accidentally changed during refactoring
- A TypeAdapter that still references the old class name

Fix every error.  Run \`./verify schema.json generated\` after fixing.

**Do not revert to the pre-refactoring code.**  Fix the refactored code
so it passes verification while keeping the quality improvements.
`.trim();
}
