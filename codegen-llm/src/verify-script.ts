/**
 * Embedded Python verification script.
 *
 * This script is written into the Codex agent workspace at runtime.
 * It performs two checks:
 *
 * 1. Code quality — scans generated .py files for banned patterns that
 *    indicate the agent took shortcuts (RootModel[Any], raw schema dicts,
 *    dynamic class factories, etc.).
 *
 * 2. Schema comparison — loads the original River JSON schema and compares
 *    it against JSON schemas produced by the generated Pydantic models via
 *    TypeAdapter.json_schema(), after extensive normalisation of both sides.
 *
 * The generated package must contain a `_schema_map` module exporting
 * SCHEMA_MAP: a nested dict of service -> procedure -> {input, output, errors, type}.
 */
export const VERIFY_SCRIPT = `#!/usr/bin/env python3
"""
Verify generated Pydantic models against the original River JSON schema.

Usage:
    python verify_schema.py <original_schema.json> <generated_package_dir>

Exit codes:
    0  All procedures match
    1  Mismatches found (details printed to stdout)
    2  Usage error, import failure, or banned patterns detected
"""
from __future__ import annotations

import copy
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Code quality check — banned patterns
# ---------------------------------------------------------------------------

_BANNED_PATTERNS: list[tuple[str, str]] = [
    ('RootModel', 'Do not use RootModel — use BaseModel with typed fields'),
    ('make_schema_model', 'Do not use make_schema_model() — define concrete BaseModel classes'),
    ('_SchemaBackedModel', 'Do not use _SchemaBackedModel — define concrete BaseModel classes'),
    ('schema_definition', '"schema_definition" ClassVar is banned — use real typed fields'),
    ('__get_pydantic_json_schema__', 'Do not override __get_pydantic_json_schema__'),
    ('SchemaAdapter', 'Do not use SchemaAdapter wrappers — use TypeAdapter directly'),
    ('make_schema_adapter', 'Do not use make_schema_adapter() — use TypeAdapter directly'),
    ('create_model(', 'Do not use create_model() — define BaseModel classes statically'),
    ('schema_override_json', 'Do not use schema_override_json — models must produce correct schemas natively'),
    ('schema_override', 'Do not override json_schema() output — models must produce correct schemas natively'),
    ('_schema_json', 'Do not cache/embed raw JSON schemas — models must produce correct schemas natively'),
    ('RiverTypeAdapter', 'Do not subclass TypeAdapter — use TypeAdapter directly with correct models'),
    ('json.loads(self._', 'Do not return embedded JSON from json_schema() — fix the model instead'),
]

# Standard River error class names that must ONLY be defined in _errors.py.
_STANDARD_ERROR_CLASSES = frozenset({
    'UncaughtError', 'UnexpectedDisconnectError', 'InvalidRequestError', 'CancelError',
})

# Regex for banned naming: class names ending in Variant + digits, or
# names like Input2, Output2, ErrorsVariant3, etc.
_BANNED_NAME_RE = re.compile(
    r'^class\\s+'
    r'('
    r'\\w*Variant\\d+\\w*'        # any name with VariantN in it
    r'|\\w*Errors?Variant\\d+'   # ErrorsVariant1, ErrorVariant2, ...
    r'|(?:Input|Output|Init|Errors?)\\d+'  # Input2, Output3, Errors4
    r')'
    r'\\s*\\(',
    re.MULTILINE,
)

# Regex for ALLCAPS class names — e.g. NOTFOUNDError, CGROUPCLEANUPERRORError.
# These come from a broken to_pascal_case that just strips underscores from
# UPPER_SNAKE_CASE instead of properly title-casing each word.
# Allows known abbreviations: API, RPC, URL, HTTP, SQL, ID, FS, PTY, MCP, PDF.
_ALLCAPS_NAME_RE = re.compile(
    r'^class\\s+((?:[A-Z]{4,})+\\w*)\\s*\\(',
    re.MULTILINE,
)
_ALLCAPS_WHITELIST = frozenset({
    # Add known abbreviation-heavy names that are actually correct
})


def check_code_quality(generated_dir: Path) -> list[str]:
    """Scan generated Python files for banned patterns."""
    errors: list[str] = []
    for py_file in sorted(generated_dir.rglob('*.py')):
        if '__pycache__' in py_file.parts:
            continue
        try:
            content = py_file.read_text()
        except Exception:
            continue
        rel = py_file.relative_to(generated_dir)
        for pattern, msg in _BANNED_PATTERNS:
            if pattern in content:
                errors.append(f'[{rel}] BANNED: {msg} (found \\"{pattern}\\")')

        # Check for numbered variant names
        for m in _BANNED_NAME_RE.finditer(content):
            class_name = m.group(1)
            errors.append(
                f'[{rel}] BANNED NAME: "{class_name}" — class names must be '
                f'meaningful, not numbered. Name error classes after their '
                f'code literal (e.g. NotFoundError), $kind variants after '
                f'their kind value (e.g. FinishedOutput), and types after '
                f'their TypeScript schema name.'
            )

        # Check for ALLCAPS class names (broken PascalCase conversion)
        for m in _ALLCAPS_NAME_RE.finditer(content):
            class_name = m.group(1)
            if class_name not in _ALLCAPS_WHITELIST:
                errors.append(
                    f'[{rel}] BANNED NAME: "{class_name}" — class name has 4+ '
                    f'consecutive uppercase letters, indicating a broken '
                    f'PascalCase conversion. Use the names from '
                    f'naming_hints.json (e.g. NOT_FOUND → NotFoundError, '
                    f'not NOTFOUNDError).'
                )

        # Check for standard error classes redefined outside _errors.py
        if rel.name != '_errors.py':
            for line in content.splitlines():
                class_match = re.match(r'^class\\s+(\\w+)\\s*\\(', line)
                if class_match and class_match.group(1) in _STANDARD_ERROR_CLASSES:
                    errors.append(
                        f'[{rel}] BANNED: class {class_match.group(1)} must not '
                        f'be redefined — import it from _errors.py instead'
                    )
    return errors


# ---------------------------------------------------------------------------
# Schema normalisation helpers
# ---------------------------------------------------------------------------

# Fields that carry no structural meaning and should be ignored when comparing.
_STRIP_KEYS = frozenset({
    'description', 'title', '$id', '$comment', 'examples', '$schema',
    'discriminator', 'additionalProperties',
})


def _resolve_refs(node: Any, defs: dict[str, Any], seen: set[str] | None = None) -> Any:
    """Recursively inline $ref / $defs."""
    if seen is None:
        seen = set()

    if isinstance(node, dict):
        if '$ref' in node:
            ref = node['$ref']
            name = None
            if ref.startswith('#/$defs/'):
                name = ref[len('#/$defs/'):]
            elif not ref.startswith('#') and not ref.startswith('http'):
                # Bare ref like "$ref": "Skill" (TypeBox Type.Recursive $id)
                name = ref
            if name and name in defs and name not in seen:
                seen = seen | {name}
                return _resolve_refs(copy.deepcopy(defs[name]), defs, seen)
            # Unresolvable ref — strip it (treat as unconstrained)
            remaining = {k: v for k, v in node.items() if k != '$ref'}
            return _resolve_refs(remaining, defs, seen) if remaining else node

        out: dict[str, Any] = {}
        local_defs = node.get('$defs', defs)  # prefer local $defs scope
        for k, v in node.items():
            if k == '$defs':
                continue
            out[k] = _resolve_refs(v, local_defs, seen)
        return out

    if isinstance(node, list):
        return [_resolve_refs(item, defs, seen) for item in node]

    return node


def _strip_metadata(node: Any) -> Any:
    """Remove non-structural metadata fields."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in _STRIP_KEYS:
                continue
            out[k] = _strip_metadata(v)

        # Pydantic emits "default": null for Optional fields.  The original
        # schema usually omits it.  Ignore null defaults.
        if out.get('default') is None:
            out.pop('default', None)

        return out

    if isinstance(node, list):
        return [_strip_metadata(item) for item in node]

    return node


def _normalise_uint8array(node: Any) -> Any:
    """
    Normalise TypeBox's Uint8Array to string.

    TypeBox emits {"type": "Uint8Array", "maxByteLength": N} which is not
    standard JSON Schema.  Pydantic's 'bytes' produces {"type": "string",
    "format": "binary"} or similar.  We normalise both to {"type": "string"}.
    """
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k == 'maxByteLength':
                continue  # Strip TypeBox-specific constraint
            if k == 'format' and v in ('binary', 'byte', 'byte-array'):
                continue  # Strip Pydantic binary format markers
            out[k] = _normalise_uint8array(v)
        if out.get('type') == 'Uint8Array':
            out['type'] = 'string'
        return out

    if isinstance(node, list):
        return [_normalise_uint8array(item) for item in node]

    return node


def _normalise_const_type(node: Any) -> Any:
    """
    Strip 'type' when 'const' is present.

    TypeBox emits {"const": "foo", "type": "string"} but Pydantic emits
    just {"const": "foo"}.  The 'const' keyword is strictly more specific
    than 'type', so the type is redundant.
    """
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            out[k] = _normalise_const_type(v)
        if 'const' in out and 'type' in out:
            del out['type']
        return out

    if isinstance(node, list):
        return [_normalise_const_type(item) for item in node]

    return node


def _normalise_enum_to_anyof(node: Any) -> Any:
    """
    Convert {"enum": ["a", "b"]} to {"anyOf": [{"const": "a"}, {"const": "b"}]}.

    Pydantic's Literal['a', 'b'] produces an enum array, while TypeBox's
    Type.Union([Type.Literal('a'), Type.Literal('b')]) produces anyOf with
    individual const entries.  Normalise to the anyOf form.
    """
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            out[k] = _normalise_enum_to_anyof(v)
        if 'enum' in out and isinstance(out['enum'], list):
            variants = out.pop('enum')
            out.pop('type', None)  # type is implied by the consts
            out['anyOf'] = [{'const': v} for v in variants]
        return out

    if isinstance(node, list):
        return [_normalise_enum_to_anyof(item) for item in node]

    return node


def _normalise_nullable(node: Any) -> Any:
    """
    Normalise the various representations of nullable types into a single
    canonical form so that Pydantic output and TypeBox output compare equal.

    Canonical form for nullable X:  {"anyOf": [X, {"type": "null"}]}
    """
    if isinstance(node, dict):
        # type: ["string", "null"]  -->  anyOf
        if 'type' in node and isinstance(node['type'], list):
            types = list(node['type'])
            has_null = 'null' in types
            non_null = [t for t in types if t != 'null']
            base = dict(node)
            del base['type']
            if len(non_null) == 1:
                variant = {**base, 'type': non_null[0]}
            else:
                variant = {**base, 'type': non_null}
            if has_null:
                return _normalise_nullable({'anyOf': [variant, {'type': 'null'}]})
            return _normalise_nullable(variant)

        out = {}
        for k, v in node.items():
            if k == 'oneOf':
                # oneOf -> anyOf for comparison
                out['anyOf'] = _normalise_nullable(v)
            else:
                out[k] = _normalise_nullable(v)
        return out

    if isinstance(node, list):
        return [_normalise_nullable(item) for item in node]

    return node


def _strip_null_variant(node: Any) -> Any:
    """
    For anyOf with exactly [X, {"type": "null"}], simplify to X.

    This normalises the difference between:
    - TypeBox Optional: field not in required, value schema is just X
    - Pydantic Optional (X | None = None): anyOf [X, null], not in required

    By stripping the null variant, both representations collapse to X.
    """
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            out[k] = _strip_null_variant(v)
        if 'anyOf' in out and isinstance(out['anyOf'], list) and len(out['anyOf']) == 2:
            null_count = sum(1 for v in out['anyOf'] if v == {'type': 'null'})
            non_null = [v for v in out['anyOf'] if v != {'type': 'null'}]
            if null_count == 1 and len(non_null) == 1:
                return non_null[0]
        return out

    if isinstance(node, list):
        return [_strip_null_variant(item) for item in node]

    return node


def _flatten_allof(node: Any) -> Any:
    """
    Flatten allOf into a single merged object schema.

    TypeBox Type.Intersect produces allOf in JSON Schema.  Pydantic generates
    a flat object with all properties merged.  Normalise both to the merged
    form so they compare equal.
    """
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            out[k] = _flatten_allof(v)

        if 'allOf' in out and isinstance(out['allOf'], list):
            merged_props: dict[str, Any] = {}
            merged_required: list[str] = []
            merged_pattern_props: dict[str, Any] = {}
            remaining: dict[str, Any] = {}

            for sub in out['allOf']:
                if not isinstance(sub, dict):
                    continue
                if 'properties' in sub:
                    merged_props.update(sub['properties'])
                if 'patternProperties' in sub:
                    merged_pattern_props.update(sub['patternProperties'])
                if 'required' in sub and isinstance(sub['required'], list):
                    merged_required.extend(sub['required'])
                for sk, sv in sub.items():
                    if sk not in ('properties', 'patternProperties', 'required', 'type'):
                        remaining[sk] = sv

            # Carry over keys from the parent that aren't allOf
            for pk, pv in out.items():
                if pk == 'allOf':
                    continue
                if pk == 'properties':
                    merged_props.update(pv)
                elif pk == 'patternProperties':
                    merged_pattern_props.update(pv)
                elif pk == 'required' and isinstance(pv, list):
                    merged_required.extend(pv)
                elif pk != 'type':
                    remaining[pk] = pv

            result: dict[str, Any] = {'type': 'object'}
            if merged_props:
                result['properties'] = merged_props
            if merged_pattern_props:
                result['patternProperties'] = merged_pattern_props
            if merged_required:
                result['required'] = list(dict.fromkeys(merged_required))
            result.update(remaining)
            return result

        return out

    if isinstance(node, list):
        return [_flatten_allof(item) for item in node]

    return node


def _sort_canonical(node: Any) -> Any:
    """Sort dict keys and union variant lists for stable comparison."""
    if isinstance(node, dict):
        out = {}
        for k in sorted(node.keys()):
            if k in ('anyOf', 'oneOf'):
                items = [_sort_canonical(i) for i in node[k]]
                items.sort(key=lambda x: json.dumps(x, sort_keys=True))
                out[k] = items
            elif k == 'required' and isinstance(node[k], list):
                out[k] = sorted(node[k])
            else:
                out[k] = _sort_canonical(node[k])
        return out

    if isinstance(node, list):
        return [_sort_canonical(item) for item in node]

    return node


def prepare(schema: Any) -> Any:
    """Full normalisation pipeline."""
    defs = schema.get('$defs', {}) if isinstance(schema, dict) else {}
    s = _resolve_refs(schema, defs)
    s = _strip_metadata(s)
    s = _normalise_uint8array(s)
    s = _normalise_const_type(s)
    s = _normalise_enum_to_anyof(s)
    s = _normalise_nullable(s)
    s = _strip_null_variant(s)
    s = _flatten_allof(s)
    s = _sort_canonical(s)
    return s


# ---------------------------------------------------------------------------
# Deep comparison
# ---------------------------------------------------------------------------

def diff(original: Any, generated: Any, path: str = '') -> list[str]:
    """Return human-readable list of structural differences."""
    errs: list[str] = []
    if type(original) is not type(generated):
        errs.append(
            f'{path}: type mismatch: '
            f'{type(original).__name__}({_trunc(original)}) vs '
            f'{type(generated).__name__}({_trunc(generated)})'
        )
        return errs

    if isinstance(original, dict):
        all_keys = sorted(set(original) | set(generated))
        for k in all_keys:
            child_path = f'{path}.{k}' if path else k
            if k not in generated:
                errs.append(f'{child_path}: missing in generated')
            elif k not in original:
                errs.append(f'{child_path}: unexpected in generated')
            else:
                errs.extend(diff(original[k], generated[k], child_path))

    elif isinstance(original, list):
        if len(original) != len(generated):
            errs.append(
                f'{path}: array length {len(original)} vs {len(generated)}'
            )
        for i, (a, b) in enumerate(zip(original, generated)):
            errs.extend(diff(a, b, f'{path}[{i}]'))

    else:
        if original != generated:
            errs.append(f'{path}: {_trunc(original)} vs {_trunc(generated)}')

    return errs


def _trunc(v: Any, n: int = 120) -> str:
    s = repr(v)
    return s if len(s) <= n else s[:n] + '...'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 3:
        print('Usage: python verify_schema.py <schema.json> <generated_dir>')
        sys.exit(2)

    schema_path = Path(sys.argv[1])
    generated_dir = Path(sys.argv[2]).resolve()

    # ==== Step 1: Code quality check ====
    print('Checking code quality (banned patterns)...')
    quality_errors = check_code_quality(generated_dir)
    if quality_errors:
        print(f'\\nCODE QUALITY CHECK FAILED: {len(quality_errors)} banned pattern(s) found\\n')
        for e in quality_errors:
            print(f'  - {e}')
        print()
        print('You MUST use concrete BaseModel subclasses with typed fields.')
        print('Do NOT use RootModel, make_schema_model, dynamic class factories,')
        print('or raw JSON Schema dicts embedded in Python code.')
        print()
        print('Rewrite ALL affected files with proper BaseModel classes that')
        print('declare every field with an explicit Python type annotation.')
        sys.exit(2)

    print('Code quality check passed.')

    # ==== Step 2: Schema comparison ====
    with open(schema_path) as f:
        original = json.load(f)

    # Make the generated package importable
    sys.path.insert(0, str(generated_dir.parent))
    pkg = generated_dir.name

    try:
        sm = importlib.import_module(f'{pkg}._schema_map')
    except Exception as exc:
        print(f'Failed to import {pkg}._schema_map: {exc}')
        sys.exit(2)

    schema_map: dict = sm.SCHEMA_MAP  # type: ignore[attr-defined]

    total_procs = 0
    all_errors: list[str] = []

    for svc_name, svc_data in original.get('services', {}).items():
        if svc_name not in schema_map:
            all_errors.append(f'[{svc_name}] service missing from schema map')
            continue

        for proc_name, proc_data in svc_data.get('procedures', {}).items():
            total_procs += 1
            svc_map = schema_map[svc_name].get('procedures', {})
            if proc_name not in svc_map:
                all_errors.append(f'[{svc_name}.{proc_name}] procedure missing from schema map')
                continue

            gen = svc_map[proc_name]

            # Procedure type
            if proc_data.get('type') != gen.get('type'):
                all_errors.append(
                    f'[{svc_name}.{proc_name}] type: '
                    f'{proc_data.get("type")!r} vs {gen.get("type")!r}'
                )

            # Compare each schema facet
            for facet in ('input', 'output', 'errors'):
                orig_facet = proc_data.get(facet)
                gen_adapter = gen.get(facet)
                if orig_facet is None and gen_adapter is None:
                    continue
                if orig_facet is None:
                    all_errors.append(
                        f'[{svc_name}.{proc_name}.{facet}] '
                        f'original has no {facet} but generated does'
                    )
                    continue
                if gen_adapter is None:
                    all_errors.append(
                        f'[{svc_name}.{proc_name}.{facet}] '
                        f'generated has no {facet} but original does'
                    )
                    continue

                try:
                    gen_schema = gen_adapter.json_schema()
                except Exception as exc:
                    all_errors.append(
                        f'[{svc_name}.{proc_name}.{facet}] '
                        f'json_schema() failed: {exc}'
                    )
                    continue

                errs = diff(prepare(orig_facet), prepare(gen_schema))
                for e in errs:
                    all_errors.append(f'[{svc_name}.{proc_name}.{facet}] {e}')

    # Report
    if all_errors:
        print(f'\\nVERIFICATION FAILED: {len(all_errors)} error(s) across {total_procs} procedures\\n')
        for e in all_errors[:100]:
            print(f'  - {e}')
        if len(all_errors) > 100:
            print(f'  ... and {len(all_errors) - 100} more')
        sys.exit(1)
    else:
        print(f'\\nVERIFICATION PASSED: all {total_procs} procedures match\\n')
        sys.exit(0)


if __name__ == '__main__':
    main()
`;
