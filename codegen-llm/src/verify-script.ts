/**
 * Embedded Python verification script.
 *
 * This script is written into the Codex agent workspace at runtime.
 * It loads the original River JSON schema and compares it against
 * JSON schemas produced by the generated Pydantic models via
 * TypeAdapter.json_schema().
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
    2  Usage error or import failure
"""
from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Schema normalisation helpers
# ---------------------------------------------------------------------------

# Fields that carry no structural meaning and should be ignored when comparing.
_STRIP_KEYS = frozenset({
    "description", "title", "$id", "$comment", "examples", "$schema",
})


def _resolve_refs(node: Any, defs: dict[str, Any], seen: set[str] | None = None) -> Any:
    """Recursively inline $ref / $defs."""
    if seen is None:
        seen = set()

    if isinstance(node, dict):
        if "$ref" in node:
            ref = node["$ref"]
            if ref.startswith("#/$defs/"):
                name = ref[len("#/$defs/"):]
                if name in defs and name not in seen:
                    seen = seen | {name}
                    return _resolve_refs(copy.deepcopy(defs[name]), defs, seen)
            # Unresolvable ref -- keep as-is
            return node

        out: dict[str, Any] = {}
        local_defs = node.get("$defs", defs)  # prefer local $defs scope
        for k, v in node.items():
            if k == "$defs":
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
        if out.get("default") is None:
            out.pop("default", None)

        return out

    if isinstance(node, list):
        return [_strip_metadata(item) for item in node]

    return node


def _normalise_nullable(node: Any) -> Any:
    """
    Normalise the various representations of nullable types into a single
    canonical form so that Pydantic output and TypeBox output compare equal.

    Canonical form for nullable X:  {"anyOf": [X, {"type": "null"}]}
    """
    if isinstance(node, dict):
        # type: ["string", "null"]  -->  anyOf
        if "type" in node and isinstance(node["type"], list):
            types = list(node["type"])
            has_null = "null" in types
            non_null = [t for t in types if t != "null"]
            base = dict(node)
            del base["type"]
            if len(non_null) == 1:
                variant = {**base, "type": non_null[0]}
            else:
                variant = {**base, "type": non_null}
            if has_null:
                return _normalise_nullable({"anyOf": [variant, {"type": "null"}]})
            return _normalise_nullable(variant)

        out = {}
        for k, v in node.items():
            if k == "oneOf":
                # oneOf -> anyOf for comparison
                out["anyOf"] = _normalise_nullable(v)
            else:
                out[k] = _normalise_nullable(v)
        return out

    if isinstance(node, list):
        return [_normalise_nullable(item) for item in node]

    return node


def _sort_canonical(node: Any) -> Any:
    """Sort dict keys and union variant lists for stable comparison."""
    if isinstance(node, dict):
        out = {}
        for k in sorted(node.keys()):
            if k in ("anyOf", "oneOf"):
                items = [_sort_canonical(i) for i in node[k]]
                items.sort(key=lambda x: json.dumps(x, sort_keys=True))
                out[k] = items
            elif k == "required" and isinstance(node[k], list):
                out[k] = sorted(node[k])
            else:
                out[k] = _sort_canonical(node[k])
        return out

    if isinstance(node, list):
        return [_sort_canonical(item) for item in node]

    return node


def prepare(schema: Any) -> Any:
    """Full normalisation pipeline."""
    defs = schema.get("$defs", {}) if isinstance(schema, dict) else {}
    s = _resolve_refs(schema, defs)
    s = _strip_metadata(s)
    s = _normalise_nullable(s)
    s = _sort_canonical(s)
    return s


# ---------------------------------------------------------------------------
# Deep comparison
# ---------------------------------------------------------------------------

def diff(original: Any, generated: Any, path: str = "") -> list[str]:
    """Return human-readable list of structural differences."""
    errs: list[str] = []
    if type(original) is not type(generated):
        errs.append(
            f"{path}: type mismatch: "
            f"{type(original).__name__}({_trunc(original)}) vs "
            f"{type(generated).__name__}({_trunc(generated)})"
        )
        return errs

    if isinstance(original, dict):
        all_keys = sorted(set(original) | set(generated))
        for k in all_keys:
            child_path = f"{path}.{k}" if path else k
            if k not in generated:
                errs.append(f"{child_path}: missing in generated")
            elif k not in original:
                errs.append(f"{child_path}: unexpected in generated")
            else:
                errs.extend(diff(original[k], generated[k], child_path))

    elif isinstance(original, list):
        if len(original) != len(generated):
            errs.append(
                f"{path}: array length {len(original)} vs {len(generated)}"
            )
        for i, (a, b) in enumerate(zip(original, generated)):
            errs.extend(diff(a, b, f"{path}[{i}]"))

    else:
        if original != generated:
            errs.append(f"{path}: {_trunc(original)} vs {_trunc(generated)}")

    return errs


def _trunc(v: Any, n: int = 120) -> str:
    s = repr(v)
    return s if len(s) <= n else s[:n] + "..."


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python verify_schema.py <schema.json> <generated_dir>")
        sys.exit(2)

    schema_path = Path(sys.argv[1])
    generated_dir = Path(sys.argv[2]).resolve()

    # Load original schema
    with open(schema_path) as f:
        original = json.load(f)

    # Make the generated package importable
    sys.path.insert(0, str(generated_dir.parent))
    pkg = generated_dir.name

    try:
        sm = importlib.import_module(f"{pkg}._schema_map")
    except Exception as exc:
        print(f"Failed to import {pkg}._schema_map: {exc}")
        sys.exit(2)

    schema_map: dict = sm.SCHEMA_MAP  # type: ignore[attr-defined]

    total_procs = 0
    all_errors: list[str] = []

    for svc_name, svc_data in original.get("services", {}).items():
        if svc_name not in schema_map:
            all_errors.append(f"[{svc_name}] service missing from schema map")
            continue

        for proc_name, proc_data in svc_data.get("procedures", {}).items():
            total_procs += 1
            svc_map = schema_map[svc_name].get("procedures", {})
            if proc_name not in svc_map:
                all_errors.append(f"[{svc_name}.{proc_name}] procedure missing from schema map")
                continue

            gen = svc_map[proc_name]

            # Procedure type
            if proc_data.get("type") != gen.get("type"):
                all_errors.append(
                    f"[{svc_name}.{proc_name}] type: "
                    f"{proc_data.get('type')!r} vs {gen.get('type')!r}"
                )

            # Compare each schema facet
            for facet in ("input", "output", "errors"):
                orig_facet = proc_data.get(facet)
                gen_adapter = gen.get(facet)
                if orig_facet is None and gen_adapter is None:
                    continue
                if orig_facet is None:
                    all_errors.append(
                        f"[{svc_name}.{proc_name}.{facet}] "
                        f"original has no {facet} but generated does"
                    )
                    continue
                if gen_adapter is None:
                    all_errors.append(
                        f"[{svc_name}.{proc_name}.{facet}] "
                        f"generated has no {facet} but original does"
                    )
                    continue

                try:
                    gen_schema = gen_adapter.json_schema()
                except Exception as exc:
                    all_errors.append(
                        f"[{svc_name}.{proc_name}.{facet}] "
                        f"json_schema() failed: {exc}"
                    )
                    continue

                errs = diff(prepare(orig_facet), prepare(gen_schema))
                for e in errs:
                    all_errors.append(f"[{svc_name}.{proc_name}.{facet}] {e}")

    # Report
    if all_errors:
        print(f"\\nVERIFICATION FAILED: {len(all_errors)} error(s) across {total_procs} procedures\\n")
        for e in all_errors[:100]:
            print(f"  - {e}")
        if len(all_errors) > 100:
            print(f"  ... and {len(all_errors) - 100} more")
        sys.exit(1)
    else:
        print(f"\\nVERIFICATION PASSED: all {total_procs} procedures match\\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
`;
