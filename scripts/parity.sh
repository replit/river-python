#!/usr/bin/env bash
#
# parity.sh: Generate Pydantic and TypedDict models and check for deep equality.
#            This script expects that ai-infra is cloned alongside river-python.

set -e

scripts="$(dirname "$0")"
cd "${scripts}/.."

root="$(mktemp -d --tmpdir 'river-codegen-parity.XXX')"
mkdir "$root/src"

echo "Using $root" >&2

function cleanup {
  if [ -z "${DEBUG}" ]; then
    echo "Cleaning up..." >&2
    rm -rfv "${root}" >&2
  fi
}
trap "cleanup" 0 2 3 15

gen() {
  fname="$1"; shift
  name="$1"; shift
  poetry run python -m replit_river.codegen \
    client \
    --output "${root}/src/${fname}" \
    --client-name "${name}" \
    ../ai-infra/pkgs/pid2_client/src/schema/schema.json \
    "$@"
}

gen tyd.py Pid2TypedDict --typed-dict-inputs
gen pyd.py Pid2Pydantic

PYTHONPATH="${root}/src:${scripts}"
poetry run bash -c "MYPYPATH='$PYTHONPATH' mypy -m parity.check_parity"
poetry run bash -c "PYTHONPATH='$PYTHONPATH' python -m parity.check_parity"
