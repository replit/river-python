#!/bin/bash

set -e

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

(cd server && pnpm i && pnpm run --silent build > ../test_schema.json)

echo "Generating client using the above generated JSON schema..."
python -m river.codegen client --output client/test_river.py --client-name TestClient ./test_schema.json