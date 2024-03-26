#!/bin/bash

set -e

# This needs to be run before running the tests.

VIRTUAL_ENV_PATH=$(poetry env info -p)
# Use the extracted path to form the full path to the protobuf include directory
PROTO_INCLUDE_PATH="${VIRTUAL_ENV_PATH}/lib/python3.11/site-packages/grpc_tools/_proto"

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
python -m grpc_tools.protoc \
  --proto_path=./proto \
  --proto_path=$PROTO_INCLUDE_PATH \
  --python_out=./server \
  --mypy_out=./server \
  --grpc_python_out=./server \
  ./proto/test.proto \
  --mypy_grpc_out=./server
sed -i '' 's/import test_pb2 as test__pb2/from . &/' server/test_pb2_grpc.py
python -m river.codegen server ./proto/test.proto --output server/
sed -i '' '1s/^/# # type: ignore\n/' server/test_river.py
poetry run ../../scripts/format.sh