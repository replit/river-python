#!/bin/bash

set -ex

black .

# Some of the ruff fixes go beyond "formatting".
# Consider switching to `ruff format` if it becomes available.
ruff check --fix --show-fixes .
