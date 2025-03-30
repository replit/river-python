#!/usr/bin/env python

import os
import sys


def raise_err(code: int) -> None:
    if code > 0:
        sys.exit(1)


def main() -> None:
    fix = ["--fix"] if "--fix" in sys.argv else []
    watch = ["--watch"] if "--watch" in sys.argv else []
    raise_err(os.system(" ".join(["ruff", "check", "src", "scripts", "tests"] + fix)))
    raise_err(os.system("ruff format src scripts tests"))
    raise_err(os.system("mypy src tests"))
    raise_err(os.system(" ".join(["pyright"] + watch + ["src", "tests"])))
