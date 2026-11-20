#!/usr/bin/env python3
"""Compatibility entry point; implementation lives in dev.tools.checks.expression_protocol."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dev.tools.checks.expression_protocol import main

if __name__ == "__main__":
    raise SystemExit(main())
