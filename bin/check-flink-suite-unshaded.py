#!/usr/bin/env python3
"""Compatibility entry point; implementation lives in dev.tools.checks.flink_suite_unshaded."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dev.tools.checks.flink_suite_unshaded import main

if __name__ == "__main__":
    raise SystemExit(main())
