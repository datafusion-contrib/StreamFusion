"""Exercise stable commands in foreign working directories and minimal workflow checkouts."""

import ast
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
ENVIRONMENT = {**os.environ, "GITHUB_EVENT_NAME": "tool-entrypoint-test"}


class ToolEntrypointsTest(unittest.TestCase):
    def test_every_python_command_loads_from_an_unrelated_working_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            for command in sorted((ROOT / "bin").glob("*.py")):
                with self.subTest(command=command.name):
                    args = [] if command.name in ("check-native-tls.py", "check-native-glibc.py") else ["--help"]
                    result = subprocess.run([sys.executable, str(command), *args], cwd=directory,
                                            env=ENVIRONMENT, capture_output=True, text=True, timeout=15)
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_ci_plan_sparse_checkout_includes_its_python_implementation(self):
        workflow = (ROOT / ".github/workflows/ci-plan.yml").read_text()
        checkout = workflow.split("          sparse-checkout: |\n", 1)[1].split("      - id:", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for row in checkout.splitlines():
                relative = row.strip().lstrip("/")
                if not relative:
                    continue
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)
            result = subprocess.run([sys.executable, str(root / "bin/ci-plan.py"), "--help"],
                                    cwd=directory, env=ENVIRONMENT, capture_output=True, text=True, timeout=15)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_command_targets_and_internal_libraries_have_one_owner(self):
        targets = []
        for command in (ROOT / "bin").glob("*.py"):
            tree = ast.parse(command.read_text())
            imports = [node.module for node in tree.body if isinstance(node, ast.ImportFrom)
                       and node.module.startswith("dev.tools.")]
            self.assertEqual(1, len(imports), command.name)
            targets.extend(imports)
            self.assertTrue(callable(importlib.import_module(imports[0]).main))
        self.assertEqual(len(targets), len(set(targets)))
        self.assertFalse(list((ROOT / "bin").glob("test_*.py")))
        self.assertFalse(list((ROOT / "bin").glob("*.java")))
        self.assertTrue((ROOT / "dev/profiling/CiJfrEvents.java").is_file())

    def test_trusted_fork_reporting_tests_run_in_their_minimal_checkout(self):
        workflow = (ROOT / ".github/workflows/fork-ci-status.yml").read_text()
        checkout = workflow.split("          sparse-checkout: |\n", 1)[1].split("      - name:", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for row in checkout.splitlines():
                relative = row.strip().lstrip("/")
                if not relative:
                    continue
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)
            result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s",
                                     "dev/tools/tests", "-p", "test_fork_ci_status.py"],
                                    cwd=directory, env=ENVIRONMENT, capture_output=True, text=True, timeout=15)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
