"""Exercise the workflow's shell commands with failing suites and dependency results."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

from runtime_shards import plan_digest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/flink-suite.yml"


def command(name):
    path = WORKFLOW
    if name in ("Run unchanged upstream suite with StreamFusion", "Require declared runtime audit routes"):
        path = WORKFLOW.with_name("flink-suite-line.yml")
    lines = path.read_text().splitlines()
    start = lines.index("      - name: " + name)
    start = next(i for i in range(start, len(lines)) if lines[i] == "        run: |") + 1
    end = start
    while end < len(lines) and (not lines[end] or lines[end].startswith("          ")):
        end += 1
    return textwrap.dedent("\n".join(lines[start:end]))


class SuiteWorkflowTest(unittest.TestCase):
    def test_group_runs_every_suite_and_preserves_any_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bin").mkdir()
            runner = root / "bin/flink-suite.sh"
            runner.write_text('#!/bin/bash\necho "$1" >> invoked\necho "report-$1"\n[[ "$1" != "$FAIL_SUITE" ]]\n')
            runner.chmod(0o755)
            suites = "formats parquet orc kafka delta"
            for failure in ("", *suites.split()):
                with self.subTest(failure=failure):
                    (root / "invoked").unlink(missing_ok=True)
                    result = subprocess.run(["bash", "-e", "-c", command("Run unchanged upstream suite with StreamFusion")],
                                            cwd=root, env={**os.environ, "SUITES": suites, "FAIL_SUITE": failure},
                                            capture_output=True, text=True)
                    self.assertEqual(bool(failure), result.returncode != 0, result.stderr)
                    self.assertEqual(suites.split(), (root / "invoked").read_text().splitlines())
                    for suite in suites.split():
                        self.assertEqual(f"report-{suite}\n", (root / f"upstream-{suite}.log").read_text())

    def test_runtime_audit_failure_survives_log_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bin").mkdir()
            runner = root / "bin/flink-suite.sh"
            runner.write_text('#!/bin/bash\necho "audit-$1"\nexit "$AUDIT_STATUS"\n')
            runner.chmod(0o755)
            for status in (0, 17):
                result = subprocess.run(
                    ["bash", "-e", "-c", command("Require declared runtime audit routes")],
                    cwd=root, env={**os.environ, "AUDIT_STATUS": str(status)}, capture_output=True)
                self.assertEqual(status, result.returncode, result.stderr)
                self.assertEqual("audit-runtime\n", (root / "upstream-runtime-audit.log").read_text())

    def test_combined_coverage_gate_requires_all_shards_on_each_line(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "dev").symlink_to(WORKFLOW.parents[2] / "dev", target_is_directory=True)
            plan = {"shards": [[name] for name in "ABCD"],
                    "expected_cases": {name: 1 for name in "ABCD"}}
            reports = []
            for line in ("2.2", "1.18"):
                for shard, name in enumerate("ABCD", 1):
                    target = root / f"runtime-coverage/{line}/runtime-coverage-{line}-{shard}"
                    target.mkdir(parents=True)
                    (target / "runtime-shards.json").write_text(json.dumps(plan))
                    report = target / "shard-coverage.json"
                    report.write_text(json.dumps({"plan": plan_digest(plan), "shard": shard,
                                                  "classes": [name], "cases": {name: 1}}))
                    reports.append(report)
            script = command("Require every runtime shard and parameterized invocation on both lines")
            self.assertEqual(0, subprocess.run(["bash", "-e", "-c", script], cwd=root,
                                              capture_output=True).returncode)
            for report in reports:
                with self.subTest(missing=report):
                    saved = report.read_text()
                    report.unlink()
                    self.assertNotEqual(0, subprocess.run(["bash", "-e", "-c", script], cwd=root,
                                                         capture_output=True).returncode)
                    report.write_text(saved)

    def test_final_gate_rejects_failed_cancelled_and_unexpected_skipped_jobs(self):
        dependencies = ("upstream-line", "legacy-delta-audit")
        for inventory in (False, True):
            for dependency in dependencies:
                for state in ("success", "failure", "cancelled", "skipped"):
                    with self.subTest(inventory=inventory, dependency=dependency, state=state):
                        results = {name: {"result": state if name == dependency else "success"}
                                   for name in dependencies}
                        result = subprocess.run(["bash", "-e", "-c", command("Require every upstream suite to succeed")],
                                                env={**os.environ, "NEEDS_JSON": json.dumps(results),
                                                     "INVENTORY": str(inventory).lower()}, capture_output=True)
                        accepted = state == "success" or (inventory and dependency == "legacy-delta-audit" and state == "skipped")
                        self.assertEqual(accepted, result.returncode == 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
