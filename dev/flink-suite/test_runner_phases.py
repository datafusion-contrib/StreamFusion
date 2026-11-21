"""Keep suite preparation ordered and retain audit failures after unsuccessful test execution."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RunnerPhasesTest(unittest.TestCase):
    def invoke_all(self, version, failed_suite):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "suite with spaces"
            (root / "bin").mkdir(parents=True)
            shutil.copy2(ROOT / "bin/flink-suite.sh", root / "bin/flink-suite.sh")
            iceberg = root / "bin/iceberg-suite.sh"
            iceberg.write_text(
                '#!/bin/bash\nprintf "iceberg:%s\\n" "${FLINK_SUITE_REUSE_BUILD:-false}"\n'
                '[[ "$FAILED_SUITE" != iceberg ]] || exit 23\n'
            )
            iceberg.chmod(0o755)
            phases = root / "dev/flink-suite/runner"
            phases.mkdir(parents=True)
            shutil.copy2(ROOT / "dev/flink-suite/runner/configure.sh", phases / "configure.sh")
            (phases / "prepare.sh").write_text(
                'printf "%s:%s\\n" "$SUITE_MODE" "${FLINK_SUITE_REUSE_BUILD:-false}"\n'
                '[[ "$SUITE_MODE" != "$FAILED_SUITE" ]] || exit 23\nexit 0\n'
            )
            return subprocess.run(
                ["bash", str(root / "bin/flink-suite.sh"), "all"],
                env={**os.environ, "FLINK_VERSION": version, "FAILED_SUITE": failed_suite,
                     "FLINK_SUITE_REUSE_BUILD": "false"},
                capture_output=True, text=True,
            )

    def test_all_reenters_the_public_runner_and_preserves_build_reuse(self):
        for version in ("1.18.1", "2.2.1"):
            with self.subTest(version=version):
                result = self.invoke_all(version, "")
                self.assertEqual(0, result.returncode, result.stderr)
                expected = ["formats:false", "parquet:true", "orc:true", "runtime:true", "state:true", "paimon:false"]
                if version == "2.2.1":
                    expected.extend(["delta:false", "iceberg:false"])
                self.assertEqual(expected + ["kafka:false"], result.stdout.splitlines())

    def test_all_propagates_a_child_failure_and_stops_later_suites(self):
        for version in ("1.18.1", "2.2.1"):
            suites = ["formats", "parquet", "orc", "runtime", "state", "paimon"]
            if version == "2.2.1":
                suites.extend(["delta", "iceberg"])
            suites.append("kafka")
            for index, failed_suite in enumerate(suites):
                with self.subTest(version=version, failed_suite=failed_suite):
                    result = self.invoke_all(version, failed_suite)
                    self.assertEqual(23, result.returncode, result.stderr)
                    self.assertEqual(suites[:index + 1], [row.split(":")[0] for row in result.stdout.splitlines()])

    def invoke(self, configure, execute, audit):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bin").mkdir()
            shutil.copy2(ROOT / "bin/flink-suite.sh", root / "bin/flink-suite.sh")
            phases = root / "dev/flink-suite/runner"
            phases.mkdir(parents=True)
            for name, body in {
                "configure": 'readonly SHARED_VALUE="value with spaces"\necho configure\n' + configure,
                "prepare": '[[ "$SHARED_VALUE" == "value with spaces" ]] || exit 91\necho prepare\n',
                "execute": 'echo execute\n' + execute,
                "audit": 'echo "audit:${TEST_STATUS}"\n' + audit,
            }.items():
                (phases / (name + ".sh")).write_text(body)
            return subprocess.run(["bash", str(root / "bin/flink-suite.sh"), "runtime"],
                                  env={**os.environ}, capture_output=True, text=True)

    def test_unsuccessful_tests_still_reach_the_audit_with_their_exit_status(self):
        result = self.invoke("", "readonly TEST_STATUS=23\n", 'exit "$TEST_STATUS"\n')
        self.assertEqual(23, result.returncode, result.stderr)
        self.assertEqual(["configure", "prepare", "execute", "audit:23"], result.stdout.splitlines())

    def test_invalid_configuration_stops_before_preparation(self):
        result = self.invoke("exit 17\n", "readonly TEST_STATUS=0\n", "exit 0\n")
        self.assertEqual(17, result.returncode, result.stderr)
        self.assertEqual(["configure"], result.stdout.splitlines())

    def test_audit_failure_cannot_be_hidden_by_successful_tests(self):
        result = self.invoke("", "readonly TEST_STATUS=0\n", "exit 19\n")
        self.assertEqual(19, result.returncode, result.stderr)
        self.assertEqual("audit:0", result.stdout.splitlines()[-1])


if __name__ == "__main__":
    unittest.main()
