"""Keep suite preparation ordered and retain audit failures after unsuccessful test execution."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RunnerPhasesTest(unittest.TestCase):
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
