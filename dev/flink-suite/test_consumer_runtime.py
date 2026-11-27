"""Prepare the actual consumer models before any offline test invocation."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "dev/flink-suite/runner"
NS = "http://maven.apache.org/POM/4.0.0"


class ConsumerRuntimeTest(unittest.TestCase):
    def invoke(self, mode, version, failed_pom="", reuse="true"):
        with tempfile.TemporaryDirectory(prefix="consumer runtime ") as directory:
            root = Path(directory).resolve()
            line = "1.18" if version == "1.18.1" else "2.2"
            suite = root / line
            suite.mkdir()
            log = root / "calls"
            record = 'printf "%s\\n" "$PWD" "${MAVEN_USER_HOME:-ambient}" "$@" "<END>" >> "$CALL_LOG"\n'
            for name in (f"flink-{version}", "flink-connector-kafka-5.0.0"):
                checkout = suite / name
                checkout.mkdir()
                wrapper = checkout / "mvnw"
                wrapper.write_text('#!/bin/bash\nset -- test "$@"\n' + record)
                wrapper.chmod(0o755)
            (suite / "flink-connector-kafka-3.2.0").mkdir()
            calcite = suite / "m2/org/apache/calcite/calcite-core/1/calcite-core-1.jar"
            calcite.parent.mkdir(parents=True)
            calcite.touch()
            (suite / "streamfusion-classpath.txt").write_text(str(root / "native.jar"))
            paimon_version = "2.0.0" if line == "1.18" else "2.1.0"
            paimon = suite / f"paimon-{paimon_version}"
            for module in ("common", "1.18"):
                path = paimon / f"paimon-flink/paimon-flink-{module}"
                (path / "target/test-classes").mkdir(parents=True)
                (path / "pom.xml").write_text(
                    f'<project xmlns="{NS}"><parent><artifactId>parent</artifactId></parent>'
                    f'<artifactId>paimon-flink-{module}</artifactId></project>')
            runtime = suite / f"m2/org/apache/paimon/paimon-flink-1.18/{paimon_version}/paimon-flink-1.18-{paimon_version}.jar"
            runtime.parent.mkdir(parents=True)
            runtime.touch()
            command = '''
set -uo pipefail
source "$REPO_ROOT/dev/flink-suite/runner/configure.sh" "$TEST_MODE"
record() { RECORD_BODY }
python3() {
  case "$1" in
    */ci-maven-dependencies.py)
      shift
      record prepare "$@"
      if [[ -n "$FAILED_POM" && "$*" == *"$FAILED_POM"* ]]; then return 17; fi
      ;;
    */summarize.py) return 0 ;;
    *) command python3 "$@" ;;
  esac
}
mvn() { record test "$@"; }
PREPARE_FUNCTIONS
source "$REPO_ROOT/dev/flink-suite/runner/execute.sh"
exit "$TEST_STATUS"
'''.replace("RECORD_BODY", record)
            source = (RUNNER / "prepare.sh").read_text()
            command = command.replace("PREPARE_FUNCTIONS", source[
                source.index("flink_mvn() {"):source.index("prepare_kafka_maven() {")])
            result = subprocess.run(
                ["bash", "-c", command], cwd=root, capture_output=True, text=True,
                env={**os.environ, "REPO_ROOT": str(ROOT), "FLINK_SUITE_ROOT": str(root),
                     "TEST_MODE": mode, "FLINK_VERSION": version, "FAILED_POM": failed_pom,
                     "FLINK_SUITE_REUSE_COMMON_BUILD": reuse, "CALL_LOG": str(log)},
            )
            calls = [call.splitlines() for call in log.read_text().split("<END>\n")[:-1]]
            return result, calls

    def test_actual_models_wrappers_and_properties_are_prepared_before_offline_tests(self):
        for mode, version in (("parquet", "2.2.1"), ("kafka", "2.2.1"), ("kafka", "1.18.1"),
                              ("paimon", "2.2.1"), ("paimon", "1.18.1")):
            with self.subTest(mode=mode, version=version):
                result, calls = self.invoke(mode, version)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                preparations = [call for call in calls if call[2] == "prepare"]
                tests = [call for call in calls if call[2] == "test"]
                self.assertEqual(2 if mode == "paimon" and version == "1.18.1" else 1, len(preparations))
                for index, preparation in enumerate(preparations):
                    arguments = preparation[preparation.index("--test-runtime") + 1:]
                    self.assertIn("-o", arguments)
                    self.assertIn("-Dmaven.test.failure.ignore=false", arguments)
                    expected = next(test for test in tests if test[test.index("-f") + 1]
                                    == arguments[arguments.index("-f") + 1])
                    self.assertLess(calls.index(preparation), calls.index(expected))
                    self.assertEqual(arguments, expected[3:-1])
                    self.assertEqual(preparation[:2], expected[:2])
                    if mode == "paimon":
                        self.assertEqual("mvn", preparation[preparation.index("--maven") + 1])
                        self.assertIn("-Dpaimon-flink-common.flink.version=" + version, arguments)
                        if version == "1.18.1":
                            self.assertIn("-Pflink1", arguments)
                            self.assertLess(arguments.index("-Dpaimon-flink-common.flink.version=1.20.1"),
                                            arguments.index("-Dpaimon-flink-common.flink.version=1.18.1"))
                            self.assertTrue(arguments[arguments.index("-f") + 1].endswith(
                                "paimon-line-pom.xml" if index else "paimon-runtime-pom.xml"))
                    else:
                        self.assertEqual("./mvnw", preparation[preparation.index("--maven") + 1])
                        self.assertTrue(preparation[0].endswith(
                            "flink-connector-kafka-5.0.0" if mode == "kafka" and version == "2.2.1"
                            else "flink-" + version))
                        if mode == "kafka":
                            self.assertIn("-Dflink.version=" + version, arguments)
                        else:
                            self.assertIn("-Pjava11-target", arguments)

    def test_preparation_failure_blocks_the_corresponding_test_invocation(self):
        for mode, version, failed_pom, executed in (
                ("parquet", "2.2.1", "pom.xml", 0), ("kafka", "2.2.1", "pom.xml", 0),
                ("paimon", "2.2.1", "pom.xml", 0),
                ("paimon", "1.18.1", "paimon-runtime-pom.xml", 0),
                ("paimon", "1.18.1", "paimon-line-pom.xml", 1)):
            with self.subTest(mode=mode, version=version, failed_pom=failed_pom):
                result, calls = self.invoke(mode, version, failed_pom)
                self.assertEqual(17, result.returncode, result.stdout + result.stderr)
                self.assertEqual(executed, sum(call[2] == "test" for call in calls))

    def test_nonshared_consumer_keeps_its_existing_preparation_policy(self):
        result, calls = self.invoke("parquet", "2.2.1", reuse="false")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["test"], [call[2] for call in calls])


if __name__ == "__main__":
    unittest.main()
