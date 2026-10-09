"""Wrapper-less Kafka must not inherit an incompatible host Maven."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"


class KafkaMavenTest(unittest.TestCase):
    def invoke(self, kafka_wrapper):
        source = RUNNER.read_text()
        functions = source[source.index("flink_mvn() {"):source.index('mkdir -p "${SUITE_ROOT}"')]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            flink = root / "flink with spaces"
            kafka = root / "kafka with spaces"
            ambient = root / "ambient"
            for path in (flink, kafka, ambient):
                path.mkdir()
            wrapper = '#!/bin/bash\nprintf "%s\\n" "$MAVEN_USER_HOME" "$PWD" "$@"\nexit 7\n'
            (flink / "mvnw").write_text(wrapper)
            (flink / "mvnw").chmod(0o755)
            if kafka_wrapper:
                (kafka / "mvnw").write_text(wrapper)
                (kafka / "mvnw").chmod(0o755)
            (ambient / "mvn").write_text('#!/bin/bash\necho WRONG_HOST_MAVEN\nexit 91\n')
            (ambient / "mvn").chmod(0o755)
            result = subprocess.run(
                ["bash", "-c", functions + '\nkafka_mvn -B -f "${KAFKA_CONNECTOR_ROOT}/pom.xml" -pl flink-connector-kafka test'],
                env={**os.environ, "FLINK_ROOT": str(flink), "KAFKA_CONNECTOR_ROOT": str(kafka),
                     "SUITE_MAVEN_USER_HOME": str(root / "wrapper home"),
                     "PATH": str(ambient) + os.pathsep + os.environ["PATH"]},
                capture_output=True, text=True,
            )
            self.assertEqual(7, result.returncode, result.stderr)
            self.assertEqual(
                [str(root / "wrapper home"), str(kafka if kafka_wrapper else flink), "-B", "-f", str(kafka / "pom.xml"),
                 "-pl", "flink-connector-kafka", "test"], result.stdout.splitlines(),
            )

    def test_wrapperless_release_uses_pinned_flink_maven_and_propagates_failure(self):
        self.invoke(False)

    def test_release_wrapper_takes_precedence_and_propagates_failure(self):
        self.invoke(True)


if __name__ == "__main__":
    unittest.main()
