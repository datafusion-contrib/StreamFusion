"""Wrapper-less Kafka must not inherit an incompatible host Maven."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"


class KafkaMavenTest(unittest.TestCase):
    def test_dependency_setup_precedes_compilation_and_keeps_the_pinned_environment(self):
        source = (RUNNER.parents[1] / "dev/flink-suite/runner/prepare.sh").read_text()
        functions = source[source.index("flink_mvn() {"):source.index('mkdir -p "${SUITE_ROOT}"')]
        start = source.index('  if [[ "${SUITE_MODE}" == "kafka" ]]; then\n')
        end = source.index('  if [[ "${SUITE_MODE}" == "paimon" ]]; then\n', start)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            flink, kafka, commands = (root / name for name in ("flink with spaces", "kafka with spaces", "commands"))
            for path in (flink, kafka, commands):
                path.mkdir()
            recorder = (
                '#!/bin/bash\nprintf "%s\\n" "$MAVEN_USER_HOME" "$PWD" "$@" >> "$CALL_LOG"\n'
                'printf "end\\n" >> "$CALL_LOG"\n'
            )
            python = commands / "python3"
            python.write_text(recorder + 'exit "$RESOLVE_STATUS"\n')
            python.chmod(0o755)
            wrapper = recorder + 'exit "$COMPILE_STATUS"\n'
            (flink / "mvnw").write_text(wrapper)
            (flink / "mvnw").chmod(0o755)
            for kafka_wrapper in (False, True):
                if kafka_wrapper:
                    (kafka / "mvnw").write_text(wrapper)
                    (kafka / "mvnw").chmod(0o755)
                for resolve_status, compile_status in ((0, 0), (17, 0), (0, 23)):
                    with self.subTest(kafka_wrapper=kafka_wrapper, resolve=resolve_status, compile=compile_status):
                        call_log = root / "calls"
                        call_log.write_text("")
                        result = subprocess.run(
                            ["bash", "-c", functions + source[start:end]],
                            env={**os.environ, "FLINK_ROOT": str(flink), "KAFKA_CONNECTOR_ROOT": str(kafka),
                                 "SUITE_MAVEN_USER_HOME": str(root / "wrapper home"), "CALL_LOG": str(call_log),
                                 "SUITE_MAVEN_REPO": str(root / "isolated m2"), "SUITE_MODE": "kafka",
                                 "FLINK_VERSION": "2.2.1" if kafka_wrapper else "1.18.1",
                                 "REPO_ROOT": str(RUNNER.parents[1]), "MAVEN_SETTINGS": str(root / "settings.xml"),
                                 "RESOLVE_STATUS": str(resolve_status), "COMPILE_STATUS": str(compile_status),
                                 "PATH": str(commands) + os.pathsep + os.environ["PATH"]},
                            capture_output=True, text=True,
                        )
                        self.assertEqual(resolve_status or compile_status, result.returncode, result.stderr)
                        calls = [call.splitlines() for call in call_log.read_text().split("end\n")[:-1]]
                        self.assertEqual(1 if resolve_status else 2, len(calls))
                        version = "2.2.1" if kafka_wrapper else "1.18.1"
                        self.assertEqual([
                            str(root / "wrapper home"), str(kafka if kafka_wrapper else flink),
                            str(RUNNER.parents[1] / "bin/ci-maven-dependencies.py"), "--maven", "./mvnw", "--graph",
                            "--offline-first", "--repository", str(root / "isolated m2"),
                            "--pom", str(kafka / "flink-connector-kafka/pom.xml"), "--define", f"flink.version={version}",
                        ], calls[0])
                        if not resolve_status:
                            self.assertEqual([
                                str(root / "wrapper home"), str(kafka if kafka_wrapper else flink),
                                "-B", "-ntp", "-s", str(root / "settings.xml"),
                                "-f", str(kafka / "flink-connector-kafka/pom.xml"),
                                f"-Dmaven.repo.local={root / 'isolated m2'}", f"-Dflink.version={version}",
                                "-DskipTests", "test-compile",
                            ], calls[1])

    def invoke(self, kafka_wrapper):
        source = (RUNNER.parents[1] / "dev/flink-suite/runner/prepare.sh").read_text()
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

    def test_prepare_resolves_the_kafka_model_with_its_released_wrapper(self):
        source = (RUNNER.parents[1] / "dev/flink-suite/runner/prepare.sh").read_text()
        start = source.index('if [[ "${SUITE_MODE}" == "kafka" ||\n')
        end = source.index('if [[ "${SUITE_MODE}" == "paimon" ]]; then', start)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            kafka = root / "kafka checkout"
            (kafka / ".git").mkdir(parents=True)
            for name, contents in (
                ("git", '#!/bin/bash\nexit 0\n'),
                ("python3", '#!/bin/bash\nprintf "%s\n" "$MAVEN_USER_HOME" "$PWD" "$@"\nexit "$RESOLVE_STATUS"\n'),
            ):
                executable = root / name
                executable.write_text(contents)
                executable.chmod(0o755)
            wrapper = kafka / "mvnw"
            wrapper.touch()
            expected = [
                "Preparing the released Kafka Maven wrapper and connector model...",
                str(root / "wrapper home"), str(kafka), str(RUNNER.parents[1] / "bin/ci-maven-dependencies.py"),
                "--maven", "./mvnw", "--model-only", "--repository", str(root / "isolated m2"),
                "--pom", str(kafka / "flink-connector-kafka/pom.xml"), "--define", "flink.version=2.2.1",
            ]
            for mode, version, executable, status in (
                ("prepare", "3.2.0", True, 0),
                ("prepare", "5.0.0", False, 0),
                ("prepare", "5.0.0", True, 0),
                ("prepare", "5.0.0", True, 17),
                ("kafka", "5.0.0", True, 0),
            ):
                with self.subTest(mode=mode, version=version, executable=executable, resolve_status=status):
                    wrapper.chmod(0o755 if executable else 0o644)
                    result = subprocess.run(
                        ["bash", "-c", source[start:end]],
                        env={**os.environ, "SUITE_MODE": mode, "KAFKA_CONNECTOR_ROOT": str(kafka),
                             "KAFKA_CONNECTOR_VERSION": version, "FLINK_VERSION": "2.2.1",
                             "SUITE_MAVEN_USER_HOME": str(root / "wrapper home"),
                             "SUITE_MAVEN_REPO": str(root / "isolated m2"),
                             "REPO_ROOT": str(RUNNER.parents[1]), "RESOLVE_STATUS": str(status),
                             "PATH": str(root) + os.pathsep + os.environ["PATH"]},
                        capture_output=True, text=True,
                    )
                    self.assertEqual(status, result.returncode, result.stderr)
                    prepare_wrapper = mode == "prepare" and version != "3.2.0" and executable
                    self.assertEqual(expected if prepare_wrapper else [], result.stdout.splitlines())

    def test_wrapperless_release_uses_pinned_flink_maven_and_propagates_failure(self):
        self.invoke(False)

    def test_release_wrapper_takes_precedence_and_propagates_failure(self):
        self.invoke(True)


if __name__ == "__main__":
    unittest.main()
