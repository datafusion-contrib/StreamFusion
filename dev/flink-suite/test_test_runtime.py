"""Dependency preparation must use the transferred repository and block incomplete builds."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import xml.etree.ElementTree as ET


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"
ROOT = RUNNER.parents[1]
FIXTURE = ROOT / "dev/flink-suite/runtime-fixture"


def fixture_settings(directory):
    seed = os.environ.get("SF_RUNTIME_TEST_MAVEN_SEED")
    if not seed:
        return ROOT / "dev/flink-suite/settings.xml"
    seed = Path(seed).resolve()
    if not seed.is_dir():
        raise ValueError(f"Missing shared Maven fixture cache: {seed}")
    settings = ET.Element("settings")
    mirrors = ET.SubElement(settings, "mirrors")
    mirror = ET.SubElement(mirrors, "mirror")
    ET.SubElement(mirror, "id").text = "shared-runtime-fixture-cache"
    ET.SubElement(mirror, "mirrorOf").text = "*"
    ET.SubElement(mirror, "url").text = seed.as_uri()
    path = Path(directory) / "settings.xml"
    ET.ElementTree(settings).write(path, encoding="utf-8")
    return path


@contextmanager
def unavailable_repository(directory, original_settings):
    requests = []

    class Unavailable(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_error(503)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Unavailable)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    namespace = {"m": "http://maven.apache.org/SETTINGS/1.0.0"}
    original = ET.parse(original_settings)
    identifier = (original.findtext("m:mirrors/m:mirror/m:id", namespaces=namespace)
                  or original.findtext("mirrors/mirror/id", "central"))
    settings = ET.Element("settings")
    mirror = ET.SubElement(ET.SubElement(settings, "mirrors"), "mirror")
    ET.SubElement(mirror, "id").text = identifier
    ET.SubElement(mirror, "mirrorOf").text = "*"
    ET.SubElement(mirror, "url").text = f"http://127.0.0.1:{server.server_port}"
    path = Path(directory) / "unavailable-settings.xml"
    ET.ElementTree(settings).write(path, encoding="utf-8")
    try:
        yield str(path), requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class TestRuntimePreparationTest(unittest.TestCase):
    def test_real_providers_start_offline_after_isolated_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = str(fixture_settings(directory))
            for cohort in json.loads((FIXTURE / "cohorts.json").read_text()):
                line = cohort["line"]
                surefire = cohort["surefire"]
                platform = cohort["platform"]
                original_opentest = cohort["original_opentest"]
                with self.subTest(line=line):
                    root = Path(directory) / line
                    source = root / "src/test/java"
                    source.mkdir(parents=True)
                    repository = root / "m2"
                    pom = root / "pom.xml"
                    shutil.copyfile(FIXTURE / "pom.xml", pom)
                    (source / "JupiterTest.java").write_text(
                        'class JupiterTest { @org.junit.jupiter.api.Test void runs() {} }')
                    (source / "VintageTest.java").write_text(
                        'public class VintageTest { @org.junit.Test public void runs() {} }')
                    base = ["mvn", "-B", "-ntp", "-s", settings, "-gs", settings,
                            "-f", str(pom), "-Dmaven.repo.local=" + str(repository),
                            "-Dfixture.junit.version=" + cohort["junit"],
                            "-Dfixture.surefire.version=" + surefire]

                    def run(command, succeeds=True, timeout=600):
                        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
                        if succeeds:
                            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                        else:
                            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
                        return result.stdout + result.stderr

                    run(base + ["-DskipTests", "test"])
                    # Model missing runtime artifacts after a successful skipped-test build.
                    run(base + ["org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get",
                                "-Dartifact=org.apache.maven.surefire:surefire-junit-platform:" +
                                (surefire if line == "kafka-5.0" else "3.2.2"),
                                "-Dtransitive=true"])
                    if line == "2.2":
                        shutil.rmtree(repository / f"org/opentest4j/opentest4j/{original_opentest}")
                    elif line == "kafka-5.0":
                        self.assertFalse((repository / f"org/junit/platform/junit-platform-launcher/{platform}"
                                          / f"junit-platform-launcher-{platform}.jar").exists())
                    else:
                        self.assertFalse((repository / f"org/apache/maven/surefire/surefire-junit-platform/{surefire}"
                                          / f"surefire-junit-platform-{surefire}.jar").exists())
                    failure = run(base + ["-o", "surefire:test@integration-tests"], succeeds=False)
                    missing = {"2.2": "opentest4j", "kafka-5.0": "junit-platform-launcher"}
                    self.assertRegex(failure, missing.get(line, "surefire-junit-platform|JUnitPlatformProvider"))
                    run([sys.executable, str(ROOT / "bin/ci-maven-dependencies.py"),
                         "--maven", "mvn", "--settings", settings, "--repository", str(repository),
                         "--artifact", f"org.apache.maven.surefire:surefire-junit-platform:{surefire}",
                         "--artifact", "org.junit.platform:junit-platform-launcher:" + platform,
                         "--artifact", "org.apache.httpcomponents:httpclient:4.4.1"])
                    self.assertTrue((repository / f"org/opentest4j/opentest4j/{original_opentest}"
                                     / f"opentest4j-{original_opentest}.jar").exists())
                    self.assertTrue((repository / "org/apache/httpcomponents/httpcomponents-client/4.4.1"
                                     / "httpcomponents-client-4.4.1.pom").is_file())
                    run(base + ["-o", "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:get",
                                "-Dartifact=org.apache.httpcomponents:httpclient:4.4.1",
                                "-Dtransitive=true"])
                    run(base + ["-o", "surefire:test@integration-tests"])
                    if line == "1.18":
                        with unavailable_repository(root, settings) as (unavailable, requests):
                            consumer = list(base)
                            for flag in ("-s", "-gs"):
                                consumer[consumer.index(flag) + 1] = unavailable
                            consumer.extend(["-e", "-Dmaven.wagon.http.retryHandler.count=0",
                                             "-Dmaven.wagon.rto=1000",
                                             "-Daether.connector.http.retryHandler.count=0",
                                             "-Daether.connector.http.retryHandler.interval=1",
                                             "-Daether.transport.http.retryHandler.count=0",
                                             "-Daether.transport.http.retryHandler.interval=1"])
                            failure = run(consumer + ["surefire:test@integration-tests"],
                                          succeeds=False, timeout=30)
                            self.assertIn("ClasspathCache.setCachedClasspath", failure)
                            self.assertTrue(requests, failure)
                            attempted = len(requests)
                            for report in (root / "target/surefire-reports").glob("TEST-*.xml"):
                                report.unlink()
                            run(consumer + ["-o", "surefire:test@integration-tests"], timeout=30)
                            self.assertEqual(attempted, len(requests))
                    reports = list((root / "target/surefire-reports").glob("TEST-*.xml"))
                    self.assertEqual(2, len(reports))
                    for report in reports:
                        result = ET.parse(report).getroot()
                        self.assertEqual("1", result.get("tests"))
                        self.assertEqual("0", result.get("errors"))
                        self.assertEqual("0", result.get("failures"))
                        self.assertEqual("0", result.get("skipped"))

    def test_every_shared_build_consumer_runs_offline_with_the_prepared_settings(self):
        source = (ROOT / "dev/flink-suite/runner/execute.sh").read_text()
        start = source.index("MAVEN_TEST_ARGS=(")
        end = source.index('if [[ "${SUITE_MODE}" == "runtime" ||', start)
        command = source[start:end] + '\nprintf "%s\\n" "${MAVEN_TEST_ARGS[@]}"'
        for mode in ("runtime", "diagnostic", "state", "formats", "parquet", "orc",
                     "kafka", "paimon", "delta"):
            for reuse in ("true", "false"):
                with self.subTest(mode=mode, reuse=reuse):
                    result = subprocess.run(
                        ["bash", "-u", "-c", command], capture_output=True, text=True,
                        env={**os.environ, "SUITE_MODE": mode, "FLINK_SUITE_REUSE_COMMON_BUILD": reuse,
                             "MAVEN_SETTINGS": "suite settings.xml", "SUITE_MAVEN_REPO": "suite m2",
                             "TEST_MODULES": "upstream-module", "STREAMFUSION_CLASSPATH": "native.jar",
                             "NATIVE_REPORT_ROOT": "native reports"},
                    )
                    self.assertEqual(0, result.returncode, result.stderr)
                    arguments = result.stdout.splitlines()
                    self.assertEqual(reuse == "true", "-o" in arguments)
                    self.assertEqual(reuse == "true", "-gs" in arguments)
                    if reuse == "true":
                        self.assertEqual("suite settings.xml", arguments[arguments.index("-gs") + 1])
                    self.assertIn("-Dmaven.repo.local=suite m2", arguments)
                    self.assertIn("-Dmaven.test.failure.ignore=false", arguments)

    def test_each_line_prepares_its_own_runtime_and_preserves_download_failure(self):
        source = (RUNNER.parents[1] / "dev/flink-suite/runner/prepare.sh").read_text()
        delta_start = source.index("prepare_delta_maven() {")
        delta_end = source.index("\n}\n", delta_start) + len("\n}\n")
        start = source.index('if [[ "${SUITE_MODE}" == "prepare" ]]; then\n')
        end = len(source)
        command = source[delta_start:delta_end] + source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            executable = root / "python3"
            executable.write_text(
                '#!' + sys.executable + '\n'
                'import json, os, sys\n'
                'with open(os.environ["CALLS"], "a") as output:\n'
                '    output.write(json.dumps([os.environ.get("MAVEN_USER_HOME"), os.getcwd(), sys.argv[1:]]) + "\\n")\n'
                'if sys.argv[1].endswith("check-flink-suite-unshaded.py"):\n'
                '    sys.exit(int(os.environ["ARTIFACT_STATUS"]))\n'
                'if sys.argv[1].endswith("ci-maven-dependencies.py"):\n'
                '    status = "DELTA_STATUS" if "--resolve-project" in sys.argv else "DOWNLOAD_STATUS"\n'
                '    sys.exit(int(os.environ[status]))\n'
            )
            executable.chmod(0o755)
            calls = root / "calls.jsonl"
            for line in ("2.2", "1.18"):
                suite = root / f"suite with spaces/{line}"
                (suite / "flink").mkdir(parents=True)
                outcomes = [(0, 0, 0, 0), (7, 0, 0, 0), (0, 9, 0, 0), (0, 0, 13, 0)]
                if line == "2.2":
                    outcomes.append((0, 0, 0, 17))
                for status, probe_status, artifact_status, delta_status in outcomes:
                    with self.subTest(line=line, status=status, probe_status=probe_status,
                                      artifact_status=artifact_status, delta_status=delta_status):
                        calls.unlink(missing_ok=True)
                        result = subprocess.run(
                            ["bash", "-c", 'flink_mvn() { echo "offline-probe:$*"; return "$PROBE_STATUS"; }\n' + command],
                            capture_output=True, text=True,
                            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                 "CALLS": str(calls), "DOWNLOAD_STATUS": str(status),
                                 "PROBE_STATUS": str(probe_status), "ARTIFACT_STATUS": str(artifact_status), "MAVEN_SETTINGS": "settings",
                                 "SUITE_MODE": "prepare", "FLINK_LINE": line, "FLINK_VERSION": "test",
                                 "PAIMON_VERSION": "2.0.0", "DELTA_STATUS": str(delta_status),
                                 "PAIMON_ENFORCER_ARTIFACT": "org.apache.maven.plugins:maven-enforcer-plugin:3.0.0-M1",
                                 "KAFKA_CONNECTOR_VERSION": "5.0.0" if line == "2.2" else "3.2.0",
                                 "DELTA_TEST_POM": "delta fixture/pom.xml", "DELTA_VERSION": "test-delta",
                                 "FLINK_TARGET_PROFILE": "java11-target" if line == "2.2" else "",
                                 "REPO_ROOT": str(RUNNER.parents[1]), "SUITE_ROOT": str(suite),
                                 "SUITE_MAVEN_REPO": str(suite / "m2"),
                                 "SUITE_MAVEN_USER_HOME": str(suite / "maven-wrapper"),
                                 "FLINK_ROOT": str(suite / "flink"), "CONTRACT_FILE": "contracts"},
                        )
                        self.assertEqual(artifact_status or status or delta_status or probe_status,
                                         result.returncode, result.stderr)
                        invocations = [json.loads(row) for row in calls.read_text().splitlines()]
                        guard = invocations.pop(0)[2]
                        self.assertTrue(guard[0].endswith("check-flink-suite-unshaded.py"))
                        self.assertEqual([str(suite), str(suite / "m2"), "test"], guard[1:])
                        if artifact_status:
                            self.assertEqual([], invocations)
                            self.assertNotIn("offline-probe:", result.stdout)
                            continue
                        wrapper_home, working_directory, arguments = invocations[0]
                        self.assertEqual(str(suite / "maven-wrapper"), wrapper_home)
                        self.assertEqual(str(suite / "flink"), working_directory)
                        self.assertEqual(str(suite / "m2"), arguments[arguments.index("--repository") + 1])
                        self.assertEqual(str(suite / "flink/mvnw"), arguments[arguments.index("--maven") + 1])
                        self.assertNotIn("--profile", arguments)
                        self.assertNotIn("--projects", arguments)
                        self.assertNotIn("--pom", arguments)
                        self.assertIn("--offline-first", arguments)
                        artifacts = [arguments[i + 1] for i, value in enumerate(arguments) if value == "--artifact"]
                        expected = ["org.apache.maven.surefire:surefire-junit-platform:" +
                                    ("3.0.0-M5" if line == "1.18" else "3.2.2"),
                                    "org.junit.platform:junit-platform-launcher:" +
                                    ("1.9.1" if line == "1.18" else "1.11.4"),
                                    "org.apache.httpcomponents:httpclient:4.4.1",
                                    "org.apache.paimon:paimon-vfs-hadoop:2.0.0",
                                    "org.apache.maven.plugins:maven-remote-resources-plugin:1.7.0",
                                    "org.apache.maven.plugins:maven-enforcer-plugin:3.0.0-M1"]
                        if line == "2.2":
                            expected.extend(["org.apache.maven.surefire:surefire-junit-platform:3.2.5",
                                             "org.junit.platform:junit-platform-launcher:1.13.3",
                                             "org.apache.maven.plugins:maven-surefire-plugin:3.5.3",
                                             "org.apache.maven.plugins:maven-resources-plugin:3.5.0"])
                        else:
                            expected.append("org.apache.flink:flink-connector-parent:1.0.0:pom")
                        self.assertEqual(expected, artifacts)
                        expected_calls = 1
                        if line == "2.2" and not status:
                            self.assertEqual(
                                ["--resolve-project", "--pom", "delta fixture/pom.xml",
                                 "--repository", str(suite / "m2"),
                                 "--define", "flink.version=test", "--define", "delta.version=test-delta"],
                                invocations[1][2][1:],
                            )
                            expected_calls += 1
                            if not delta_status:
                                self.assertEqual(
                                    ["--repository", str(suite / "m2"),
                                     "--artifact", "org.apache.maven.surefire:surefire-junit-platform:3.5.3",
                                     "--artifact", "org.junit.platform:junit-platform-launcher:1.11.4"],
                                    invocations[2][2][1:],
                                )
                                expected_calls += 1
                        if not (status or delta_status or probe_status):
                            expected_calls += 1
                        self.assertEqual(expected_calls, len(invocations))
                        self.assertEqual(not (status or delta_status or probe_status), "build is ready" in result.stdout)
                        if status or delta_status:
                            self.assertNotIn("offline-probe:", result.stdout)
                        else:
                            self.assertIn(" -o ", result.stdout)
                            self.assertIn("CalcITCase#testSimpleProject", result.stdout)
                            self.assertIn("-Dmaven.test.failure.ignore=false", result.stdout)


    def test_delta_consumer_tests_offline_after_the_required_preparation(self):
        source = (RUNNER.parents[1] / "dev/flink-suite/runner/execute.sh").read_text()
        start = source.index('  if [[ "${FLINK_SUITE_REUSE_COMMON_BUILD:-false}" != "true" ]]; then\n'
                             '    prepare_delta_maven || exit $?')
        end = source.index('\nelif [[ "${SUITE_MODE}" == "paimon" ]]; then', start)
        command = (
            'prepare_delta_maven() { echo prepare; return "$PREPARATION_STATUS"; }\n'
            'mvn() { printf "<%s>\n" "$@"; }\n'
            'MAVEN_SETTINGS="suite settings.xml"\n'
            'MAVEN_TEST_ARGS=(-f "delta fixture/pom.xml" test)\n' + source[start:end]
        )
        expected_test = ["<-o>", "<-gs>", "<suite settings.xml>", "<-f>", "<delta fixture/pom.xml>", "<test>"]
        for reuse, status, expected in (
                ("true", 17, expected_test),
                ("false", 0, ["prepare", *expected_test]),
                ("false", 17, ["prepare"])):
            with self.subTest(reuse=reuse, status=status):
                result = subprocess.run(
                    ["bash", "-c", command], capture_output=True, text=True,
                    env={**os.environ, "FLINK_SUITE_REUSE_COMMON_BUILD": reuse,
                         "PREPARATION_STATUS": str(status)},
                )
                self.assertEqual(0 if reuse == "true" else status, result.returncode, result.stderr)
                self.assertEqual(expected, result.stdout.splitlines())


if __name__ == "__main__":
    unittest.main()
