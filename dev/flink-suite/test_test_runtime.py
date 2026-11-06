"""Dependency preparation must use the transferred repository and block incomplete builds."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"
ROOT = RUNNER.parents[1]


class TestRuntimePreparationTest(unittest.TestCase):
    def test_real_providers_start_offline_after_isolated_resolution(self):
        settings = str(ROOT / "dev/flink-suite/settings.xml")
        with tempfile.TemporaryDirectory() as directory:
            for line, surefire, junit, platform, original_opentest in (
                    ("2.2", "3.2.2", "5.11.4", "1.11.4", "1.2.0"),
                    ("1.18", "3.0.0-M5", "5.9.1", "1.9.1", "1.1.1"),
                    ("kafka-5.0", "3.2.5", "5.13.3", "1.13.3", "1.2.0")):
                with self.subTest(line=line):
                    root = Path(directory) / line
                    source = root / "src/test/java"
                    source.mkdir(parents=True)
                    repository = root / "m2"
                    pom = root / "pom.xml"
                    pom.write_text(f'''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>upstream-runtime</artifactId><version>1</version>
  <properties><maven.compiler.release>11</maven.compiler.release></properties>
  <dependencyManagement><dependencies><dependency>
    <groupId>org.junit</groupId><artifactId>junit-bom</artifactId><version>{junit}</version><type>pom</type><scope>import</scope>
  </dependency></dependencies></dependencyManagement>
  <dependencies>
    <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.junit.vintage</groupId><artifactId>junit-vintage-engine</artifactId><scope>test</scope></dependency>
  </dependencies>
  <build><plugins>
    <plugin><artifactId>maven-resources-plugin</artifactId><version>3.3.1</version></plugin>
    <plugin><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
    <plugin><artifactId>maven-surefire-plugin</artifactId><version>{surefire}</version>
      <configuration><forkNode implementation="org.apache.maven.plugin.surefire.extensions.SurefireForkNodeFactory"/></configuration>
      <executions><execution><id>integration-tests</id><goals><goal>test</goal></goals></execution></executions>
    </plugin>
  </plugins></build>
</project>''')
                    (source / "JupiterTest.java").write_text(
                        'class JupiterTest { @org.junit.jupiter.api.Test void runs() {} }')
                    (source / "VintageTest.java").write_text(
                        'public class VintageTest { @org.junit.Test public void runs() {} }')
                    base = ["mvn", "-B", "-ntp", "-s", settings, "-gs", settings,
                            "-f", str(pom), "-Dmaven.repo.local=" + str(repository)]

                    def run(command, succeeds=True):
                        result = subprocess.run(command, capture_output=True, text=True, timeout=600)
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
                         "--maven", "mvn", "--repository", str(repository),
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
                    reports = list((root / "target/surefire-reports").glob("TEST-*.xml"))
                    self.assertEqual(2, len(reports))
                    for report in reports:
                        result = ET.parse(report).getroot()
                        self.assertEqual("1", result.get("tests"))
                        self.assertEqual("0", result.get("errors"))
                        self.assertEqual("0", result.get("failures"))

    def test_each_line_prepares_its_own_runtime_and_preserves_download_failure(self):
        source = RUNNER.read_text()
        start = source.index('if [[ "${SUITE_MODE}" == "prepare" ]]; then\n')
        end = source.index('\nif [[ -n "${FLINK_SUITE_SHARD:-}" ]]; then', start)
        command = source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            executable = root / "python3"
            executable.write_text(
                '#!' + sys.executable + '\n'
                'import json, os, sys\n'
                'with open(os.environ["CALLS"], "a") as output:\n'
                '    output.write(json.dumps([os.environ.get("MAVEN_USER_HOME"), os.getcwd(), sys.argv[1:]]) + "\\n")\n'
                'if sys.argv[1].endswith("ci-maven-dependencies.py"):\n'
                '    sys.exit(int(os.environ["DOWNLOAD_STATUS"]))\n'
            )
            executable.chmod(0o755)
            calls = root / "calls.jsonl"
            for line in ("2.2", "1.18"):
                suite = root / f"suite with spaces/{line}"
                (suite / "flink").mkdir(parents=True)
                for status, probe_status in ((0, 0), (7, 0), (0, 9)):
                    with self.subTest(line=line, status=status, probe_status=probe_status):
                        calls.unlink(missing_ok=True)
                        result = subprocess.run(
                            ["bash", "-c", 'flink_mvn() { echo "offline-probe:$*"; return "$PROBE_STATUS"; }\n' + command],
                            capture_output=True, text=True,
                            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                 "CALLS": str(calls), "DOWNLOAD_STATUS": str(status),
                                 "PROBE_STATUS": str(probe_status), "MAVEN_SETTINGS": "settings",
                                 "SUITE_MODE": "prepare", "FLINK_LINE": line, "FLINK_VERSION": "test",
                                 "PAIMON_VERSION": "2.0.0",
                                 "FLINK_TARGET_PROFILE": "java11-target" if line == "2.2" else "",
                                 "REPO_ROOT": str(RUNNER.parents[1]), "SUITE_ROOT": str(suite),
                                 "SUITE_MAVEN_REPO": str(suite / "m2"),
                                 "SUITE_MAVEN_USER_HOME": str(suite / "maven-wrapper"),
                                 "FLINK_ROOT": str(suite / "flink"), "CONTRACT_FILE": "contracts"},
                        )
                        self.assertEqual(status or probe_status, result.returncode, result.stderr)
                        invocations = [json.loads(row) for row in calls.read_text().splitlines()]
                        wrapper_home, working_directory, arguments = invocations[0]
                        self.assertEqual(str(suite / "maven-wrapper"), wrapper_home)
                        self.assertEqual(str(suite / "flink"), working_directory)
                        self.assertEqual(str(suite / "m2"), arguments[arguments.index("--repository") + 1])
                        self.assertEqual(str(suite / "flink/mvnw"), arguments[arguments.index("--maven") + 1])
                        self.assertNotIn("--profile", arguments)
                        self.assertNotIn("--projects", arguments)
                        self.assertNotIn("--pom", arguments)
                        artifacts = [arguments[i + 1] for i, value in enumerate(arguments) if value == "--artifact"]
                        expected = ["org.apache.maven.surefire:surefire-junit-platform:" +
                                    ("3.0.0-M5" if line == "1.18" else "3.2.2"),
                                    "org.junit.platform:junit-platform-launcher:" +
                                    ("1.9.1" if line == "1.18" else "1.11.4"),
                                    "org.apache.httpcomponents:httpclient:4.4.1",
                                    "org.apache.paimon:paimon-vfs-hadoop:2.0.0"]
                        if line == "2.2":
                            expected.extend(["org.apache.maven.surefire:surefire-junit-platform:3.2.5",
                                             "org.junit.platform:junit-platform-launcher:1.13.3"])
                        self.assertEqual(expected, artifacts)
                        self.assertEqual(1 if status or probe_status else 2, len(invocations))
                        self.assertEqual(not (status or probe_status), "build is ready" in result.stdout)
                        if status:
                            self.assertNotIn("offline-probe:", result.stdout)
                        else:
                            self.assertIn(" -o ", result.stdout)
                            self.assertIn("CalcITCase#testSimpleProject", result.stdout)
                            self.assertIn("-Dmaven.test.failure.ignore=false", result.stdout)


if __name__ == "__main__":
    unittest.main()
