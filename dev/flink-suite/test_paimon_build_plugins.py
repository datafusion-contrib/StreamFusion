"""Prepare inherited plugins even when their checks are skipped in the source suite."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

from test_test_runtime import FIXTURE, fixture_settings


ROOT = Path(__file__).resolve().parents[2]


class PaimonBuildPluginsTest(unittest.TestCase):
    def test_plugin_preparation_precedes_both_compile_models_and_failure_blocks_the_build(self):
        source = (ROOT / "dev/flink-suite/runner/prepare.sh").read_text()
        start = source.index('  if [[ "${SUITE_MODE}" == "paimon" ]]; then\n',
                             source.index('if [[ "${FLINK_SUITE_REUSE_BUILD:-false}" != "true" ]]; then\n'))
        end = source.index('\nfi\n\nprepare_delta_maven()', start)
        command = '''
set -uo pipefail
PAIMON_BUILD_ARGS=(-Pflink1 -Dtest.flink.main.version=1.18
  -Dpaimon-flink-common.flink.version=1.20.1 -Dtest.flink.version=1.18.1
  -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true)
record() { printf '%s\\n' "$@" '<END>' >> "$CALL_LOG"; }
python3() { record prepare "$@"; return "$RESOLVE_STATUS"; }
mvn() { record build "$@"; return "$BUILD_STATUS"; }
''' + source[start:end]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for line in ("1.18", "2.2"):
                for reuse in ("true", "false"):
                    for resolve, build in ((0, 0), (17, 0), (0, 23)):
                        with self.subTest(line=line, reuse=reuse, resolve=resolve, build=build):
                            log = root / "calls"
                            log.write_text("")
                            result = subprocess.run(
                                ["bash", "-c", command], capture_output=True, text=True,
                                env={**os.environ, "SUITE_MODE": "paimon", "FLINK_LINE": line,
                                     "FLINK_SUITE_REUSE_COMMON_BUILD": reuse, "CALL_LOG": str(log),
                                     "RESOLVE_STATUS": str(resolve), "BUILD_STATUS": str(build),
                                     "REPO_ROOT": str(ROOT), "MAVEN_SETTINGS": "settings with spaces.xml",
                                     "PAIMON_ENFORCER_ARTIFACT": "",
                                     "PAIMON_ROOT": "paimon release", "PAIMON_MODULE": "common",
                                     "PAIMON_LINE_MODULE": "flink-1.18", "PAIMON_FLINK_PROFILE": "flink1",
                                     "SUITE_MAVEN_REPO": "isolated m2"})
                            self.assertEqual(resolve or build, result.returncode, result.stderr)
                            calls = [call.splitlines() for call in log.read_text().split("<END>\n")[:-1]]
                            self.assertEqual("prepare", calls[0][0])
                            arguments = calls[0][2:]
                            self.assertIn("--plugin-help", arguments)
                            self.assertEqual("org.apache.maven.plugins:maven-checkstyle-plugin", arguments[arguments.index("--plugin-help") + 1])
                            self.assertIn("--offline-first", arguments)
                            self.assertEqual("paimon release/pom.xml", arguments[arguments.index("--pom") + 1])
                            self.assertEqual("common,flink-1.18" if line == "1.18" else "common",
                                             arguments[arguments.index("--projects") + 1])
                            self.assertIn("--also-make", arguments)
                            self.assertIn("checkstyle.skip=true", arguments)
                            self.assertIn("paimon-flink-common.flink.version=1.20.1", arguments)
                            self.assertEqual(1 if resolve else (2 if build or line == "2.2" else 3), len(calls))
                            for call in calls[1:]:
                                self.assertEqual("build", call[0])
                                self.assertIn("-DskipTests", call)

    def test_skipped_checkstyle_still_needs_its_inherited_plugin_and_custom_dependency(self):
        with tempfile.TemporaryDirectory(prefix="paimon plugin fixture ") as directory:
            root = Path(directory)
            settings = str(fixture_settings(directory))
            repository = root / "m2"
            pom = root / "pom.xml"
            namespace = "{http://maven.apache.org/POM/4.0.0}"
            ET.register_namespace("", namespace[1:-1])
            tree = ET.parse(FIXTURE / "checkstyle-pom.xml")
            ET.SubElement(ET.SubElement(tree.getroot(), namespace + "modules"),
                          namespace + "module").text = "common"
            tree.write(pom, encoding="utf-8", xml_declaration=True)
            module = root / "common"
            module.mkdir()
            (module / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><parent><groupId>test.paimon</groupId>
    <artifactId>parent</artifactId><version>1</version></parent><artifactId>common</artifactId>
  <dependencies><dependency><groupId>test.unbuilt</groupId><artifactId>reactor-output</artifactId>
    <version>1</version></dependency></dependencies></project>''')
            base = ["mvn", "-B", "-ntp", "-s", settings, "-gs", settings,
                    "-Dmaven.repo.local=" + str(repository), "-f", str(pom),
                    "-pl", ":common", "-am", "-Dcheckstyle.skip=true"]

            def run(command, succeeds=True):
                result = subprocess.run(command, capture_output=True, text=True, timeout=180)
                output = result.stdout + result.stderr
                self.assertEqual(0 if succeeds else 1, result.returncode, output)
                return output

            failure = run(base + ["-o", "validate"], succeeds=False)
            self.assertIn("maven-checkstyle-plugin", failure)
            run([sys.executable, str(ROOT / "bin/ci-maven-dependencies.py"),
                 "--plugin-help", "org.apache.maven.plugins:maven-checkstyle-plugin", "--offline-first", "--settings", settings,
                 "--pom", str(pom), "--projects", ":common", "--also-make",
                 "--repository", str(repository), "--define", "checkstyle.skip=true"])
            self.assertTrue((repository / "com/puppycrawl/tools/checkstyle/8.14/checkstyle-8.14.jar").is_file())
            self.assertFalse((repository / "test/unbuilt").exists())
            # Checkstyle's check goal requests project dependencies even when skipped;
            # its help goal above must leave the unbuilt dependency unresolved.
            parent = [argument for argument in base if argument not in ("-pl", ":common", "-am")]
            run(parent + ["-o", "-N", "validate"])


if __name__ == "__main__":
    unittest.main()
