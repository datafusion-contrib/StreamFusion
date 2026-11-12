"""Execute both format providers and retain a failed format without suppressing the other."""

import copy
from contextlib import redirect_stderr
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import ci_lake_tests as lake


class LakeTestsTest(unittest.TestCase):
    def fixture(self, root):
        pom = '''<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>format-fixture</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>
<properties><maven.compiler.release>11</maven.compiler.release></properties>
<modules><module>streamfusion-paimon</module></modules><dependencies><dependency>
<groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.2</version><scope>test</scope>
</dependency></dependencies><build><plugins><plugin><groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin><plugin>
<groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.2.5</version>
</plugin></plugins></build></project>'''
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        parent = ET.parse(Path(__file__).resolve().parents[1] / "pom.xml")
        profile = copy.deepcopy(next(p for p in parent.findall("m:profiles/m:profile", ns)
                                     if p.findtext("m:id", namespaces=ns) == "ci-precompiled-java"))
        for item in profile.iter():
            item.tag = item.tag.split("}")[-1]
        (root / "pom.xml").write_text(pom.replace("</project>", "<profiles>" + ET.tostring(profile, encoding="unicode") + "</profiles></project>"))
        module = root / "streamfusion-paimon"
        source = module / "src/test/java"
        source.mkdir(parents=True)
        (module / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><parent><groupId>format-fixture</groupId><artifactId>parent</artifactId>
<version>1</version></parent><artifactId>streamfusion-paimon</artifactId></project>''')
        (source / "FormatTest.java").write_text('''import java.nio.file.*; class FormatTest {
@org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(ints={1,2})
void allCases(int value) throws Exception { String format = System.getenv("SF_PAIMON_FILE_FORMAT");
org.junit.jupiter.api.Assertions.assertTrue(java.util.Set.of("parquet", "orc").contains(format));
Files.writeString(Path.of("target/ran-" + format + "-" + value), "ran");
org.junit.jupiter.api.Assertions.assertNotEquals(System.getenv("FAIL_FORMAT"), format); }
}''')
        return module

    def test_real_maven_retains_every_format_invocation_and_failure(self):
        for failing in ("", "parquet"):
            with self.subTest(failing=failing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                module = self.fixture(root)
                real_run = subprocess.run
                logs = []

                def maven(command, **kwargs):
                    result = real_run(command, capture_output=True, text=True, **kwargs)
                    logs.append(result.stdout + result.stderr)
                    return result

                with patch.object(lake, "ROOT", root), patch.dict(os.environ, {"FAIL_FORMAT": failing}), \
                        patch.object(lake.subprocess, "run", side_effect=maven):
                    self.assertEqual(int(bool(failing)), lake.run("streamfusion-paimon", "", root / "summary.json"),
                                     "\n".join(logs)[-5000:])
                    self.assertEqual(2, lake.outcomes("streamfusion-paimon", "paimon-orc")["tests"])
                self.assertEqual({f"ran-{format}-{value}" for format in ("parquet", "orc") for value in (1, 2)},
                                 {p.name for p in (module / "target").glob("ran-*")})

    def test_preparation_failure_stops_before_format_work(self):
        with patch.object(lake.subprocess, "run", return_value=subprocess.CompletedProcess([], 2)) as run:
            self.assertEqual(2, lake.run("streamfusion-paimon", "paimon"))
            self.assertEqual(1, run.call_count)

    def test_success_without_actual_tests_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(lake, "ROOT", Path(directory)), \
                patch.object(lake.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
            reports = Path(directory) / "streamfusion-paimon/target/surefire-reports"
            reports.mkdir(parents=True)
            for format in ("parquet", "orc"):
                (reports / f"TEST-stale-paimon-{format}.xml").write_text(
                    '<testsuite tests="1"><testcase classname="stale" name="old"/></testsuite>')
            with redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(1, lake.run("streamfusion-paimon", "paimon"))
            self.assertIn("Incomplete paimon-orc", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
