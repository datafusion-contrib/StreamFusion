"""Verify that the real Surefire executions retain every invocation across shards."""

from collections import Counter
import copy
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

import ci_runtime_test_shards as shards


ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def plain(element):
    element = copy.deepcopy(element)
    for node in element.iter():
        node.tag = node.tag.split("}")[-1]
    return ET.tostring(element, encoding="unicode")


class RuntimeTestShardsTest(unittest.TestCase):
    def test_default_names_are_complete_disjoint_stable_and_audits_stay_together(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = {"TestPrefix", "SuffixTest", "SuffixTests", "SuffixTestCase"}
            names.update(f"Case{index}Test" for index in range(20))
            names.update(name.rsplit("/", 1)[-1] for name in shards.AUDIT_CLASSES)
            for name in names | {"Helper", "Outer$NestedTest", "ExampleITCase"}:
                path = root / "tech/streamfusion" / (name + ".class")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            plan = shards.partition(root, 2)
            self.assertEqual(plan, shards.partition(root, 2))
            self.assertEqual({"tech/streamfusion/" + name for name in names},
                             set(plan[0]) | set(plan[1]))
            self.assertFalse(set(plan[0]) & set(plan[1]))
            self.assertTrue(shards.AUDIT_CLASSES <= set(plan[0]))

    def test_invalid_or_empty_selection_cannot_restore_default_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                shards.partition(root, 2)
            with self.assertRaises(ValueError):
                shards.partition(root, 0)
            (root / "OnlyTest.class").touch()
            with self.assertRaises(ValueError):
                shards.partition(root, 2)
            output = root / "includes.txt"
            with self.assertRaises(ValueError):
                shards.write_selection(root, 2, 0, output)
            with self.assertRaises(ValueError):
                shards.write_selection(root, 2, 3, output)
            self.assertFalse(output.exists())

    def test_audit_artifact_producers_are_pinned(self):
        producers = {
            path.relative_to(ROOT / "src/test/java").with_suffix("").as_posix()
            for path in (ROOT / "src/test/java").rglob("*.java")
            if '"sql-audit"' in path.read_text()
        }
        self.assertEqual(producers, shards.AUDIT_CLASSES)

    def test_real_maven_partition_matches_unpartitioned_tags_nested_and_parameterized_tests(self):
        parent = ET.parse(ROOT / "pom.xml")
        runtime = ET.parse(ROOT / "streamfusion-runtime/pom.xml")
        parent_plugin = next(p for p in parent.findall(".//m:plugin", NS)
                             if p.findtext("m:artifactId", namespaces=NS) == "maven-surefire-plugin")
        plugin = copy.deepcopy(next(p for p in runtime.findall("m:build/m:plugins/m:plugin", NS)
                                    if p.findtext("m:artifactId", namespaces=NS) == "maven-surefire-plugin"))
        ET.SubElement(plugin, "version").text = parent_plugin.findtext("m:version", namespaces=NS)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>runtime-partition</artifactId><version>1</version>
<properties><maven.compiler.release>11</maven.compiler.release><sf.runtime.tests.skip>false</sf.runtime.tests.skip>
<sf.orc.test.groups>streamfusion-orc</sf.orc.test.groups></properties>
<dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.2</version><scope>test</scope></dependency></dependencies>
<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
''' + plain(plugin) + "</plugins></build>" + plain(runtime.find("m:profiles", NS)) + "</project>")
            source = root / "src/test/java/tech/streamfusion"
            source.mkdir(parents=True)

            def body(label):
                return ('java.nio.file.Files.createDirectories(java.nio.file.Path.of("target", "selected"));'
                        'java.nio.file.Files.writeString(java.nio.file.Path.of("target", "selected", '
                        + label + '), "selected", java.nio.file.StandardOpenOption.CREATE_NEW);')

            for name in ("TestPrefix", "SuffixTestCase", "FlinkPortableSqlAuditTest", "OrcTest",
                         "DisabledTest", "NestedTest", "Helper"):
                annotation = ('@org.junit.jupiter.api.Tag("streamfusion-orc")' if name == "OrcTest"
                              else "@org.junit.jupiter.api.Disabled" if name == "DisabledTest" else "")
                nested = (f'''@org.junit.jupiter.api.Nested class Context {{
@org.junit.jupiter.api.Test void inside() throws Exception {{ {body('"nested"')} }} }}'''
                          if name == "NestedTest" else "")
                (source / (name + ".java")).write_text(f'''package tech.streamfusion;
{annotation} class {name} {{
@org.junit.jupiter.api.Test void selected() throws Exception {{ {body('"' + name + '"')} }}
{nested}
}}''')
            (source / "SuffixTests.java").write_text('''package tech.streamfusion;
class SuffixTests {
@org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(ints={1,2})
void repeated(int value) throws Exception { ''' + body('"parameter-" + value') + " } }")

            def run(*options):
                result = subprocess.run(["mvn", "-B", "-ntp", "test", *options], cwd=root,
                                        capture_output=True, text=True, timeout=120)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                cases = Counter()
                for report in (root / "target/surefire-reports").glob("TEST-*.xml"):
                    for case in ET.parse(report).getroot().findall("testcase"):
                        cases[(case.attrib["classname"], case.attrib["name"],
                               case.find("skipped") is not None)] += 1
                    report.unlink()
                return cases

            baseline = run()
            self.assertTrue(baseline)
            self.assertFalse(any("Helper" in name for name, _, _ in baseline))
            self.assertEqual(1, sum(count for (_, _, skipped), count in baseline.items() if skipped))
            selected = root / "target/selected"
            expected_markers = {path.name for path in selected.iterdir()}
            for path in selected.iterdir():
                path.unlink()
            actual = Counter()
            for index in (1, 2):
                output = root / f"shard-{index}.txt"
                shards.write_selection(root / "target/test-classes", 2, index, output)
                actual.update(run(f"-Dsf.runtime.testIncludesFile={output}"))
            self.assertEqual(baseline, actual)
            self.assertEqual(expected_markers, {path.name for path in selected.iterdir()})

            for path in selected.iterdir():
                path.unlink()
            (source / "FlinkPortableSqlAuditTest.java").write_text('''package tech.streamfusion;
class FlinkPortableSqlAuditTest {
@org.junit.jupiter.api.Test void selected() { throw new AssertionError("partition-sentinel"); }
}''')
            result = subprocess.run(["mvn", "-B", "-ntp", "test",
                                     f"-Dsf.runtime.testIncludesFile={root / 'shard-1.txt'}"],
                                    cwd=root, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
            report = root / "target/surefire-reports/TEST-tech.streamfusion.FlinkPortableSqlAuditTest.xml"
            failure = ET.parse(report).getroot().find("testcase/failure")
            self.assertIsNotNone(failure)
            self.assertIn("partition-sentinel", failure.attrib["message"])


if __name__ == "__main__":
    unittest.main()
