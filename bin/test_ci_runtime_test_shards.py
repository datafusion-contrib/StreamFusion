"""Verify that the real Surefire executions retain every invocation across shards."""

from collections import Counter
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
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
    def test_line_costs_change_placement_while_retaining_the_complete_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ["LongTest", "MediumTest", "ShortTest", "NewTest"]
            for name in names:
                (root / (name + ".class")).touch()
            data = root / "durations.json"
            data.write_text(json.dumps({"seconds": {"LongTest": 90, "MediumTest": 60, "ShortTest": 30},
                                        "seconds_by_line": {"1.18": {"LongTest": 1}}}))
            with patch.object(shards, "DURATIONS", data):
                default = shards.partition(root, 2, "runtime")
                legacy = shards.partition(root, 2, "runtime", line="1.18")
                self.assertNotEqual(default, legacy)
                self.assertEqual(Counter(names), Counter(n for part in legacy for n in part))
                self.assertEqual(default, shards.partition(root, 2, "runtime", line="2.2"))
                output = root / "includes.txt"
                shards.write_selection(root, 2, 1, output, "runtime", "1.18")
                self.assertEqual("1.18", json.loads(output.with_suffix(".json").read_text())["flink_line"])
                data.write_text(json.dumps({"seconds": {}, "seconds_by_line": {"1.18": {"LongTest": -1}}}))
                with self.assertRaises(ValueError):
                    shards.partition(root, 2, "runtime", line="1.18")

    def test_canonical_jobs_and_runtime_are_a_complete_disjoint_bounded_partition(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = {f"CanonicalCase{index}JobTest" for index in range(118)}
            names.update(f"Case{index}Test" for index in range(20))
            names.add("CanonicalStateContractTest")
            for name in names | {"Helper", "CanonicalCase0JobTest$Nested"}:
                path = root / "tech/streamfusion" / (name + ".class")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            runtime = shards.partition(root, 3, "runtime")
            for count in (16, 32):
                with self.subTest(count=count):
                    canonical = shards.partition(root, count, "canonical")
                    self.assertEqual(Counter("tech/streamfusion/" + n for n in names),
                                     Counter(n for part in canonical + runtime for n in part))
                    self.assertLessEqual(max(map(len, canonical)), (118 + count - 1) // count)
                    self.assertEqual(118, sum(map(len, canonical)))
                    self.assertEqual(canonical, shards.partition(root, count, "canonical"))
                    weighted = shards.partition(root, count, "canonical", durations={
                        "tech/streamfusion/CanonicalCase0JobTest": 1000})
                    self.assertEqual(Counter(n for part in canonical for n in part),
                                     Counter(n for part in weighted for n in part))
            with self.assertRaises(ValueError):
                shards.partition(root, 3, "unknown")

    def test_measured_costs_balance_long_classes_without_dropping_new_classes(self):
        for suite in ("all", "runtime", "canonical"):
            with self.subTest(suite=suite), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                def class_name(name):
                    suffix = "Canonical" + name + "JobTest" if suite == "canonical" else name + "Test"
                    return "tech/streamfusion/" + suffix
                durations = {class_name(name): seconds for name, seconds in
                             {"Long": 90, "Medium": 60, "Short": 30}.items()}
                names = [*durations, class_name("New")]
                for name in names:
                    path = root / (name + ".class")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.touch()
                plan = shards.partition(root, 2, suite, durations=durations)
                self.assertEqual(Counter(names), Counter(name for part in plan for name in part))
                loads = [sum(durations.get(name, 1) for name in part) for part in plan]
                self.assertEqual([91, 90], loads)

    def test_invalid_duration_estimates_fail_before_selecting_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ExampleTest.class").touch()
            for value in (-1, float("nan"), float("inf"), "1", True):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    shards.partition(root, 1, durations={"ExampleTest": value})

    def test_default_names_are_complete_disjoint_stable_and_pinned_suites_stay_together(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = {"TestPrefix", "SuffixTest", "SuffixTests", "SuffixTestCase"}
            names.update(f"Case{index}Test" for index in range(20))
            names.update(name.rsplit("/", 1)[-1] for name in shards.FIRST_SHARD_CLASSES)
            for name in names | {"Helper", "Outer$NestedTest", "ExampleITCase"}:
                path = root / "tech/streamfusion" / (name + ".class")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            for count in (2, 3):
                with self.subTest(count=count):
                    plan = shards.partition(root, count)
                    self.assertEqual(plan, shards.partition(root, count))
                    self.assertEqual(Counter("tech/streamfusion/" + name for name in names),
                                     Counter(name for shard in plan for name in shard))
                    self.assertTrue(shards.FIRST_SHARD_CLASSES <= set(plan[0]))

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
            (source / "CanonicalParentJobTest.java").write_text('''package tech.streamfusion;
class CanonicalParentJobTest {
@org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(ints={1,2})
void inherited(int value) throws Exception { ''' + body('getClass().getSimpleName() + value') + ''' }
@org.junit.jupiter.api.Nested class Context {
@org.junit.jupiter.api.Test void nested() throws Exception { ''' + body('CanonicalParentJobTest.this.getClass().getSimpleName() + "-nested"') + ''' }
}
}''')
            (source / "CanonicalChildJobTest.java").write_text('''package tech.streamfusion;
class CanonicalChildJobTest extends CanonicalParentJobTest {
@org.junit.jupiter.api.Test void own() throws Exception { ''' + body('"child-only"') + ''' }
}''')

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
            for count in (2, 3):
                with self.subTest(count=count):
                    for path in selected.iterdir():
                        path.unlink()
                    actual = Counter()
                    for index in range(1, count + 1):
                        output = root / f"shard-{count}-{index}.txt"
                        shards.write_selection(root / "target/test-classes", count, index, output)
                        actual.update(run(f"-Dsf.runtime.testIncludesFile={output}"))
                    self.assertEqual(baseline, actual)
                    self.assertEqual(expected_markers, {path.name for path in selected.iterdir()})

            for path in selected.iterdir():
                path.unlink()
            actual = Counter()
            inventories = set()
            for suite in ("runtime", "canonical"):
                for index in (1, 2):
                    output = root / f"{suite}-{index}.txt"
                    shards.write_selection(root / "target/test-classes", 2, index, output, suite)
                    inventories.add(json.loads(output.with_suffix(".json").read_text())[
                        "complete_inventory_sha256"])
                    actual.update(run(f"-Dsf.runtime.testIncludesFile={output}"))
            self.assertEqual(1, len(inventories))
            self.assertEqual(baseline, actual)
            self.assertEqual(expected_markers, {path.name for path in selected.iterdir()})

            for path in selected.iterdir():
                path.unlink()
            (source / "FlinkPortableSqlAuditTest.java").write_text('''package tech.streamfusion;
class FlinkPortableSqlAuditTest {
@org.junit.jupiter.api.Test void selected() { throw new AssertionError("partition-sentinel"); }
}''')
            result = subprocess.run(["mvn", "-B", "-ntp", "test",
                                     f"-Dsf.runtime.testIncludesFile={root / 'shard-3-1.txt'}"],
                                    cwd=root, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
            report = root / "target/surefire-reports/TEST-tech.streamfusion.FlinkPortableSqlAuditTest.xml"
            failure = ET.parse(report).getroot().find("testcase/failure")
            self.assertIsNotNone(failure)
            self.assertIn("partition-sentinel", failure.attrib["message"])


if __name__ == "__main__":
    unittest.main()
