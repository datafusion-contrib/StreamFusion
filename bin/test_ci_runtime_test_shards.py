"""Verify that the real Surefire executions retain every invocation across shards."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid
import xml.etree.ElementTree as ET

import ci_runtime_test_shards as shards
import ci_canonical_jobs as canonical


ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def plain(element):
    element = copy.deepcopy(element)
    for node in element.iter():
        node.tag = node.tag.split("}")[-1]
    return ET.tostring(element, encoding="unicode")


class RuntimeTestShardsTest(unittest.TestCase):
    def test_worker_lanes_balance_without_pinning_and_ignore_inventory_order(self):
        pinned = sorted(shards.FIRST_SHARD_CLASSES)[:2]
        names = [*pinned, "MediumTest", "ShortTest", "NewTest", "ZeroTest"]
        durations = {pinned[0]: 90, pinned[1]: 80, "MediumTest": 60,
                     "ShortTest": 30, "ZeroTest": 0}
        plan = shards.partition_workers(names, 2, durations)
        self.assertEqual(plan, shards.partition_workers(reversed(names), 2, durations))
        self.assertEqual(Counter(names), Counter(name for lane in plan for name in lane))
        self.assertTrue(all(lane == sorted(lane) for lane in plan))
        self.assertFalse(any(set(pinned) <= set(lane) for lane in plan))
        self.assertEqual([121, 140], [sum(durations.get(name, 1) for name in lane) for lane in plan])

    def test_worker_lanes_never_emit_an_empty_includes_file(self):
        for count in (1, 2, 4):
            with self.subTest(count=count):
                plan = shards.partition_workers(["OnlyTest"], count, {})
                self.assertEqual([["OnlyTest"]], plan)
        self.assertEqual([["ATest"], ["BTest"]],
                         shards.partition_workers(["BTest", "ATest"], 4, {}))

    def test_worker_lanes_reject_invalid_counts_inventories_and_costs(self):
        for count in (0, -1, True, 1.5):
            with self.subTest(count=count), self.assertRaises(ValueError):
                shards.partition_workers(["ExampleTest"], count, {})
        for names in ([], ["ExampleTest", "ExampleTest"], [""], [None]):
            with self.subTest(names=names), self.assertRaises(ValueError):
                shards.partition_workers(names, 4, {})
        for value in (-1, float("nan"), float("inf"), "1", True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                shards.partition_workers(["ExampleTest"], 4, {"OtherTest": value})

    def test_provisional_costs_are_validated_floors_without_replacing_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "durations.json"
            recorded = {"seconds": {"GlobalTest": 100},
                        "seconds_by_line": {"2.2": {"LowTest": 80, "HighTest": 150}},
                        "provisional_seconds_by_line": {
                            "2.2": {"LowTest": 120, "HighTest": 120, "NewTest": 90}}}
            path.write_text(json.dumps(recorded))
            with patch.object(shards, "DURATIONS", path):
                self.assertEqual({"GlobalTest": 100, "LowTest": 120, "HighTest": 150, "NewTest": 90},
                                 shards.load_durations("2.2"))
                self.assertEqual({"GlobalTest": 100}, shards.load_durations("1.18"))
                self.assertEqual(80, json.loads(path.read_text())["seconds_by_line"]["2.2"]["LowTest"])
                for value in (-1, float("nan"), float("inf"), "1", True):
                    with self.subTest(value=value):
                        recorded["provisional_seconds_by_line"]["2.2"]["LowTest"] = value
                        path.write_text(json.dumps(recorded))
                        with self.assertRaises(ValueError):
                            shards.load_durations("2.2")

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
                self.assertEqual(90, shards.load_durations()["LongTest"])
                self.assertEqual(1, shards.load_durations("1.18")["LongTest"])
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

    def test_unmeasured_shape_estimates_balance_without_overriding_measured_costs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ["ShapeAJobTest", "ShapeBJobTest", "MeasuredTest", "NewTest"]
            for name in names:
                (root / (name + ".class")).touch()
            data = root / "durations.json"
            data.write_text(json.dumps({
                "seconds": {"MeasuredTest": 30},
                "estimated_seconds": {"ShapeAJobTest": 100, "ShapeBJobTest": 100,
                                      "MeasuredTest": 1000},
                "estimated_seconds_by_line": {"1.18": {"ShapeAJobTest": 10,
                                                      "ShapeBJobTest": 10}},
                "seconds_by_line": {"1.18": {"MeasuredTest": 5}}}))
            with patch.object(shards, "DURATIONS", data):
                current = shards.partition(root, 2)
                legacy = shards.partition(root, 2, line="1.18")
            self.assertEqual([["MeasuredTest", "ShapeAJobTest"],
                              ["NewTest", "ShapeBJobTest"]], current)
            self.assertEqual(current, legacy)
            self.assertEqual(Counter(names), Counter(x for part in current for x in part))

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
            path.relative_to(ROOT / "streamfusion-runtime/src/test/java").with_suffix("").as_posix()
            for path in (ROOT / "streamfusion-runtime/src/test/java").rglob("*.java")
            if '"sql-audit"' in path.read_text()
        }
        self.assertEqual(producers, shards.AUDIT_CLASSES)

    def test_real_maven_installed_owners_and_concurrent_lanes_retain_every_invocation(self):
        parent = ET.parse(ROOT / "pom.xml")
        runtime = ET.parse(ROOT / "streamfusion-runtime/pom.xml")
        plugin = copy.deepcopy(next(p for p in runtime.findall("m:build/m:plugins/m:plugin", NS)
                                    if p.findtext("m:artifactId", namespaces=NS) == "maven-surefire-plugin"))
        ET.SubElement(plugin, "version").text = "3.2.5"
        config = plugin.find("m:configuration", NS)
        ET.SubElement(config, "forkCount").text = "${sf.testForks}"
        ET.SubElement(config, "reuseForks").text = "${sf.testReuseForks}"
        precompiled = next(p for p in parent.findall("m:profiles/m:profile", NS)
                           if p.findtext("m:id", namespaces=NS) == "ci-precompiled-java")
        group = "test.streamfusion.lanes." + uuid.uuid4().hex
        installed = Path.home() / ".m2/repository" / group.replace(".", "/")
        self.addCleanup(shutil.rmtree, installed, ignore_errors=True)
        with tempfile.TemporaryDirectory(prefix="canonical-lanes-") as directory:
            reactor = Path(directory)
            owner, consumer = reactor / "owner", reactor / "streamfusion-runtime"
            owner.mkdir()
            consumer.mkdir()
            header = '<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
            (reactor / "pom.xml").write_text(header + f'''
<groupId>{group}</groupId><artifactId>reactor</artifactId><version>1</version><packaging>pom</packaging>
<modules><module>owner</module><module>streamfusion-runtime</module></modules>
<properties><maven.compiler.release>11</maven.compiler.release><sf.testForks>4</sf.testForks>
<sf.testReuseForks>false</sf.testReuseForks><sf.runtime.tests.skip>false</sf.runtime.tests.skip>
<sf.orc.test.groups>streamfusion-orc</sf.orc.test.groups></properties>
<dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId>
<version>5.10.2</version><scope>test</scope></dependency></dependencies>
<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId>
<version>3.13.0</version></plugin><plugin><groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-surefire-plugin</artifactId><version>3.2.5</version></plugin></plugins></build>
<profiles>''' + plain(precompiled) + "</profiles></project>")
            parent_ref = f"<parent><groupId>{group}</groupId><artifactId>reactor</artifactId><version>1</version></parent>"
            (owner / "pom.xml").write_text(header + parent_ref + '''<artifactId>owner</artifactId>
<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-jar-plugin</artifactId>
<version>3.4.1</version><executions><execution><id>default-jar</id><phase>process-classes</phase>
<goals><goal>jar</goal></goals></execution></executions></plugin></plugins></build></project>''')
            (consumer / "pom.xml").write_text(header + parent_ref + f'''<artifactId>streamfusion-runtime</artifactId>
<dependencies><dependency><groupId>{group}</groupId><artifactId>owner</artifactId><version>1</version>
</dependency></dependencies><build><plugins>''' + plain(plugin) + "</plugins></build>"
                + plain(runtime.find("m:profiles", NS)) + "</project>")
            owner_source = owner / "src/main/java/example/Owner.java"
            owner_source.parent.mkdir(parents=True)
            owner_source.write_text('''package example;
public class Owner { public static String value() { return "current-owner"; } }''')
            resources = owner / "src/main/resources"
            resources.mkdir(parents=True)
            (resources / "owner.txt").write_text("current-resource")
            owner_tests = owner / "src/test/java/example/OwnerTest.java"
            owner_tests.parent.mkdir(parents=True)
            owner_tests.write_text('''package example; class OwnerTest {
@org.junit.jupiter.api.Test void preparedOnce() throws Exception {
java.nio.file.Files.writeString(java.nio.file.Path.of("target", "owner-ran"), "once",
java.nio.file.StandardOpenOption.CREATE_NEW); } }''')
            source = consumer / "src/test/java/tech/streamfusion"
            source.mkdir(parents=True)
            (source / "Probe.java").write_text('''package tech.streamfusion; class Probe {
static void record(String identity) throws Exception {
org.junit.jupiter.api.Assertions.assertEquals("current-owner", example.Owner.value());
try (var resource = example.Owner.class.getResourceAsStream("/owner.txt")) {
org.junit.jupiter.api.Assertions.assertEquals("current-resource", new String(resource.readAllBytes())); }
var location = java.nio.file.Path.of(example.Owner.class.getProtectionDomain().getCodeSource().getLocation().toURI());
org.junit.jupiter.api.Assertions.assertTrue(location.toString().endsWith(".jar"));
if (System.getProperty("fixture.ownerJar") != null)
org.junit.jupiter.api.Assertions.assertEquals(java.nio.file.Path.of(System.getProperty("fixture.ownerJar")), location);
java.nio.file.Files.createDirectories(java.nio.file.Path.of("target", "selected"));
java.nio.file.Files.writeString(java.nio.file.Path.of("target", "selected", identity), "once",
java.nio.file.StandardOpenOption.CREATE_NEW); } }''')
            fixtures = {
                "LaneParameterizedTest": '''@org.junit.jupiter.params.ParameterizedTest
@org.junit.jupiter.params.provider.ValueSource(ints={1,2})
void repeated(int value) throws Exception { Probe.record("parameter-" + value); }''',
                "LaneParentTest": '''@org.junit.jupiter.api.Test
void inherited() throws Exception { Probe.record(getClass().getSimpleName() + "-inherited"); }''',
                "LaneChildTest extends LaneParentTest": '''@org.junit.jupiter.api.Test
void own() throws Exception { Probe.record("child-only"); }''',
                "LaneNestedTest": '''@org.junit.jupiter.api.Test
void outside() throws Exception { Probe.record("outer"); }
@org.junit.jupiter.api.Nested class Context {
@org.junit.jupiter.api.Test void inside() throws Exception { Probe.record("nested"); } }''',
                "LaneOrcTest": '''@org.junit.jupiter.api.Tag("streamfusion-orc")
@org.junit.jupiter.api.Test void orc() throws Exception { Probe.record("orc"); }''',
                "LaneDisabledTest": '''@org.junit.jupiter.api.Disabled
@org.junit.jupiter.api.Test void disabled() { throw new AssertionError("disabled invocation ran"); }''',
            }
            for declaration, body in fixtures.items():
                name = declaration.split()[0]
                (source / (name + ".java")).write_text("package tech.streamfusion; class "
                                                      + declaration + " { " + body + " }")

            def run(*options):
                result = subprocess.run(["mvn", "-B", "-ntp", *options], cwd=reactor,
                                        capture_output=True, text=True, timeout=120)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)

            def reports():
                cases = Counter()
                for report in (consumer / "target/surefire-reports").glob("TEST-*.xml"):
                    for case in ET.parse(report).getroot().findall("testcase"):
                        cases[(case.attrib["classname"], case.attrib["name"],
                               case.find("skipped") is not None)] += 1
                    report.unlink()
                return cases

            run("-pl", "streamfusion-runtime", "-am", "test")
            baseline = reports()
            self.assertEqual(9, sum(baseline.values()))
            self.assertEqual(1, sum(count for (_, _, skipped), count in baseline.items() if skipped))
            selected = consumer / "target/selected"
            expected_markers = {path.name for path in selected.iterdir()}
            for path in selected.iterdir():
                path.unlink()
            (owner / "target/owner-ran").unlink()
            owner_source.write_text("invalid source: the installed owner must use precompiled classes")
            (source / "Probe.java").write_text("invalid source: lane execution must not compile")
            run("-pl", "streamfusion-runtime", "-am", "install", "-Dsf.ci.precompiledJava=true",
                "-Dsf.runtime.tests.skip=true", "-DskipTests=false")
            owner_jar = installed / "owner/1/owner-1.jar"
            self.assertTrue(owner_jar.is_file())

            def artifact_state():
                paths = [*reactor.rglob("*.jar"), *reactor.rglob("*.class"),
                         *installed.rglob("*.jar"), owner / "target/owner-ran"]
                return {path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                        for path in paths}

            frozen = artifact_state()
            candidates = shards.candidates_for_suite(consumer / "target/test-classes", "all")
            lanes = shards.partition_workers(candidates, 4, {})
            self.assertEqual(4, len(lanes))
            inputs = []
            for index, lane in enumerate(lanes):
                output = reactor / f"lane-{index}.txt"
                output.write_text("".join(name + ".java\n" for name in lane))
                inputs.append(output)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(run, "-pl", "streamfusion-runtime", "surefire:test@default-test",
                                           "surefire:test@orc-tests", "-Dsf.testForks=1",
                                           "-Dsf.testReuseForks=false", "-Dsf.runtime.tests.skip=false",
                                           f"-DtempDir=surefire-lane-{index}",
                                           f"-Dsf.runtime.testIncludesFile={output}",
                                           f"-Dfixture.ownerJar={owner_jar}")
                           for index, output in enumerate(inputs)]
                for future in futures:
                    future.result()
            self.assertEqual(baseline, reports())
            self.assertEqual(expected_markers, {path.name for path in selected.iterdir()})
            self.assertEqual(frozen, artifact_state())

            # Module units already ran in the ordinary cohort. Repeating them would
            # fail CREATE_NEW above; canonical workers must still execute every case.
            for shard in (1, 2):
                with self.subTest(canonical_shard=shard):
                    for path in selected.iterdir():
                        path.unlink()
                    includes = reactor / f"canonical-{shard}.txt"
                    includes.write_text("".join(name + ".java\n" for name in candidates))
                    includes.with_suffix(".json").write_text(json.dumps({
                        "suite": "canonical", "flink_line": "2.2", "shard": shard,
                        "inventory_sha256": "fixture", "classes": candidates,
                    }))
                    self.assertEqual(0, canonical.run(reactor, includes,
                        "ci-precompiled-java", "2.2", 4, "-Xmx128m"))
                    self.assertEqual(baseline, reports())
                    self.assertEqual(expected_markers, {path.name for path in selected.iterdir()})

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

            (source / "CanonicalMatrixBaseJobTest.java").write_text('''package tech.streamfusion;
abstract class CanonicalMatrixBaseJobTest {
@org.junit.jupiter.params.ParameterizedTest
@org.junit.jupiter.params.provider.MethodSource("cases")
void inherited(int value) throws Exception { ''' + body('getClass().getSimpleName() + value') + ''' }
}''')
            for label, values in (("A", "1,2"), ("B", "3,4")):
                name = "CanonicalMatrix" + label + "JobTest"
                (source / (name + ".java")).write_text(f'''package tech.streamfusion;
class {name} extends CanonicalMatrixBaseJobTest {{
static java.util.stream.Stream<Integer> cases() {{ return java.util.stream.Stream.of({values}); }}
}}''')

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
