#!/usr/bin/env python3
"""Require physical module source ownership and prevent implicit cross-module class emission."""
import importlib.util
import os
import pathlib
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


class ModuleSourceOwnershipTest(unittest.TestCase):
    def test_runtime_responsibilities_and_relocated_tests_have_explicit_owners(self):
        spec = importlib.util.spec_from_file_location("runtime_layout", ROOT / "dev/runtime_layout.py")
        layout = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(layout)
        operators, tests = layout.validate()
        self.assertGreater(operators, 0)
        self.assertGreater(tests, 0)

    def test_runtime_layout_rejects_omissions_duplicates_and_misplaced_tests(self):
        spec = importlib.util.spec_from_file_location("runtime_layout", ROOT / "dev/runtime_layout.py")
        layout = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(layout)
        for problem in ("missing_operator", "duplicate_test", "old_source", "package", "mode"):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                inventory = root / "dev/runtime-layout"
                inventory.mkdir(parents=True)
                operator = root / "streamfusion-core/src/main/java/tech/streamfusion/operator/Example.java"
                operator.parent.mkdir(parents=True)
                operator.write_text("package tech.streamfusion.operator; class Example {}")
                owners = inventory / "operators.tsv"
                owners.write_text("class\tfamily\tmode\tidentity\n"
                                  "tech.streamfusion.operator.Example\tcalc\tshared\timplementation\n")
                source = root / "streamfusion-runtime/src/test/java"
                test = source / "tech/streamfusion/runtime/batch/sql/ExampleTest.java"
                test.parent.mkdir(parents=True)
                test.write_text("package tech.streamfusion.runtime.batch.sql; class ExampleTest {}")
                relocations = inventory / "test-relocations.tsv"
                row = ("streamfusion-runtime\ttech.streamfusion.ExampleTest\t"
                       "tech.streamfusion.runtime.batch.sql.ExampleTest\tbatch\n")
                relocations.write_text("owner\tprevious_class\tcurrent_class\tmode\n" + row)
                self.assertEqual((1, 1), layout.validate(root))
                if problem == "missing_operator":
                    owners.write_text("class\tfamily\tmode\tidentity\n")
                elif problem == "duplicate_test":
                    with relocations.open("a") as stream:
                        stream.write(row)
                elif problem == "old_source":
                    (source / "tech/streamfusion/ExampleTest.java").write_text("class ExampleTest {}")
                elif problem == "package":
                    test.write_text("package tech.streamfusion; class ExampleTest {}")
                else:
                    relocations.write_text(relocations.read_text().replace("\tbatch\n", "\tstream\n"))
                with self.assertRaises(ValueError):
                    layout.validate(root)

    def test_runtime_owns_cross_module_tests_and_uses_its_default_java_root(self):
        self.assertFalse((ROOT / "src/test").exists())
        runtime = ET.parse(ROOT / "streamfusion-runtime/pom.xml").getroot()
        self.assertIsNone(runtime.find("m:build/m:testSourceDirectory", NS))
        for source in ("java", "java-flink1.18", "java-flink2.2", "resources", "proto"):
            self.assertTrue((ROOT / "streamfusion-runtime/src/test" / source).is_dir(), source)
        for module in ("streamfusion-core", "streamfusion-runtime"):
            pom = ET.parse(ROOT / module / "pom.xml").getroot()
            for element in pom.iter():
                self.assertNotIn("${project.basedir}/../src/test", element.text or "", module)

    def test_test_utilities_do_not_depend_on_a_deployable_module(self):
        utility = ET.parse(ROOT / "streamfusion-test-utils/pom.xml").getroot()
        for dependency in utility.findall("m:dependencies/m:dependency", NS):
            self.assertNotEqual("tech.streamfusion", dependency.findtext("m:groupId", namespaces=NS))
        for module in ("streamfusion-core", "streamfusion-runtime", "streamfusion-kafka",
                       "streamfusion-parquet", "streamfusion-orc", "streamfusion-paimon",
                       "streamfusion-paimon1", "streamfusion-delta", "streamfusion-fluss"):
            pom = ET.parse(ROOT / module / "pom.xml").getroot()
            utility_dependencies = [d for d in pom.findall("m:dependencies/m:dependency", NS)
                                    if d.findtext("m:artifactId", namespaces=NS)
                                    == "streamfusion-test-utils${streamfusion.artifact.suffix}"]
            self.assertEqual(1, len(utility_dependencies), module)
            self.assertEqual("test", utility_dependencies[0].findtext("m:scope", namespaces=NS))

    def test_runtime_assembles_artifacts_without_compiling_shared_production_sources(self):
        self.assertFalse(any((ROOT / "src/main").glob("java*")))
        runtime = ET.parse(ROOT / "streamfusion-runtime/pom.xml").getroot()
        self.assertIsNone(runtime.find("m:build/m:sourceDirectory", NS))
        self.assertFalse(list((ROOT / "streamfusion-runtime/src/main").rglob("*.java")))
        dependencies = {d.findtext("m:artifactId", namespaces=NS)
                        for d in runtime.findall("m:dependencies/m:dependency", NS)}
        for module in ["core", "kafka", "json", "csv", "raw", "avro", "avro-confluent-registry", "protobuf", "parquet", "orc"]:
            self.assertIn("streamfusion-" + module + "${streamfusion.artifact.suffix}", dependencies)

    def test_default_deployable_modules_have_one_owner_per_binary_name(self):
        parent = ET.parse(ROOT / "pom.xml").getroot()
        modules = [m.text for m in parent.findall("m:modules/m:module", NS)
                   if m.text != "streamfusion-runtime"]
        for line in ["1.18", "2.2"]:
            owners = {}
            for module in modules:
                pom = ET.parse(ROOT / module / "pom.xml").getroot()
                self.assertIsNone(pom.find("m:build/m:sourceDirectory", NS), module)
                for directory in ["src/main/java", "src/main/java-flink" + line]:
                    source = ROOT / module / directory
                    for file in source.rglob("*.java"):
                        binary = file.relative_to(source).as_posix()
                        self.assertNotIn(binary, owners, (line, binary, module, owners.get(binary)))
                        owners[binary] = module
            self.assertEqual("streamfusion-core", owners["tech/streamfusion/Native.java"])
            self.assertEqual("streamfusion-kafka", owners["tech/streamfusion/kafka/NativeKafka.java"])
            self.assertEqual("streamfusion-json", owners["tech/streamfusion/format/json/NativeJsonFormat.java"])

    def test_optional_runtime_dependencies_are_not_inherited_by_core(self):
        parent = ET.parse(ROOT / "pom.xml").getroot()
        optional = {"flink-connector-kafka", "kafka-clients", "flink-json", "flink-csv", "flink-avro", "flink-orc", "hadoop-common"}
        for dep in parent.findall("m:dependencies/m:dependency", NS):
            if dep.findtext("m:artifactId", namespaces=NS) in optional:
                self.assertEqual("test", dep.findtext("m:scope", namespaces=NS))

    def test_newer_shared_source_does_not_duplicate_old_jar_classes(self):
        pom = ET.parse(ROOT / "pom.xml").getroot()
        plugins = pom.findall("m:build/m:pluginManagement/m:plugins/m:plugin", NS)
        compiler = next(p for p in plugins
                        if p.findtext("m:artifactId", namespaces=NS) == "maven-compiler-plugin")
        arguments = [arg.text for arg in compiler.findall("m:configuration/m:compilerArgs/m:arg", NS)]
        self.assertIn("-implicit:none", arguments)
        with tempfile.TemporaryDirectory() as temp:
            directory = pathlib.Path(temp)
            source = directory / "shared/tech/streamfusion/probe"
            source.mkdir(parents=True)
            dependency = source / "CoreOwned.java"
            dependency.write_text("package tech.streamfusion.probe; public class CoreOwned { public static int value() { return 1; } }")
            core = directory / "core"
            core.mkdir()
            subprocess.run(["javac", "-d", str(core), str(dependency)], check=True)
            jar = directory / "core.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                # Reproducible artifact timestamps precede newly checked-out shared source.
                archive.writestr(zipfile.ZipInfo("tech/streamfusion/probe/CoreOwned.class", (1980, 1, 1, 0, 0, 0)),
                                 (core / "tech/streamfusion/probe/CoreOwned.class").read_bytes())
            connector = source / "ConnectorOwned.java"
            connector.write_text("package tech.streamfusion.probe; public class ConnectorOwned { public int read() { return CoreOwned.value(); } }")
            for mode, flags in [("before", []), ("after", arguments)]:
                output = directory / mode
                output.mkdir()
                subprocess.run(["javac", "-cp", str(jar), "-sourcepath", str(directory / "shared"),
                                "-d", str(output), *flags, str(connector)], check=True)
                classes = {str(path.relative_to(output)) for path in output.rglob("*.class")}
                expected = {"tech/streamfusion/probe/ConnectorOwned.class"}
                if mode == "before":
                    expected.add("tech/streamfusion/probe/CoreOwned.class")
                self.assertEqual(expected, classes)
                if mode == "after":
                    main = directory / "ModuleProbe.java"
                    main.write_text("""
public class ModuleProbe {
  public static void main(String[] args) throws Exception {
    if (new tech.streamfusion.probe.ConnectorOwned().read() != 1) throw new AssertionError();
    String origin = tech.streamfusion.probe.CoreOwned.class.getProtectionDomain().getCodeSource().getLocation().toString();
    if (!origin.endsWith("core.jar")) throw new AssertionError(origin);
  }
}
""")
                    cp = str(output) + os.pathsep + str(jar)
                    subprocess.run(["javac", "-cp", cp, "-d", str(output), str(main)], check=True)
                    subprocess.run(["java", "-Xverify:all", "-cp", cp, "ModuleProbe"], check=True)


if __name__ == "__main__":
    unittest.main()
