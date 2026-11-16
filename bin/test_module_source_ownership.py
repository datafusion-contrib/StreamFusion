#!/usr/bin/env python3
"""Require physical module source ownership and prevent implicit cross-module class emission."""
import os
import pathlib
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


class ModuleSourceOwnershipTest(unittest.TestCase):
    def test_new_module_inherits_no_foreign_source_roots(self):
        pom = ET.parse(ROOT / "pom.xml").getroot()
        revision = pom.findtext("m:properties/m:revision", namespaces=NS)
        for line in ("2.2", "1.18"):
            with self.subTest(line=line), tempfile.TemporaryDirectory() as temp:
                module = pathlib.Path(temp).resolve() / "connector"
                module.mkdir()
                project = ET.Element("project", xmlns=NS["m"])
                ET.SubElement(project, "modelVersion").text = "4.0.0"
                parent = ET.SubElement(project, "parent")
                for name, value in [("groupId", "tech.streamfusion"),
                                    ("artifactId", "StreamFusion"), ("version", "${revision}"),
                                    ("relativePath", os.path.relpath(ROOT / "pom.xml", module))]:
                    ET.SubElement(parent, name).text = value
                ET.SubElement(project, "artifactId").text = "source-boundary-probe"
                plugin = ET.SubElement(ET.SubElement(ET.SubElement(project, "build"), "plugins"),
                                       "plugin")
                for name, value in [("groupId", "org.apache.maven.plugins"),
                                    ("artifactId", "maven-help-plugin"), ("version", "3.5.2")]:
                    ET.SubElement(plugin, name).text = value
                executions = ET.SubElement(plugin, "executions")
                outputs = [module / f"root-{index}.txt" for index in range(3)]
                # Read scalar entries: Maven 3.10's LoggingList cannot be serialized by XStream.
                for index, output in enumerate(outputs):
                    execution = ET.SubElement(executions, "execution")
                    ET.SubElement(execution, "id").text = f"root-{index}"
                    ET.SubElement(execution, "phase").text = "process-sources"
                    ET.SubElement(ET.SubElement(execution, "goals"), "goal").text = "evaluate"
                    configuration = ET.SubElement(execution, "configuration")
                    ET.SubElement(configuration, "expression").text = f"project.compileSourceRoots[{index}]"
                    ET.SubElement(configuration, "output").text = str(output)
                ET.ElementTree(project).write(module / "pom.xml", encoding="utf-8")
                profiles = ["-Pflink-1.18"] if line == "1.18" else []
                result = subprocess.run(
                    ["mvn", "-B", "-ntp", "-N", "-f", str(module / "pom.xml"),
                     f"-Drevision={revision}", "-Denforcer.skip=true", *profiles,
                     "process-sources"],
                    cwd=ROOT, capture_output=True, text=True, timeout=120,
                )
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                values = [output.read_text().strip() for output in outputs]
                absent = "null object or invalid expression"
                self.assertEqual(absent, values[2], "A child module must not inherit a third source root")
                roots = {pathlib.Path(value).resolve() for value in values[:2] if value != absent}
                owned = {(module / "src/main/java").resolve(),
                         (module / f"src/main/java-flink{line}").resolve()}
                self.assertIn((module / "src/main/java").resolve(), roots)
                self.assertLessEqual(roots, owned)

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
