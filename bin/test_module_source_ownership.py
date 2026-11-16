#!/usr/bin/env python3
"""Prevent javac from emitting another module's newer shared sources into a connector JAR."""
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
                ET.ElementTree(project).write(module / "pom.xml", encoding="utf-8")
                output = module / "roots.xml"
                profiles = ["-Pflink-1.18"] if line == "1.18" else []
                result = subprocess.run(
                    ["mvn", "-B", "-ntp", "-N", "-f", str(module / "pom.xml"),
                     f"-Drevision={revision}", "-Denforcer.skip=true", *profiles,
                     "generate-sources", "help:evaluate", "-Dexpression=project.compileSourceRoots",
                     f"-Doutput={output}"],
                    cwd=ROOT, capture_output=True, text=True, timeout=120,
                )
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                roots = {pathlib.Path(node.text).resolve() for node in ET.parse(output).getroot()}
                self.assertEqual({(module / "src/main/java").resolve()}, roots)

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
