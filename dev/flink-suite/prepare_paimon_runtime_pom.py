#!/usr/bin/env python3
"""Run unchanged common tests with the released, line-specific Paimon runtime."""

from pathlib import Path
import sys
import shutil
import xml.etree.ElementTree as ET


NS = "http://maven.apache.org/POM/4.0.0"
ET.register_namespace("", NS)


def child(parent, name):
    element = parent.find(f"{{{NS}}}{name}")
    return element if element is not None else ET.SubElement(parent, f"{{{NS}}}{name}")


def prepare(source, runtime, output):
    source, runtime, output = (Path(path).resolve() for path in (source, runtime, output))
    if not source.is_file() or not runtime.is_file():
        raise ValueError("The upstream POM and released runtime JAR must exist")
    if output == source:
        raise ValueError("The upstream POM must remain unchanged")
    module = source.parent
    tests = module / "target/test-classes"
    if not tests.is_dir():
        raise ValueError("Compile the unchanged upstream tests before preparing their runtime")
    artifact = "paimon-flink-1.18"
    version = runtime.parent.name
    if runtime.name != f"{artifact}-{version}.jar":
        raise ValueError("Expected the canonical released Flink 1.18 runtime JAR")
    classes = output.with_suffix(".classes")
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir(parents=True)
    tree = ET.parse(source)
    root = tree.getroot()
    dependency = ET.Element(f"{{{NS}}}dependency")
    for name, value in {"groupId": "org.apache.paimon", "artifactId": artifact,
                        "version": version, "scope": "test"}.items():
        child(dependency, name).text = value
    child(root, "dependencies").insert(0, dependency)
    child(child(root, "parent"), "relativePath").text = str(module.parent / "pom.xml")
    build = child(root, "build")
    child(build, "directory").text = str(module / "target")
    plugins = child(build, "plugins")
    plugin = next((p for p in plugins if p.findtext(f"{{{NS}}}artifactId")
                   == "maven-surefire-plugin"), None)
    if plugin is None:
        plugin = ET.SubElement(plugins, f"{{{NS}}}plugin")
        child(plugin, "groupId").text = "org.apache.maven.plugins"
        child(plugin, "artifactId").text = "maven-surefire-plugin"
    config = child(plugin, "configuration")
    for name, value in {
        "classesDirectory": classes,
        "testClassesDirectory": tests,
        "reportsDirectory": module / "target/surefire-reports",
        "workingDirectory": module,
    }.items():
        child(config, name).text = str(value)
    child(child(config, "systemPropertyVariables"), "project.basedir").text = str(module)
    output.parent.mkdir(parents=True, exist_ok=True)
    tree.write(output, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    prepare(*sys.argv[1:])
