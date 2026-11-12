"""Reject stale shared builds and exercise fresh-runner Maven reactor resolution."""

import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import ci_java_build as build


class SharedJavaBuildTest(unittest.TestCase):
    def test_runtime_collector_options_do_not_change_the_compiler_identity(self):
        original_run = subprocess.run
        def run(command, **kwargs):
            if command[0] == "git":
                return subprocess.CompletedProcess(command, 0, stdout="source\n")
            return original_run(command, **kwargs)
        versions = []
        for collector in ("UseG1GC", "UseSerialGC"):
            options = "-XX:+" + collector
            with patch.dict(os.environ, {"JAVA_TOOL_OPTIONS": options,
                                        "JDK_JAVA_OPTIONS": "-Dfixture.jdk.option=true",
                                        "_JAVA_OPTIONS": "-Dfixture.legacy.option=true"}), \
                    patch.object(build.subprocess, "run", side_effect=run):
                versions.append(build.identity(Path("."), "2.2")["java"])
                self.assertEqual(options, os.environ["JAVA_TOOL_OPTIONS"])
        self.assertEqual(versions[0], versions[1])
        self.assertNotIn("Picked up", versions[0])

    def fixture(self, root):
        for module in build.MODULES:
            classes = root / module / "target/classes/example"
            classes.mkdir(parents=True)
            (classes / "Example.class").write_bytes(b"compiled bytes")
            (classes / "resource.txt").write_text("resource")
        tests = root / "streamfusion-runtime/target/test-classes/example"
        tests.mkdir(parents=True)
        (tests / "CanonicalExampleJobTest.class").touch()
        for name in ("surefire-reports/TEST-example.xml", "canonical-state/checkpoint", "diagnostics/log"):
            output = root / "streamfusion-runtime/target" / name
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("old outcome")

    def test_transfer_keeps_compiled_code_and_resources_but_no_execution_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            root, restored = temporary / "producer", temporary / "consumer"
            self.fixture(root)
            archive = temporary / "build.tar.gz"
            with patch.object(build, "identity", return_value={"commit": "same", "flink_line": "2.2"}):
                build.pack(root, "2.2", archive)
                build.restore(restored, "2.2", archive)
                with self.assertRaisesRegex(ValueError, "fresh module outputs"):
                    build.restore(restored, "2.2", archive)
            for module in build.MODULES:
                self.assertEqual(b"compiled bytes", (restored / module / "target/classes/example/Example.class").read_bytes())
                self.assertEqual("resource", (restored / module / "target/classes/example/resource.txt").read_text())
            runtime = restored / "streamfusion-runtime/target"
            self.assertEqual({"classes", "test-classes"}, {path.name for path in runtime.iterdir()})

    def test_wrong_source_line_jdk_and_corrupt_transfers_fail_before_restoring(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            root, restored = temporary / "producer", temporary / "consumer"
            self.fixture(root)
            archive = temporary / "build.tar.gz"
            original = {"commit": "source", "flink_line": "2.2", "java": "17"}
            with patch.object(build, "identity", return_value=original):
                build.pack(root, "2.2", archive)
            for key in original:
                with self.subTest(key=key), patch.object(build, "identity", return_value={**original, key: "other"}):
                    with self.assertRaisesRegex(ValueError, key + " mismatch"):
                        build.restore(restored, "2.2", archive)
                    self.assertFalse(restored.exists())
            with archive.open("ab") as output:
                output.write(b"corruption")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                build.restore(restored, "2.2", archive)
            self.assertFalse(restored.exists())

    def test_symlinks_and_archive_paths_cannot_escape_build_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            root = temporary / "producer"
            self.fixture(root)
            archive = temporary / "build.tar.gz"
            link = root / "streamfusion-runtime/target/classes/link"
            link.symlink_to(temporary)
            with patch.object(build, "identity", return_value={"commit": "same"}):
                with self.assertRaisesRegex(ValueError, "symbolic links"):
                    build.pack(root, "2.2", archive)
                link.unlink()
                build.pack(root, "2.2", archive)
                with tarfile.open(archive) as source:
                    members = [(member, source.extractfile(member).read() if member.isfile() else b"")
                               for member in source.getmembers()]
                with tarfile.open(archive, "w:gz") as output:
                    for member, content in members:
                        output.addfile(member, io.BytesIO(content) if member.isfile() else None)
                    member = tarfile.TarInfo("streamfusion-runtime/target/classes/../../outside")
                    member.size = 1
                    output.addfile(member, io.BytesIO(b"x"))
                archive.with_name(archive.name + ".sha256").write_text(build.digest(archive))
                with self.assertRaisesRegex(ValueError, "Invalid shared build member"):
                    build.restore(temporary / "consumer", "2.2", archive)
                self.assertFalse((temporary / "consumer").exists())

    def test_precompiled_lifecycle_resolves_reactor_dependencies_and_retains_tests_and_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            root, consumer = temporary / "producer", temporary / "consumer"
            root.mkdir()
            (root / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>shared-build-fixture</groupId><artifactId>parent</artifactId>
<version>1</version><packaging>pom</packaging><properties><maven.compiler.release>11</maven.compiler.release>
<skipTests>false</skipTests><sf.runtime.tests.skip>${skipTests}</sf.runtime.tests.skip></properties><modules>
<module>streamfusion-core</module><module>streamfusion-json</module><module>streamfusion-runtime</module></modules>
<dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId>
<version>5.10.2</version><scope>test</scope></dependency></dependencies><build><plugins>
<plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
<plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.2.5</version></plugin>
</plugins></build></project>''')
            namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
            parent = ET.parse(Path(__file__).resolve().parents[1] / "pom.xml")
            profile = next(element for element in parent.findall("m:profiles/m:profile", namespace)
                           if element.findtext("m:id", namespaces=namespace) == "ci-precompiled-java")
            for element in profile.iter():
                element.tag = element.tag.split("}")[-1]
            pom = (root / "pom.xml").read_text().replace(
                "</project>", "<profiles>" + ET.tostring(profile, encoding="unicode") + "</profiles></project>")
            (root / "pom.xml").write_text(pom)
            for index, module in enumerate(build.MODULES):
                target = root / module
                source = target / "src/main/java/example"
                source.mkdir(parents=True)
                name = ("Core", "Json", "Runtime")[index]
                (source / (name + ".java")).write_text(f'package example; public class {name} {{ public static int value() {{ return {index}; }} }}')
                dependency = (f'''<dependencies><dependency><groupId>shared-build-fixture</groupId>
<artifactId>{build.MODULES[index - 1]}</artifactId><version>1</version></dependency></dependencies>''' if index else "")
                config = ('''<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-surefire-plugin</artifactId><configuration><skipTests>${sf.runtime.tests.skip}</skipTests>
</configuration></plugin></plugins></build>''' if index == 2 else "")
                (target / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><parent><groupId>shared-build-fixture</groupId><artifactId>parent</artifactId>
<version>1</version></parent><artifactId>''' + module + "</artifactId>" + dependency + config + "</project>")
                tests = target / "src/test/java/example"
                tests.mkdir(parents=True)
                if index != 2:
                    (tests / "ModuleTest.java").write_text('''package example; class ModuleTest {
@org.junit.jupiter.api.Test void reactor() { org.junit.jupiter.api.Assertions.assertFalse(Boolean.getBoolean("skipTests")); }
}''')
                else:
                    (tests / "CanonicalExampleJobTest.java").write_text('''package example; class CanonicalExampleJobTest {
@org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(ints={1,2})
void reactor(int value) { org.junit.jupiter.api.Assertions.assertEquals(1, Json.value());
org.junit.jupiter.api.Assertions.assertEquals(0, Core.value());
org.junit.jupiter.api.Assertions.assertFalse(Boolean.getBoolean("fixture.fail")); }
}''')

            def run(where, *options, success=True):
                result = subprocess.run(["mvn", "-B", "-ntp", "-pl", "streamfusion-runtime", "-am", *options],
                                        cwd=where, text=True, capture_output=True, timeout=120)
                self.assertEqual(success, result.returncode == 0, result.stdout + result.stderr)

            run(root, "test", "-Dsf.runtime.tests.skip=true")
            shutil.copytree(root, consumer, ignore=shutil.ignore_patterns("target"))
            archive = temporary / "build.tar.gz"
            with patch.object(build, "identity", return_value={"commit": "same"}):
                build.pack(root, "2.2", archive)
                build.restore(consumer, "2.2", archive)
            # Any accidental lifecycle invocation would now fail compilation.
            (consumer / "streamfusion-runtime/src/main/java/example/Runtime.java").write_text("invalid source")
            run(consumer, "test", "-Dsf.ci.precompiledJava=true", "-DskipTests=true", "-Dsf.runtime.tests.skip=false")
            report = consumer / "streamfusion-runtime/target/surefire-reports/TEST-example.CanonicalExampleJobTest.xml"
            self.assertEqual("2", ET.parse(report).getroot().get("tests"))
            for module in build.MODULES[:2]:
                self.assertFalse((consumer / module / "target/surefire-reports").exists())
            run(consumer, "test", "-Dsf.ci.precompiledJava=true", "-DskipTests=true", "-Dsf.runtime.tests.skip=false",
                "-Dfixture.fail=true", success=False)


if __name__ == "__main__":
    unittest.main()
