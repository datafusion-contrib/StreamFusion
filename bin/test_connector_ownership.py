"""Reject missing, leaked and wrong-version classes in real ZIP artifacts."""

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
import warnings
import zipfile

SPEC = importlib.util.spec_from_file_location(
    "connector_ownership", Path(__file__).with_name("check-connector-ownership.py"))
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class ConnectorOwnershipTest(unittest.TestCase):
    def test_manifest_identity_keeps_the_module_source_directory(self):
        script = Path(__file__).with_name("check-artifacts.sh").read_text()
        start = script.index("assert_flink_identity() {")
        helper = script[start:script.index("\n}\n", start) + 3]
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "connector.jar"
            for line, suffix in [("2.2", ""), ("1.18", "-flink1.18")]:
                with self.subTest(line=line):
                    identity = "streamfusion-kafka" + suffix
                    with zipfile.ZipFile(artifact, "w") as jar:
                        jar.writestr("META-INF/MANIFEST.MF",
                                     f"StreamFusion-Flink-Line: {line}\r\n"
                                     f"StreamFusion-Module: {identity}\r\n")
                    probe = ('set -eu\n' + helper + '\n'
                             'module=streamfusion-kafka\nflink_line=$2\n'
                             'assert_flink_identity "$1" "$3"\n'
                             'test "$module" = streamfusion-kafka\n')
                    subprocess.run(["sh", "-c", probe, "probe", str(artifact), line, identity],
                                   check=True)
                    wrong_host = "1.18" if line == "2.2" else "2.2"
                    rejected = subprocess.run(
                        ["sh", "-c", probe, "probe", str(artifact), wrong_host, identity],
                        capture_output=True, text=True)
                    self.assertNotEqual(0, rejected.returncode)
                    self.assertIn("has identity", rejected.stderr)

    def test_packaged_ownership_and_negative_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            for root, name in [("java", "Sink"), ("java-flink1.18", "Legacy"),
                               ("java-flink2.2", "Modern")]:
                source = module / f"src/main/{root}/probe/{name}.java"
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text(f"package probe; public class {name} {{}}")
            spi = "META-INF/services/probe.Extension"
            resource = module / "src/main/resources" / spi
            resource.parent.mkdir(parents=True)
            resource.write_bytes(b"probe.Sink\n")
            valid = {"probe/Sink.class": b"", "probe/Sink$Writer.class": b"",
                     "probe/Modern.class": b"", spi: resource.read_bytes()}
            cases = [
                (valid, None),
                ({k: v for k, v in valid.items() if k != "probe/Modern.class"}, "missing classes"),
                ({k: v for k, v in valid.items() if k != "probe/Sink.class"}, "missing classes"),
                ({**valid, "probe/Core.class": b""}, "unexpected classes"),
                ({**valid, "probe/Core$Leaked.class": b""}, "unexpected classes"),
                ({**valid, "probe/Legacy.class": b""}, "unexpected classes"),
                ({**valid, spi: b"probe.Other\n"}, "service providers"),
                ({k: v for k, v in valid.items() if k != spi}, "service providers"),
            ]
            for entries, error in cases:
                with self.subTest(error=error):
                    artifact = module / "connector.jar"
                    with zipfile.ZipFile(artifact, "w") as jar:
                        for entry, content in entries.items():
                            jar.writestr(entry, content)
                    if error:
                        with self.assertRaisesRegex(ValueError, error):
                            CHECKER.check(module, artifact, "2.2")
                    else:
                        self.assertEqual(2, CHECKER.check(module, artifact, "2.2"))

    def test_conflicting_version_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            for root in ("java", "java-flink1.18"):
                source = module / f"src/main/{root}/probe/Sink.java"
                source.parent.mkdir(parents=True)
                source.write_text("package probe; public class Sink {}")
            with self.assertRaisesRegex(ValueError, "duplicate production source"):
                CHECKER.check(module, module / "unused.jar", "1.18")

    def test_source_attachment_ownership_contents_and_payloads(self):
        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            for root, name in [("java", "Sink"), ("java-flink1.18", "Legacy"),
                               ("java-flink2.2", "Modern")]:
                source = module / f"src/main/{root}/probe/{name}.java"
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text(f"package probe; public class {name} {{}}")
            for line, host, other in [("1.18", "Legacy", "Modern"), ("2.2", "Modern", "Legacy")]:
                common = "probe/Sink.java"
                adapter = f"probe/{host}.java"
                valid = {entry: source.read_bytes()
                         for entry, source in CHECKER.source_files(module, line).items()}
                cases = [
                    (valid, None),
                    ({k: v for k, v in valid.items() if k != common}, "missing sources"),
                    ({k: v for k, v in valid.items() if k != adapter}, "missing sources"),
                    ({**valid, "probe/Core.java": b""}, "unexpected sources"),
                    ({**valid, f"probe/{other}.java": b""}, "unexpected sources"),
                    ({**valid, common: b"stale source"}, "source contents differ"),
                    ({**valid, "probe/Sink.class": b""}, "executable/native payload"),
                    ({**valid, "libstreamfusion.so": b""}, "executable/native payload"),
                    ({**valid, "libstreamfusion.dylib": b""}, "executable/native payload"),
                    ({**valid, "streamfusion.dll": b""}, "executable/native payload"),
                ]
                for entries, error in cases:
                    with self.subTest(line=line, error=error):
                        artifact = module / "sources.jar"
                        with zipfile.ZipFile(artifact, "w") as jar:
                            for entry, content in entries.items():
                                jar.writestr(entry, content)
                        if error:
                            with self.assertRaisesRegex(ValueError, error):
                                CHECKER.check_sources(module, artifact, line)
                        else:
                            self.assertEqual(2, CHECKER.check_sources(module, artifact, line))
                with self.assertRaises(FileNotFoundError):
                    CHECKER.check_sources(module, module / "absent-sources.jar", line)

    def test_source_attachment_rejects_duplicate_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            source = module / "src/main/java/probe/Sink.java"
            source.parent.mkdir(parents=True)
            source.write_text("package probe; public class Sink {}")
            for duplicate in ("probe/Sink.java", "META-INF/MANIFEST.MF"):
                with self.subTest(duplicate=duplicate):
                    artifact = module / "sources.jar"
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        with zipfile.ZipFile(artifact, "w") as jar:
                            jar.writestr("probe/Sink.java", source.read_bytes())
                            if duplicate.startswith("META-INF"):
                                jar.writestr(duplicate, b"")
                            jar.writestr(duplicate, b"")
                    with self.assertRaisesRegex(ValueError, "duplicate ZIP entries"):
                        CHECKER.check_sources(module, artifact, "2.2")


if __name__ == "__main__":
    unittest.main()
