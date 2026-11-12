"""Reject missing, leaked and wrong-version classes in real ZIP artifacts."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location(
    "connector_ownership", Path(__file__).with_name("check-connector-ownership.py"))
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class ConnectorOwnershipTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
