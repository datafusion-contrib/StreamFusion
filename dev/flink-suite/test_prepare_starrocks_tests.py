"""Keep the released StarRocks sink ITs and their data byte-for-byte unchanged."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("prepare_starrocks_tests.py")
SPEC = importlib.util.spec_from_file_location("prepare_starrocks_tests", SCRIPT)
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


class StarRocksTestPreparationTest(unittest.TestCase):
    def test_copies_every_source_byte_for_byte_and_records_its_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            originals = {}
            for test in PREPARE.TESTS:
                source = root / "upstream/src/test/java/com/starrocks/connector/flink" / test
                source.parent.mkdir(parents=True, exist_ok=True)
                text = ("tEnv.executeSql(\"INSERT INTO sink SELECT * FROM src\");\r\n"
                        "assertEquals(expected, \u00e9actual);\n" + test).encode()
                source.write_bytes(text)
                originals[test] = text
            for resource in PREPARE.RESOURCES:
                source = root / "upstream/src/test/resources" / resource
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_bytes(b'{"before":null,"after":{"c0":1}}\r\n' + resource.encode())

            PREPARE.prepare(root / "upstream", root / "compiled", root / "resources")

            manifest = json.loads((root / "compiled/manifest.json").read_text())
            tests = [entry for entry in manifest if "test" in entry]
            self.assertEqual(list(PREPARE.TESTS), [entry["test"] for entry in tests])
            for entry in tests:
                compiled = root / "compiled/com/starrocks/connector/flink" / entry["test"]
                self.assertEqual(originals[entry["test"]], compiled.read_bytes())
                self.assertEqual("none", entry["adaptation"])
                self.assertEqual(entry["source_sha256"], entry["compiled_sha256"])
            for test, original in originals.items():
                source = root / "upstream/src/test/java/com/starrocks/connector/flink" / test
                self.assertEqual(original, source.read_bytes())

    def test_copies_the_replayed_change_logs_unchanged_onto_the_resource_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for test in PREPARE.TESTS:
                source = root / "upstream/src/test/java/com/starrocks/connector/flink" / test
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("class Present {}\n")
            originals = {}
            for resource in PREPARE.RESOURCES:
                source = root / "upstream/src/test/resources" / resource
                source.parent.mkdir(parents=True, exist_ok=True)
                text = b'{"op":"u","before":{"c0":1},"after":{"c0":2}}\r\n' + resource.encode()
                source.write_bytes(text)
                originals[resource] = text

            PREPARE.prepare(root / "upstream", root / "compiled", root / "resources")

            manifest = json.loads((root / "compiled/manifest.json").read_text())
            resources = [entry for entry in manifest if "resource" in entry]
            self.assertEqual(list(PREPARE.RESOURCES), [entry["resource"] for entry in resources])
            for entry in resources:
                copied = root / "resources" / entry["resource"]
                self.assertEqual(originals[entry["resource"]], copied.read_bytes())
                self.assertEqual("none", entry["adaptation"])
                self.assertEqual(entry["source_sha256"], entry["copied_sha256"])
            self.assertFalse((root / "compiled/data").exists())

    def test_a_missing_pinned_source_fails_instead_of_shrinking_the_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for test in PREPARE.TESTS[:-1]:
                source = root / "upstream/src/test/java/com/starrocks/connector/flink" / test
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("class Present {}\n")

            with self.assertRaises(FileNotFoundError):
                PREPARE.prepare(root / "upstream", root / "compiled", root / "resources")

    def test_a_missing_replayed_change_log_fails_instead_of_shrinking_the_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for test in PREPARE.TESTS:
                source = root / "upstream/src/test/java/com/starrocks/connector/flink" / test
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("class Present {}\n")

            with self.assertRaises(FileNotFoundError):
                PREPARE.prepare(root / "upstream", root / "compiled", root / "resources")


if __name__ == "__main__":
    unittest.main()
