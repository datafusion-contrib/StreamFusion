"""Preserve upstream assertions while adapting released connector APIs and visibility."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("prepare_doris_tests.py")
SPEC = importlib.util.spec_from_file_location("prepare_doris_tests", SCRIPT)
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


class DorisTestPreparationTest(unittest.TestCase):
    def test_preserves_all_sql_and_assertions_and_original_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_root = root / "upstream"
            output = root / "compiled"
            originals = {}
            for test in PREPARE.TESTS:
                source = (original_root / "flink-doris-connector/flink-doris-connector-it"
                          / "src/test/java/org/apache/doris/flink" / test)
                source.parent.mkdir(parents=True, exist_ok=True)
                text = ('import com.google.common.cache.Cache;\n'
                        'tEnv.executeSql("SELECT * FROM doris");\n'
                        'assertEquals(expected, actual);\n')
                source.write_text(text)
                originals[source] = text
            helper = (original_root / "flink-doris-connector/flink-doris-connector-base"
                      / "src/test/java/org/apache/doris/flink/container/ContainerUtils.java")
            helper.parent.mkdir(parents=True, exist_ok=True)
            helper_text = (
                "List<String> actual = getResult(connection, logger, expected, query, columnSize);\n"
                "assertEquals(expected, actual);\n"
            )
            helper.write_text(helper_text)
            originals[helper] = helper_text
            PREPARE.prepare(original_root, output)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(set(PREPARE.TESTS) | {"container/ContainerUtils.java"},
                             {entry["test"] for entry in manifest})
            for source, original in originals.items():
                self.assertEqual(original, source.read_text())
            for entry in manifest:
                compiled = (output / "org/apache/doris/flink" / entry["test"]).read_text()
                original = helper_text if entry["test"] == "container/ContainerUtils.java" else text
                if entry["adaptation"] == "none":
                    self.assertEqual(original, compiled)
                    self.assertEqual(entry["source_sha256"], entry["compiled_sha256"])
                elif entry["test"] == "table/DorisRowDataJdbcLookupFunctionITCase.java":
                    self.assertEqual(original.replace("com.google.common.cache.Cache",
                        "org.apache.doris.shaded.com.google.common.cache.Cache"), compiled)
                else:
                    self.assertEqual("container/ContainerUtils.java", entry["test"])
                    self.assertEqual(original.replace(
                        "List<String> actual = getResult(connection, logger, expected, query, columnSize);",
                        "List<String> actual = tech.streamfusion.doris.DorisFixtureVisibility.await("
                        "() -> getResult(connection, logger, expected, query, columnSize), expected.size());"),
                        compiled)

    def test_rejects_unexpected_upstream_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for test in PREPARE.TESTS:
                source = (root / "upstream/flink-doris-connector/flink-doris-connector-it"
                          / "src/test/java/org/apache/doris/flink" / test)
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("unexpected API version\n")
            with self.assertRaisesRegex(ValueError, "pinned upstream cache import"):
                PREPARE.prepare(root / "upstream", root / "compiled")


if __name__ == "__main__":
    unittest.main()
