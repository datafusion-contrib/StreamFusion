"""Preserve fixture bytes and reject unresolved or unsafe historical source layouts."""

import hashlib
from pathlib import Path
import tempfile
import unittest

from dev.tools.benchmarks.test_sources import copy_test_sources, fixture_source, test_root


class BenchmarkTestSourcesTest(unittest.TestCase):
    def test_extracted_owners_copy_to_old_layouts_and_preserve_new_owners(self):
        for target_layout in ("legacy", "extracted"):
            with self.subTest(layout=target_layout), tempfile.TemporaryDirectory() as directory:
                source, target = Path(directory) / "source", Path(directory) / "target"
                for checkout in (source, target):
                    (checkout / "streamfusion-runtime/src/test/java").mkdir(parents=True)
                if target_layout == "extracted":
                    (target / "streamfusion-test-utils").mkdir()
                    (target / "streamfusion-test-utils/pom.xml").write_text("test owner")
                for owner in ("streamfusion-test-utils/src/main", "streamfusion-runtime/src/benchmark"):
                    fixture = "java/" + owner.split("/")[0] + "/Fixture.java"
                    original = source / owner / fixture
                    original.parent.mkdir(parents=True, exist_ok=True)
                    original.write_bytes(b"unchanged fixture\r\n")
                    hashes = copy_test_sources(source, target, [fixture])
                    destination = target / (owner if target_layout == "extracted"
                                            else "streamfusion-runtime/src/test") / fixture
                    self.assertEqual(original.read_bytes(), destination.read_bytes())
                    self.assertIn(destination.relative_to(target).as_posix(), hashes)
                    duplicate = source / "streamfusion-runtime/src/test" / fixture
                    duplicate.parent.mkdir(parents=True, exist_ok=True)
                    duplicate.write_bytes(original.read_bytes())
                    with self.assertRaisesRegex(ValueError, "Ambiguous"):
                        fixture_source(source, fixture)

    def test_fixture_bytes_are_preserved_across_both_source_and_target_layouts(self):
        for source_layout in ("src/test", "streamfusion-runtime/src/test"):
            for target_layout in ("src/test", "streamfusion-runtime/src/test"):
                with self.subTest(source=source_layout, target=target_layout), \
                        tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    source, target = root / "source", root / "target"
                    (source / source_layout / "java").mkdir(parents=True)
                    (target / target_layout / "java").mkdir(parents=True)
                    fixture = Path("java-flink1.18/compat/Fixture.java")
                    original = source / source_layout / fixture
                    original.parent.mkdir(parents=True)
                    original.write_bytes(b"fixture\x00\xff\r\n")
                    copied = copy_test_sources(source, target, [fixture])
                    path = target / target_layout / fixture
                    self.assertEqual(original.read_bytes(), path.read_bytes())
                    self.assertEqual({path.relative_to(target).as_posix():
                                      hashlib.sha256(original.read_bytes()).hexdigest()}, copied)

    def test_unknown_and_ambiguous_layouts_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "one shared Java test source tree"):
                test_root(root)
            for layout in ("src/test", "streamfusion-runtime/src/test"):
                (root / layout / "java").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "one shared Java test source tree"):
                test_root(root)

    def test_missing_fixtures_and_escaping_paths_are_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "source", root / "target"
            for checkout in (source, target):
                (checkout / "src/test/java").mkdir(parents=True)
            with self.assertRaises(FileNotFoundError):
                copy_test_sources(source, target, ["java/Missing.java"])
            for fixture in ("../outside", "/absolute"):
                with self.subTest(fixture=fixture), self.assertRaisesRegex(ValueError, "Invalid"):
                    copy_test_sources(source, target, [fixture])


if __name__ == "__main__":
    unittest.main()
