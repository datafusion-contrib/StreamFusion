"""Preserve fixture bytes and reject unresolved or unsafe historical source layouts."""

import hashlib
from pathlib import Path
import tempfile
import unittest

from benchmark_test_sources import copy_test_sources, test_root


class BenchmarkTestSourcesTest(unittest.TestCase):
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
