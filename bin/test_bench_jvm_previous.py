"""Keep historical runtime fixtures complete without replacing production code."""

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "bench_jvm_previous", Path(__file__).with_name("bench-jvm-previous.py")
)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class HistoricalFixturesTest(unittest.TestCase):
    def test_each_released_line_copies_its_context_dependency_and_records_its_hash(self):
        for line in ("1.18", "2.2"):
            with self.subTest(line=line), tempfile.TemporaryDirectory() as directory:
                worktree = Path(directory)
                production = worktree / "src/main/java/Runtime.java"
                production.parent.mkdir(parents=True)
                production.write_text("historical runtime\n")

                hashes = BENCH.copy_fixtures(BENCH.ROOT, worktree, line)

                expected = {
                    f"src/test/java/tech/streamfusion/{name}" for name in BENCH.FIXTURES
                }
                helper = (
                    f"src/test/java-flink{line}/tech/streamfusion/compat/"
                    "FlinkTestContexts.java"
                )
                self.assertEqual(expected | {helper}, set(hashes))
                self.assertIn(
                    "tech.streamfusion.compat.FlinkTestContexts.setForLocalTests(",
                    (worktree / "src/test/java/tech/streamfusion/RecoveryExecutionMetrics.java")
                    .read_text(),
                )
                for relative, checksum in hashes.items():
                    copied = (worktree / relative).read_bytes()
                    self.assertEqual((BENCH.ROOT / relative).read_bytes(), copied)
                    self.assertEqual(hashlib.sha256(copied).hexdigest(), checksum)
                self.assertEqual("historical runtime\n", production.read_text())
                self.assertEqual(
                    expected | {helper, "src/main/java/Runtime.java"},
                    {str(path.relative_to(worktree)) for path in worktree.rglob("*.java")},
                )


if __name__ == "__main__":
    unittest.main()
