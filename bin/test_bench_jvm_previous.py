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
        for line, layout in ((line, layout) for line in ("1.18", "2.2")
                             for layout in ("", "streamfusion-runtime/")):
            with self.subTest(line=line, layout=layout), tempfile.TemporaryDirectory() as directory:
                worktree = Path(directory)
                (worktree / layout / "src/test/java").mkdir(parents=True)
                production = worktree / "src/main/java/Runtime.java"
                production.parent.mkdir(parents=True)
                production.write_text("historical runtime\n")

                hashes = BENCH.copy_fixtures(BENCH.ROOT, worktree, line)

                expected = {
                    f"{layout}src/test/java/tech/streamfusion/{name}" for name in BENCH.FIXTURES
                }
                helper = (
                    f"{layout}src/test/java-flink{line}/tech/streamfusion/compat/"
                    "FlinkTestContexts.java"
                )
                counter_reporter = (
                    f"{layout}src/test/java/org/apache/flink/runtime/testutils/"
                    "RetainedCounterReporter.java"
                )
                self.assertEqual(expected | {helper, counter_reporter}, set(hashes))
                self.assertIn(
                    "new org.apache.flink.runtime.testutils.RetainedCounterReporter()",
                    (worktree / layout / "src/test/java/tech/streamfusion/RecoveryExecutionMetrics.java")
                    .read_text(),
                )
                self.assertIn(
                    "tech.streamfusion.compat.FlinkTestContexts.setForLocalTests(",
                    (worktree / layout / "src/test/java/tech/streamfusion/RecoveryExecutionMetrics.java")
                    .read_text(),
                )
                for relative, checksum in hashes.items():
                    copied = (worktree / relative).read_bytes()
                    source_relative = Path(relative).relative_to(layout or ".")
                    self.assertEqual((BENCH.ROOT / "streamfusion-runtime" / source_relative).read_bytes(), copied)
                    self.assertEqual(hashlib.sha256(copied).hexdigest(), checksum)
                self.assertEqual("historical runtime\n", production.read_text())
                self.assertEqual(
                    expected | {helper, counter_reporter, "src/main/java/Runtime.java"},
                    {str(path.relative_to(worktree)) for path in worktree.rglob("*.java")},
                )


if __name__ == "__main__":
    unittest.main()
