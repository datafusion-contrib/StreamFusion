"""Keep historical runtime fixtures complete without replacing production code."""

import hashlib
from pathlib import Path
import tempfile
import unittest

from dev.tools.benchmarks.test_sources import fixture_source


from dev.tools.benchmarks import jvm_previous as BENCH


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
                    self.assertEqual(fixture_source(BENCH.ROOT, source_relative.relative_to("src/test")).read_bytes(), copied)
                    self.assertEqual(hashlib.sha256(copied).hexdigest(), checksum)
                self.assertEqual("historical runtime\n", production.read_text())
                self.assertEqual(
                    expected | {helper, counter_reporter, "src/main/java/Runtime.java"},
                    {str(path.relative_to(worktree)) for path in worktree.rglob("*.java")},
                )


if __name__ == "__main__":
    unittest.main()
