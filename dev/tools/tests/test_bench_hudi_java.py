import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


from dev.tools.benchmarks import hudi_java as bench


class HudiJavaComparisonTest(unittest.TestCase):
    def test_comparison_reads_owned_production_and_benchmark_sources(self):
        for source in bench.JAVA:
            self.assertTrue((bench.ROOT / source).is_file(), source)
        for source in bench.FIXED:
            if source.endswith(".java"):
                self.assertTrue((bench.ROOT / source).is_file(), source)

    def test_missing_or_duplicate_trials_cannot_pass(self):
        report = {"status": "passed", "observations": []}
        with self.assertRaisesRegex(ValueError, "trial identities"):
            bench.validate(report, 200_000, ["q8"])
        report["observations"] = [{"query": "q8", "engine": "stock", "trial": 0}] * 9
        with self.assertRaisesRegex(ValueError, "trial identities"):
            bench.validate(report, 200_000, ["q8"])

    def test_native_or_harness_changes_fail_before_mutating_sources(self):
        args = argparse.Namespace(baseline="a" * 40, queries="q8", rows=200_000, pairs=1)
        with mock.patch.object(bench, "git", side_effect=["", "b" * 40, "native/engine/src/lib.rs"]):
            with self.assertRaisesRegex(ValueError, "identical native inputs"):
                bench.compare(args)

    def test_failed_maven_restores_candidate_and_removes_runtime_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in bench.JAVA:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"candidate")
            libraries = root / "release"
            libraries.mkdir()
            for name in ["libstreamfusion.so", "libstreamfusion_parquet.so"]:
                (libraries / name).write_bytes(b"library")

            def git(*args):
                if args[0] == "rev-parse":
                    return "b" * 40
                if args[:4] == ("diff", "--name-only", "a" * 40, "b" * 40) and args[-2:] == ("streamfusion-core/src/main", "streamfusion-hudi/src/main"):
                    return "\n".join(bench.JAVA)
                return ""

            def fail(*args, **kwargs):
                self.assertTrue(all((root / name).read_bytes() == b"baseline" for name in bench.JAVA))
                (root / "results/runtime/abandoned-table").write_bytes(b"fixture")
                return mock.Mock(wait=mock.Mock(return_value=1))

            args = argparse.Namespace(baseline="a" * 40, queries="q8", rows=200_000, pairs=1,
                                      native_library_dir=libraries, output=root / "results")
            with mock.patch.object(bench, "ROOT", root), mock.patch.object(bench, "git", side_effect=git), \
                    mock.patch.object(bench.platform, "system", return_value="Linux"), \
                    mock.patch.object(bench.platform, "platform", return_value="test-host"), \
                    mock.patch.object(bench.subprocess, "check_output", return_value=b"baseline"), \
                    mock.patch.object(bench.subprocess, "Popen", side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, "comparison failed"):
                    bench.compare(args)
            self.assertTrue(all((root / name).read_bytes() == b"candidate" for name in bench.JAVA))
            self.assertFalse((root / "results/runtime").exists())
            report = json.loads((root / "results/summary.json").read_text())
            self.assertEqual("failed", report["status"])
            self.assertEqual(1, report["trials"][0]["exit_code"])


if __name__ == "__main__":
    unittest.main()
