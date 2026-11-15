import copy
import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("bench_hudi_matrix", Path(__file__).with_name("bench-hudi-matrix.py"))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def complete(backend="memory", mini_batch=False):
    observations = [{"query": query, "engine": engine, "trial": trial,
                     "milliseconds": 1, "backend": backend, "mini_batch": mini_batch,
                     "validation": "processing-time bounded counts" if query == "q12" else "full-value multiset"}
                    for query in bench.QUERIES for engine in bench.ENGINES for trial in range(3)]
    observations.append({"query": "q7", "status": "stock-planner-rejected", "reason": bench.REJECTION,
                         "backend": backend, "mini_batch": mini_batch})
    checks = [{"engine": engine, "query": "q4", "mini_batch": mini_batch, "rocksdb_opened": True}
              for engine in bench.ENGINES] if backend == "rocksdb" else []
    return {"status": "passed", "rows": 2_000_000, "input_bids": 1_840_000,
            "parallelism": 4, "source_partitions": 4, "sink_partition_modulus": 4,
            "checkpoint_ms": 1000, "warmups": 1, "runs": 3, "object_reuse": True,
            "heap_max_bytes": 8 * 1024**3, "native_task_off_heap_capacity_bytes": 48 * 1024**3,
            "observations": observations, "state_backend_checks": checks}


class HudiMatrixTest(unittest.TestCase):
    def test_focused_diagnostic_cannot_satisfy_a_full_configuration(self):
        report = complete("rocksdb", False)
        report["observations"] = [item for item in report["observations"]
                                  if item["query"] == "q10"]
        report["state_backend_checks"] = []
        bench.validate(report, "rocksdb", False, {"q10"})
        with self.assertRaisesRegex(ValueError, "Incomplete Hudi matrix"):
            bench.validate(report, "rocksdb", False)
        for selection in ["", "q7", "q6", "q10,q10", "q10,unknown"]:
            with self.assertRaises(ValueError):
                bench.diagnostic_queries(selection)
        self.assertEqual({"q10", "q8"}, bench.diagnostic_queries("q10,q8"))

    def test_diagnostic_summary_remains_separate_from_qualified_matrix_results(self):
        report = complete()
        report["observations"] = [item for item in report["observations"]
                                  if item["query"] == "q10"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            libraries = root / "release"
            libraries.mkdir()
            for name in ["libstreamfusion.so", "libstreamfusion_parquet.so"]:
                (libraries / name).write_bytes(b"native")
            output = root / "results"

            def run(command, environment, log):
                self.assertEqual("q10", environment["SF_MATRIX_QUERIES"])
                (output / "result.json").write_text(json.dumps(report))
                return 0

            args = argparse.Namespace(native_producer_run=123, backend="memory", mini_batch="false",
                                      diagnostic_queries="q10", native_library_dir=libraries,
                                      output=output)
            with mock.patch.object(bench.subprocess, "run"), \
                    mock.patch.object(bench.subprocess, "check_output", return_value="a" * 40), \
                    mock.patch.object(bench.platform, "system", return_value="Linux"), \
                    mock.patch.object(bench.platform, "platform", return_value="test-host"), \
                    mock.patch.object(bench.platform, "processor", return_value="test-cpu"), \
                    mock.patch.object(bench, "run_logged", side_effect=run):
                bench.run(args)
            summary = json.loads((output / "summary.json").read_text())
            self.assertEqual("diagnostic-passed", summary["status"])
            self.assertEqual(["q10"], summary["diagnostic_queries"])
            self.assertFalse((output / "runtime").exists())

    def test_retry_selection_preserves_successful_configurations(self):
        pairs = {(item["backend"], item["mini_batch"])
                 for item in bench.configurations("all")}
        self.assertEqual({("memory", "false"), ("memory", "true"),
                          ("rocksdb", "false"), ("rocksdb", "true")}, pairs)
        self.assertEqual([{"backend": "memory", "mini_batch": "false"},
                          {"backend": "rocksdb", "mini_batch": "false"}],
                         bench.configurations("off"))
        for backend, mini in pairs:
            self.assertEqual([{"backend": backend, "mini_batch": mini}],
                             bench.configurations(f"{backend}-{mini}"))
        for invalid in ["", "none", "memory", "rocksdb-false,memory-false"]:
            with self.assertRaises(ValueError):
                bench.configurations(invalid)

    def test_all_four_configurations_retain_every_engine_query_and_trial(self):
        for backend in ["memory", "rocksdb"]:
            for mini_batch in [False, True]:
                report = complete(backend, mini_batch)
                self.assertEqual(199, len(report["observations"]))
                bench.validate(report, backend, mini_batch)

    def test_missing_duplicate_or_relabelled_trials_cannot_pass(self):
        valid = complete()
        for mutate in [lambda r: r["observations"].pop(0),
                       lambda r: r["observations"].append(r["observations"][0]),
                       lambda r: r["observations"][0].update(engine="fallback"),
                       lambda r: r.update(rows=200_000),
                       lambda r: r["observations"][-1].update(reason="another planner failure"),
                       lambda r: r["observations"][0].update(mini_batch=True)]:
            report = copy.deepcopy(valid)
            mutate(report)
            with self.assertRaises(ValueError):
                bench.validate(report, "memory", False)

    def test_declared_rocksdb_without_actual_engagement_cannot_pass(self):
        report = complete("rocksdb", True)
        report["state_backend_checks"].pop()
        with self.assertRaisesRegex(ValueError, "actual RocksDB"):
            bench.validate(report, "rocksdb", True)
        report = complete("rocksdb", True)
        report["state_backend_checks"][0]["rocksdb_opened"] = False
        with self.assertRaisesRegex(ValueError, "actual RocksDB"):
            bench.validate(report, "rocksdb", True)


if __name__ == "__main__":
    unittest.main()
