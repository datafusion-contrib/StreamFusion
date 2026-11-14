import copy
import importlib.util
from pathlib import Path
import unittest


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
