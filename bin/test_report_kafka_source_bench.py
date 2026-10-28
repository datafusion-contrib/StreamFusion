"""Audit Kafka source benchmark archives: every trial times all routes over identical output."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "report_kafka_source_bench", Path(__file__).with_name("report-kafka-source-bench.py")
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)

PLANS = {
    "stock": "TableSourceScan(table=[[t]])",
    "previous": "NativeCalc\n+- RowDataToArrow\n   +- TableSourceScan(table=[[t]])",
    "native": "NativeCalc\n+- NativeKafkaDecode(topic=[t])",
}


def archive(root, seconds, digests=None, plans=PLANS):
    (root / "method.json").write_text(json.dumps({"warmups": 1, "trials": 2}))
    lines = []
    for run in range(3):
        for position, route in enumerate(("stock", "previous", "native")):
            lines.append(json.dumps({
                "workload": "nested-maxwell", "records": 1000, "partitions": 4,
                "parallelism": 4, "route": route, "run": run, "warmup": run < 1,
                "position": position, "seconds": seconds[route][run], "rows": 10,
                "digest": (digests or {}).get((route, run), "abc"),
            }))
    (root / "observations.jsonl").write_text("\n".join(lines) + "\n")
    for route, plan in plans.items():
        (root / f"nested-maxwell-1000-{route}.plan").write_text(plan)


SECONDS = {
    "stock": [9.0, 4.0, 6.0],
    "previous": [9.0, 3.0, 5.0],
    "native": [9.0, 1.0, 3.0],
}


class ReportTest(unittest.TestCase):
    def test_reports_medians_ranges_and_ratios_of_measured_trials_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root, SECONDS)

            REPORT.main(["report", str(root)])

            results = (root / "results.md").read_text()
            self.assertIn(
                "| nested-maxwell | 1,000 | 5.000 [4.000–6.000] | 4.000 [3.000–5.000] | "
                "2.000 [1.000–3.000] | 2.50× | 2.00× |",
                results,
            )

    def test_rejects_a_run_whose_routes_disagree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root, SECONDS, digests={("native", 2): "other"})

            with self.assertRaises(AssertionError):
                REPORT.audit(root)

    def test_rejects_a_native_plan_without_native_decode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root, SECONDS, plans=dict(PLANS, native="NativeCalc\n+- TableSourceScan"))

            with self.assertRaises(AssertionError):
                REPORT.audit(root)

    def test_rejects_a_previous_plan_without_native_operators(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root, SECONDS, plans=dict(PLANS, previous="TableSourceScan(table=[[t]])"))

            with self.assertRaises(AssertionError):
                REPORT.audit(root)

    def test_rejects_a_missing_route(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root, SECONDS)
            observations = root / "observations.jsonl"
            observations.write_text("\n".join(observations.read_text().splitlines()[:-1]) + "\n")

            with self.assertRaises(AssertionError):
                REPORT.audit(root)


if __name__ == "__main__":
    unittest.main()
