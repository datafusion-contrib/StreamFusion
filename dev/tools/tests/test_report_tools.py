"""Retain complete paired observations and report formats without starting benchmark jobs."""

from contextlib import redirect_stdout
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest

from dev.tools.benchmarks import iceberg_report, paimon_report


def fixture(root, kind):
    method = dict(events=200, warmups=1, runs=2, parallelism=4,
                  completionProtocol="fixture checkpoint", expectedCells=[["q8", "exact"]])
    rows = []
    for trial in (-1, 0, 1):
        pair = dict(kind="parity", query="q8", variant="exact", trial=trial, matched=True)
        if kind == "iceberg":
            pair["mode"] = "memory"
        rows.append(pair)
        for native in (False, True):
            engine = ("streamfusion" if native else "flink") if kind == "iceberg" else ("native" if native else "stock")
            row = dict(kind="measurement", query="q8", variant="exact", trial=trial,
                       warmup=trial < 0, engine=engine, seconds=1.0 if native else 2.0,
                       fingerprint={"count": 10}, evidence="not-retained", parquetFiles=1,
                       rustParquetFiles=int(native), parquetBytes=128, sink="paimon-fixed-bucket")
            if kind == "iceberg":
                row["mode"] = "memory"
            rows.append(row)
    if kind == "iceberg":
        method["expectedCells"] = [["q8", "memory", "exact"]]
    (root / "method.json").write_text(json.dumps(method))
    (root / "observations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    return rows


class ReportToolsTest(unittest.TestCase):
    def test_paimon_report_keeps_warmups_all_pairs_and_native_file_witnesses(self):
        with tempfile.TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
            root = Path(temporary)
            fixture(root, "paimon")
            paimon_report.report(root)
            audit = json.loads((root / "audit.json").read_text())
            self.assertEqual(6, audit["allTrials"])
            self.assertEqual(4, audit["measuredTrials"])
            self.assertEqual(3, audit["passingPairs"])
            self.assertEqual(2.0, audit["exactGeomean"])
            with (root / "metrics.csv").open() as stream:
                self.assertEqual(6, len(list(csv.DictReader(stream))))
            self.assertIn("2.000 [2.000", (root / "results.md").read_text())

    def test_paimon_missing_parity_cannot_produce_a_passing_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = fixture(root, "paimon")
            (root / "observations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows[1:]))
            with self.assertRaises(AssertionError):
                paimon_report.report(root)
            self.assertFalse((root / "audit.json").exists())

    def test_iceberg_report_retains_complete_repetitions_and_metric_columns(self):
        with tempfile.TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
            root = Path(temporary)
            fixture(root, "iceberg")
            iceberg_report.report(root)
            with (root / "metrics.csv").open() as stream:
                metrics = list(csv.DictReader(stream))
            self.assertEqual({"flink", "streamfusion"}, {row["engine"] for row in metrics})
            self.assertEqual({"2"}, {row["repetitions"] for row in metrics})
            self.assertTrue(all("checkpoint_state_median_bytes" in row for row in metrics))
            self.assertIn("2.000 [2.000", (root / "results.md").read_text())

    def test_iceberg_unpaired_measurement_cannot_produce_a_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = fixture(root, "iceberg")
            rows = [row for row in rows if not (row["kind"] == "parity" and row["trial"] == 1)]
            (root / "observations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
            with self.assertRaisesRegex(SystemExit, "Unpaired measurement"):
                iceberg_report.report(root)
            self.assertFalse((root / "results.md").exists())


if __name__ == "__main__":
    unittest.main()
