import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("hudi_suite", Path(__file__).with_name("hudi-suite.py"))
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)


class HudiSuiteEvidenceTest(unittest.TestCase):
    def test_default_corpus_has_complete_execution_contracts(self):
        manifest = json.loads((suite.FIXTURES / "invocations.json").read_text())
        classes = {case.split("#")[0] for case in manifest["cases"]}
        self.assertEqual(suite.UPSTREAM_CLASSES, classes)
        self.assertEqual(set(manifest["junit_ids"]), set(manifest["native_contracts"]))
        self.assertTrue(set(manifest["cases"].values()) <= {"passed", "skipped"})
        self.assertTrue(classes.isdisjoint(manifest["stock_incompatible_classes"]))

    def test_provenance_failure_replaces_running_report_with_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with (
                patch("sys.argv", ["hudi-suite.py", "--output", str(report)]),
                patch.object(suite.subprocess, "run"),
                patch.object(suite, "verify_provenance", side_effect=AssertionError("fixture changed")),
                self.assertRaisesRegex(AssertionError, "fixture changed"),
            ):
                suite.main()
            self.assertEqual(json.loads(report.read_text())["status"], "failed")

    def test_reports_preserve_failures_and_upstream_skips(self):
        with tempfile.TemporaryDirectory() as directory:
            reports = Path(directory)
            (reports / "TEST-fixture.xml").write_text(
                '<testsuite><testcase classname="Fixture" name="pass"/>'
                '<testcase classname="Fixture" name="fail"><failure/></testcase>'
                '<testcase classname="Fixture" name="error"><error/></testcase>'
                '<testcase classname="Fixture" name="disabled"><skipped/></testcase></testsuite>'
            )
            self.assertEqual(suite.results(reports), {
                "Fixture#pass": "passed", "Fixture#fail": "failure",
                "Fixture#error": "error", "Fixture#disabled": "skipped",
            })

    def test_empty_or_duplicate_reports_cannot_look_like_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            reports = Path(directory)
            with self.assertRaisesRegex(AssertionError, "No upstream"):
                suite.results(reports)
            xml = '<testsuite><testcase classname="Fixture" name="test"/></testsuite>'
            (reports / "TEST-one.xml").write_text(xml)
            (reports / "TEST-two.xml").write_text(xml)
            with self.assertRaisesRegex(AssertionError, "Duplicate upstream"):
                suite.results(reports)

    def test_missing_or_duplicate_execution_evidence_fails(self):
        observation = {
            "junit_id": "fixture-id", "junit_status": "SUCCESSFUL", "display_name": "fixture",
            "native_work": {"HudiParquetReader": 2},
            "plans": [{"fallback_reasons": ["Unsupported source contract"]}],
        }
        work, reasons, invocations = suite.evidence([observation], {"Fixture#test": "passed"})
        self.assertEqual(work, {"HudiParquetReader": 2})
        self.assertEqual(reasons, {"Unsupported source contract": 1})
        self.assertEqual(invocations["fixture-id"]["native_work"], work)
        with self.assertRaisesRegex(AssertionError, "Missing upstream"):
            suite.evidence([], {"Fixture#test": "passed"})
        with self.assertRaisesRegex(AssertionError, "Duplicate SQL inventory"):
            suite.evidence([observation, observation], {"Fixture#test": "passed"})
        suite.evidence([], {"Fixture#disabled": "skipped"})

    def test_native_contract_rejects_silent_fallback_and_unexpected_routes(self):
        expected = {"id": {"native_work": {"NativeCalcOperator": 1}, "fallback_reasons": ["legacy source"]}}
        observation = {"id": {"native_work": {"NativeCalcOperator": 3}, "fallback_reasons": ["legacy source"]}}
        suite.native_contracts(observation, expected)
        with self.assertRaisesRegex(AssertionError, "Missing native execution contract"):
            suite.native_contracts(observation, {})
        observation["id"]["native_work"] = {}
        with self.assertRaisesRegex(AssertionError, "Native execution changed"):
            suite.native_contracts(observation, expected)
        observation["id"]["native_work"] = {"NativeCalcOperator": 0}
        with self.assertRaisesRegex(AssertionError, "Missing native work"):
            suite.native_contracts(observation, expected)
        observation["id"]["native_work"] = {"NativeCalcOperator": 1}
        observation["id"]["fallback_reasons"] = ["unexpected type rejection"]
        with self.assertRaisesRegex(AssertionError, "Unexpected fallback"):
            suite.native_contracts(observation, expected)


if __name__ == "__main__":
    unittest.main()
