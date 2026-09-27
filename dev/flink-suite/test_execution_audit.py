import base64
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import summarize


class ExecutionAuditTest(unittest.TestCase):
    CALC = "org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testLongProjectionList"
    WINDOW = "org.apache.flink.table.planner.runtime.stream.sql.WindowDistinctAggregateITCase#testHopWindow"
    REASON = "window aggregate: attached-window aggregation requires two-phase execution"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.evidence = self.root / "native"
        self.evidence.mkdir()
        self.output = self.root / "diagnostics" / "execution-audit.json"

    def cases(self, *cases):
        suite = ET.Element("testsuite", tests=str(len(cases)))
        for kind in ("failure", "error", "skipped"):
            suite.set({"failure": "failures", "error": "errors"}.get(kind, kind),
                      str(sum(outcome == kind for _, outcome in cases)))
        for test, outcome in cases:
            classname, name = test.split("#")
            case = ET.SubElement(suite, "testcase", classname=classname, name=name)
            if outcome != "passed":
                ET.SubElement(case, outcome, message="fixture outcome")
        ET.ElementTree(suite).write(self.reports / "TEST-fixture.xml")

    def record(self, name, test, counts, variant="*", reasons=""):
        encoded = base64.b64encode(reasons.encode()).decode()
        (self.evidence / f"{name}.tsv").write_text(
            f"{test}\t{variant}\t{counts}\t{encoded}\n"
        )

    def run_audit(self, *extra):
        args = ["summarize.py", str(self.reports), "--native-reports", str(self.evidence),
                "--audit-output", str(self.output), *extra]
        with patch.object(sys, "argv", args), redirect_stdout(io.StringIO()):
            result = summarize.main()
        return result, json.loads(self.output.read_text())

    def test_exact_denominator_keeps_uncontracted_and_skipped_cases_visible(self):
        self.cases((self.CALC, "passed"), (self.CALC, "passed"), (self.WINDOW, "passed"),
                   ("Uncontracted#query", "passed"), ("Skipped#query", "skipped"))
        self.record("native", self.CALC, "NativeCalcOperator=7")
        self.record("mixed", self.CALC, "NativeCalcOperator=3", reasons="other operator: fallback")
        self.record("fallback", self.WINDOW, "", "splitDistinct=true", self.REASON)
        status, audit = self.run_audit()
        self.assertEqual(0, status)
        summary = audit["summary"]
        self.assertEqual((5, 4, 3, 1, 1), tuple(summary[key] for key in
                         ("tests", "executed", "contracted_executed",
                          "unclassified_outside_contract_scope", "skipped")))
        self.assertEqual({"native": 1, "mixed": 1, "full_fallback": 1},
                         summary["satisfied_evidence_records_by_route"])
        self.assertEqual(list(range(5)), [case["case_index"] for case in audit["testcases"]])
        self.assertEqual("unclassified", audit["scope"]["outside_contract_scope"])
        fallback = next(row for row in audit["execution_evidence"]
                        if row["variant"] == "splitDistinct=true")
        self.assertEqual([self.REASON], fallback["fallback_reasons"])
        self.assertEqual({}, fallback["native_input_rows"])
        # Witnesses cannot be paired to duplicate XML cases without an invocation identifier.
        self.assertTrue(all("case_index" not in row for row in audit["execution_evidence"]))

    def test_missing_and_zero_work_evidence_remain_failures(self):
        self.cases((self.CALC, "passed"))
        status, audit = self.run_audit()
        self.assertEqual(1, status)
        self.assertEqual("failed", audit["status"])
        self.assertEqual(1, audit["summary"]["contracted_executed"])
        self.assertEqual(0, audit["summary"]["evidence_records"])
        self.assertTrue(audit["validation_problems"])
        self.record("zero", self.CALC, "NativeCalcOperator=0")
        status, audit = self.run_audit()
        self.assertEqual(1, status)
        self.assertEqual({}, audit["summary"]["satisfied_evidence_records_by_route"])
        self.assertEqual("unclassified", audit["execution_evidence"][0]["observed_route"])
        self.assertFalse(audit["execution_evidence"][0]["contract_satisfied"])

    def test_stale_or_simulated_evidence_cannot_inflate_executed_denominator(self):
        self.cases(("Uncontracted#query", "passed"))
        self.record("stale", self.CALC, "NativeCalcOperator=7")
        status, audit = self.run_audit()
        self.assertEqual(1, status)
        self.assertEqual(0, audit["summary"]["contracted_executed"])
        self.assertEqual(1, audit["summary"]["executed"])
        self.assertTrue(any("0 executed invocations" in p for p in audit["validation_problems"]))

    def test_malformed_evidence_is_preserved_as_a_validation_problem(self):
        self.cases((self.CALC, "passed"))
        self.record("invalid", self.CALC, "NativeCalcOperator=-1")
        status, audit = self.run_audit()
        self.assertEqual(1, status)
        self.assertEqual([], audit["execution_evidence"])
        self.assertTrue(any("invalid operator count" in p for p in audit["validation_problems"]))

    def test_missing_and_malformed_reports_write_failed_artifacts(self):
        status, audit = self.run_audit()
        self.assertEqual(2, status)
        self.assertEqual("failed", audit["status"])
        (self.reports / "TEST-broken.xml").write_text("<broken")
        status, audit = self.run_audit()
        self.assertEqual(1, status)
        self.assertEqual(1, audit["summary"]["reports_found"])
        self.assertEqual(0, audit["summary"]["reports_parsed"])
        self.assertTrue(audit["validation_problems"])

    def test_test_failure_is_not_mislabelled_as_a_host_failure_or_fallback(self):
        self.cases(("Uncontracted#query", "failure"), ("Uncontracted#error", "error"))
        status, audit = self.run_audit("--xfail", "Uncontracted#query")
        self.assertEqual(1, status)
        self.assertEqual(1, audit["summary"]["expected_failures"])
        self.assertEqual(2, audit["summary"]["unclassified_outside_contract_scope"])
        self.assertEqual(["failure", "error"], [case["outcome"] for case in audit["testcases"]])
        self.assertTrue(audit["testcases"][0]["expected_failure"])
        self.assertEqual([], audit["execution_evidence"])

    def test_process_failure_cannot_be_hidden_by_passing_cases(self):
        self.cases(("Uncontracted#query", "passed"))
        status, audit = self.run_audit("--process-exit", "137")
        self.assertEqual(137, status)
        self.assertEqual("failed", audit["status"])
        self.assertEqual(137, audit["summary"]["result_exit"])
        self.assertTrue(any("Maven/process" in p for p in audit["validation_problems"]))

    def inventory(self, *observations):
        import uuid
        directory = self.root / "inventory"
        directory.mkdir(exist_ok=True)
        tree = ET.parse(self.reports / "TEST-fixture.xml")
        for case, observation in zip(tree.getroot().findall("testcase"), observations, strict=True):
            if observation is None:
                continue
            identity = str(uuid.uuid4())
            record = dict(schema_version=1, flink_line="2.2", invocation_id=identity,
                          junit_id="junit-" + identity, display_name=case.get("name"),
                          planners=[], plans=[], translations=[], sql=[], operation_failures=[],
                          **observation)
            (directory / (identity + ".json")).write_text(json.dumps(record))
            ET.SubElement(case, "system-out").text = "StreamFusion SQL inventory: " + identity
        tree.write(self.reports / "TEST-fixture.xml")
        return ["--sql-inventory", str(directory), "--flink-line", "2.2"]

    def observation(self, name, count=7, variant="*"):
        return dict(native_work={"NativeCalcOperator": count},
                    execution_contracts=[dict(record_id=name, test=self.CALC, variant=variant)])

    def test_parameterized_cases_join_exact_witnesses(self):
        self.cases((self.CALC + "[1]", "passed"), (self.CALC + "[2]", "passed"))
        self.record("first", self.CALC, "NativeCalcOperator=7")
        self.record("second", self.CALC, "NativeCalcOperator=9")
        args = self.inventory(self.observation("first"), self.observation("second", 9))
        status, audit = self.run_audit(*args)
        self.assertEqual(0, status, audit["validation_problems"])
        self.assertEqual("exact_junit_invocation", audit["scope"]["evidence_granularity"])
        self.assertEqual(2, audit["summary"]["contracted_executed"])
        for index, record in enumerate(audit["execution_evidence"]):
            self.assertEqual(index, record["case_index"])
            self.assertEqual(audit["testcases"][index]["invocation_id"], record["invocation_id"])

    def test_duplicate_links_cannot_satisfy_two_invocations(self):
        self.cases((self.CALC, "passed"), (self.CALC, "passed"))
        self.record("first", self.CALC, "NativeCalcOperator=7")
        self.record("second", self.CALC, "NativeCalcOperator=7")
        args = self.inventory(self.observation("first"), self.observation("first"))
        status, audit = self.run_audit(*args)
        self.assertEqual(1, status)
        self.assertTrue(any("Duplicate or missing linked witness" in p for p in audit["validation_problems"]))
        self.assertTrue(all("invocation_id" not in row for row in audit["execution_evidence"]))

    def test_bad_link_or_work_fails_closed(self):
        for observation in [self.observation("absent"), self.observation("native", 1),
                            self.observation("native", variant="wrong"),
                            self.observation("native", count=True), {},
                            dict(native_work={}, execution_contracts=[])]:
            with self.subTest(observation=observation):
                import shutil
                shutil.rmtree(self.root / "inventory", ignore_errors=True)
                self.cases((self.CALC, "passed"))
                self.record("native", self.CALC, "NativeCalcOperator=7")
                status, audit = self.run_audit(*self.inventory(observation))
                self.assertEqual(1, status)
                self.assertTrue(any("Invalid invocation execution join" in p for p in audit["validation_problems"]))

    def test_malformed_inventory_still_writes_failed_audit(self):
        self.cases(("Uncontracted#query", "passed"))
        args = self.inventory(dict(native_work={}, execution_contracts=[]))
        path = next((self.root / "inventory").glob("*.json"))
        path.write_text("[]")
        status, audit = self.run_audit(*args)
        self.assertEqual(1, status)
        self.assertEqual("failed", audit["status"])
        self.assertTrue(any("Wrong schema" in p for p in audit["validation_problems"]))

    def test_uncontracted_work_is_retained_without_inventing_route_or_skip_evidence(self):
        self.cases(("Uncontracted#query", "passed"), ("Skipped#query", "skipped"))
        args = self.inventory(dict(native_work={"NativeCalcOperator": 3}, execution_contracts=[]), None)
        status, audit = self.run_audit(*args)
        self.assertEqual(0, status)
        self.assertEqual({"NativeCalcOperator": 3}, audit["testcases"][0]["native_input_rows"])
        self.assertEqual("", audit["testcases"][1]["invocation_id"])
        self.assertEqual("unclassified", audit["scope"]["outside_contract_scope"])


if __name__ == "__main__":
    unittest.main()
