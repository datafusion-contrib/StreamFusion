from collections import Counter
import base64
from contextlib import redirect_stdout
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import summarize


class NativeExecutionSummaryTest(unittest.TestCase):
    CALC = "org.apache.flink.table.planner.runtime.stream.sql.CalcITCase#testLongProjectionList"
    WINDOW = "org.apache.flink.table.planner.runtime.stream.sql.WindowDistinctAggregateITCase#testHopWindow"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.contracts = summarize.execution_contracts()

    def record(self, test, counts, name="one", variant=None, reasons=""):
        variant = variant or ("splitDistinct=false" if test == self.WINDOW else "*")
        encoded = base64.b64encode(reasons.encode()).decode()
        (self.root / f"{name}.tsv").write_text(
            f"{test}\t{variant}\t{counts}\t{encoded}\n"
        )

    def check(self, expected):
        return summarize.check_execution(self.root, Counter(expected), self.contracts)

    def test_requires_evidence_for_every_parameter_invocation(self):
        self.record(self.CALC, "NativeCalcOperator=6")
        self.assertEqual((1, 0, []), self.check({self.CALC: 1}))
        self.assertTrue(self.check({self.CALC: 2})[2])

    def test_missing_agent_fails_even_when_junit_is_green(self):
        xml = f'<testsuite tests="1"><testcase classname="{self.CALC.split("#")[0]}" name="testLongProjectionList"/></testsuite>'
        (self.root / "TEST-calc.xml").write_text(xml)
        with patch.object(
            sys, "argv", ["summarize.py", str(self.root)]
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())

    def test_old_surefire_simple_class_name_still_requires_every_invocation(self):
        full_class, method = self.CALC.split("#")
        xml = (f'<testsuite name="{full_class}" tests="2">'
               f'<testcase classname="CalcITCase" name="{method}"/>'
               f'<testcase classname="CalcITCase" name="{method}"/></testsuite>')
        (self.root / "TEST-calc.xml").write_text(xml)
        self.record(self.CALC, "NativeCalcOperator=3")
        arguments = ["summarize.py", str(self.root), "--native-reports", str(self.root)]
        with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())
        self.record(self.CALC, "NativeCalcOperator=4", name="second")
        with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
            self.assertEqual(0, summarize.main())

    def test_xfail_cannot_hide_missing_execution(self):
        xml = f'<testsuite tests="1" failures="1"><testcase classname="{self.CALC.split("#")[0]}" name="testLongProjectionList"><failure message="expected"/></testcase></testsuite>'
        (self.root / "TEST-calc.xml").write_text(xml)
        with patch.object(
            sys, "argv", ["summarize.py", str(self.root), "--xfail", self.CALC]
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())

    def test_required_uncontracted_class_must_execute_not_only_skip(self):
        full_class = "org.apache.flink.UncontractedITCase"
        arguments = ["summarize.py", str(self.root), "--require-test-class", full_class]
        for actual_class, skipped, expected in (
            (full_class, False, 0), (full_class, True, 1),
            ("org.apache.flink.DifferentITCase", False, 1),
        ):
            with self.subTest(actual_class=actual_class, skipped=skipped):
                (self.root / "TEST-class.xml").write_text(
                    f'<testsuite tests="1" skipped="{int(skipped)}">'
                    f'<testcase classname="{actual_class}" name="testRows">'
                    + ('<skipped/>' if skipped else '') + '</testcase></testsuite>'
                )
                with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
                    self.assertEqual(expected, summarize.main())

    def test_zero_rows_and_wrong_operator_fail(self):
        for counts in (
            "",
            "NativeCalcOperator=0",
            "NativeColumnarGroupAggregateOperator=4",
        ):
            with self.subTest(counts=counts):
                self.record(self.CALC, counts)
                self.assertTrue(self.check({self.CALC: 1})[2])

    def test_window_requires_complete_route(self):
        self.record(self.WINDOW, "NativeColumnarLocalWindowAggregateOperator=3")
        self.assertTrue(self.check({self.WINDOW: 1})[2])
        self.record(
            self.WINDOW,
            "NativeColumnarLocalWindowAggregateOperator=3,NativeColumnarGlobalWindowAggregateOperator=2",
        )
        self.assertEqual((1, 0, []), self.check({self.WINDOW: 1}))
        self.record(self.WINDOW, "NativeColumnarWindowAggregateOperator=3")
        self.assertEqual((1, 0, []), self.check({self.WINDOW: 1}))

    def test_one_passing_variant_cannot_hide_failed_variant(self):
        self.record(self.CALC, "NativeCalcOperator=3")
        self.record(self.CALC, "", "two")
        proved, fallback, problems = self.check({self.CALC: 2})
        self.assertEqual(1, proved)
        self.assertTrue(problems)

    def test_stale_evidence_fails(self):
        self.record(self.CALC, "NativeCalcOperator=3")
        self.assertTrue(self.check({})[2])

    def test_unselected_contracts_are_not_required(self):
        self.assertEqual((0, 0, []), self.check({}))

    def test_rejects_malformed_evidence(self):
        for counts in (
            "NativeCalcOperator=-1",
            "NativeCalcOperator=1,NativeCalcOperator=2",
            "garbage",
        ):
            with self.subTest(counts=counts):
                self.record(self.CALC, counts)
                self.assertTrue(self.check({self.CALC: 1})[2])

    def test_contract_configuration_must_be_valid(self):
        path = self.root / "contract.tsv"
        for content in (
            "",
            "garbage",
            f"{self.CALC}\t*\tNativeCalcOperator\n{self.CALC}\t*\tNativeCalcOperator",
        ):
            with self.subTest(content=content):
                path.write_text(content)
                with self.assertRaises(ValueError):
                    summarize.execution_contracts(path)

    def test_expected_fallback_is_counted_separately_and_requires_its_reason(self):
        self.record(
            self.WINDOW,
            "",
            variant="splitDistinct=true",
            reasons="window aggregate: attached-window aggregation requires two-phase execution",
        )
        self.assertEqual((0, 1, []), self.check({self.WINDOW: 1}))
        self.record(
            self.WINDOW, "", variant="splitDistinct=true", reasons="different reason"
        )
        self.assertTrue(self.check({self.WINDOW: 1})[2])
        self.record(
            self.WINDOW,
            "NativeColumnarWindowAggregateOperator=1",
            variant="splitDistinct=true",
            reasons="window aggregate: attached-window aggregation requires two-phase execution",
        )
        self.assertTrue(self.check({self.WINDOW: 1})[2])

    def test_wrong_variant_does_not_prove_native_execution(self):
        self.record(
            self.WINDOW,
            "",
            variant="splitDistinct=false",
            reasons="window aggregate: attached-window aggregation requires two-phase execution",
        )
        self.assertTrue(self.check({self.WINDOW: 1})[2])

    def test_full_suite_cannot_silently_drop_contracted_tests(self):
        (self.root / "TEST-unrelated.xml").write_text(
            '<testsuite tests="1"><testcase classname="Unrelated" name="testOther"/></testsuite>'
        )
        with patch.object(
            sys, "argv", ["summarize.py", str(self.root), "--require-all-contracts"]
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())

    def test_required_contracts_are_scoped_to_the_selected_suite(self):
        self.record(self.CALC, "NativeCalcOperator=6")
        (self.root / "TEST-calc.xml").write_text(
            f'<testsuite tests="1"><testcase classname="{self.CALC.split("#")[0]}" name="testLongProjectionList"/></testsuite>'
        )
        for prefix, status in ((self.CALC, 0), ("io.delta.", 1), ("misspelled.", 1)):
            with self.subTest(prefix=prefix), patch.object(
                sys,
                "argv",
                [
                    "summarize.py", str(self.root), "--native-reports", str(self.root),
                    "--require-contract-prefix", prefix,
                ],
            ), redirect_stdout(io.StringIO()):
                self.assertEqual(status, summarize.main())

    def test_required_stock_test_cannot_be_missing_or_skipped(self):
        for case, status in (
            ("", 1),
            ('<testcase classname="Batch" name="testRows"><skipped/></testcase>', 1),
            ('<testcase classname="Batch" name="testRows"/>', 0),
        ):
            with self.subTest(case=case):
                (self.root / "TEST-batch.xml").write_text(
                    f'<testsuite tests="1">{case}</testsuite>'
                )
                with patch.object(
                    sys,
                    "argv",
                    ["summarize.py", str(self.root), "--require-test", "Batch#testRows"],
                ), redirect_stdout(io.StringIO()):
                    self.assertEqual(status, summarize.main())


class ProcessExitSummaryTest(unittest.TestCase):
    PASS = '<testsuite tests="1"><testcase classname="Probe" name="passes"/></testsuite>'
    EXPECTED = '<testsuite tests="1" failures="1"><testcase classname="Probe" name="expected"><failure message="known upstream assertion"/></testcase></testsuite>'

    def summarize(self, xml, status, marker=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            if xml is not None:
                (root / "TEST-probe.xml").write_text(xml)
            result = root / "maven-result.tsv"
            if marker is not None:
                result.write_text(marker)
            args = ["summarize.py", str(root), "--process-exit", str(status),
                    "--maven-result", str(result), "--xfail", "Probe#expected"]
            with patch.object(sys, "argv", args), redirect_stdout(io.StringIO()):
                return summarize.main()

    def test_nonzero_process_with_passing_partial_reports_never_passes(self):
        for status in (1, 2, 124, 137, 143):
            for marker in (None, "streamfusion-maven-result-v1\tsuccess\n",
                           "streamfusion-maven-result-v1\ttest-failures\n"):
                with self.subTest(status=status, marker=marker):
                    self.assertEqual(status, self.summarize(self.PASS, status, marker))

    def test_only_completed_allowed_assertion_failure_can_excuse_exit_one(self):
        marker = "streamfusion-maven-result-v1\ttest-failures\n"
        self.assertEqual(0, self.summarize(self.EXPECTED, 1, marker))
        for status in (2, 124, 137, 143):
            self.assertEqual(status, self.summarize(self.EXPECTED, status, marker))
        for marker in (None, "garbage", "streamfusion-maven-result-v1\tfailure\n"):
            self.assertEqual(1, self.summarize(self.EXPECTED, 1, marker))

    def test_allowed_method_cannot_hide_errors_or_unexpected_assertions(self):
        marker = "streamfusion-maven-result-v1\ttest-failures\n"
        error = self.EXPECTED.replace('failures="1"', 'errors="1"').replace('<failure ', '<error ')
        unexpected = self.EXPECTED.replace('name="expected"', 'name="other"')
        for xml in (error, unexpected):
            self.assertEqual(1, self.summarize(xml, 1, marker))

    def test_success_requires_a_completed_session_when_evidence_is_requested(self):
        self.assertEqual(0, self.summarize(self.PASS, 0, "streamfusion-maven-result-v1\tsuccess\n"))
        for marker in (None, "garbage", "streamfusion-maven-result-v1\tfailure\n"):
            self.assertEqual(1, self.summarize(self.PASS, 0, marker))

    def test_missing_malformed_and_incomplete_reports_fail(self):
        marker = "streamfusion-maven-result-v1\tsuccess\n"
        for xml in (None, "<broken", "<other/>", '<testsuite tests="2"><testcase/></testsuite>',
                    '<testsuite tests="NaN"/>', '<testsuite tests="-1"/>'):
            self.assertNotEqual(0, self.summarize(xml, 0, marker))


if __name__ == "__main__":
    unittest.main()
