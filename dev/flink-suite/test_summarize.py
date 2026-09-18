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

    def test_xfail_cannot_hide_missing_execution(self):
        xml = f'<testsuite tests="1" failures="1"><testcase classname="{self.CALC.split("#")[0]}" name="testLongProjectionList"><failure message="expected"/></testcase></testsuite>'
        (self.root / "TEST-calc.xml").write_text(xml)
        with patch.object(
            sys, "argv", ["summarize.py", str(self.root), "--xfail", self.CALC]
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())

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


if __name__ == "__main__":
    unittest.main()
