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
    FALLBACK = "org.apache.flink.table.planner.runtime.stream.sql.RankITCase#testTopNWithVariableTopSize"
    FALLBACK_REASON = "Top-N: nullable variable rank bounds require Flink's row-access semantics"

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

    def test_required_method_includes_parameters_but_not_skips_or_similar_names(self):
        full_class = "org.apache.paimon.flink.ReadWriteTableITCase"
        arguments = ["summarize.py", str(self.root), "--require-test-method", full_class + "#testRows"]
        for name, skipped, expected in (
            ("testRows", False, 0), ("testRows(boolean)[1]", False, 0),
            ("testRows[HEAP]", False, 0), ("testRows(boolean)[1]", True, 1),
            ("testRowsOther", False, 1),
        ):
            with self.subTest(name=name, skipped=skipped):
                (self.root / "TEST-method.xml").write_text(
                    f'<testsuite tests="1" skipped="{int(skipped)}">'
                    f'<testcase classname="{full_class}" name="{name}">'
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

    def test_shard_requires_every_contract_in_its_selected_classes(self):
        classes = self.root / "classes.txt"
        classes.write_text(self.CALC.split("#")[0] + "\n")
        contracts = self.root / "contracts.txt"
        contracts.write_text(
            f"{self.CALC}\t*\tNativeCalcOperator\n"
            f"{self.WINDOW}\tsplitDistinct=false\tNativeColumnarWindowAggregateOperator\n"
        )
        full_class, method = self.CALC.split("#")
        report = self.root / "TEST-calc.xml"
        report.write_text(
            f'<testsuite tests="1"><testcase classname="{full_class}" name="{method}"/></testsuite>'
        )
        self.record(self.CALC, "NativeCalcOperator=3")
        arguments = ["summarize.py", str(self.root), "--contracts", str(contracts),
                     "--native-reports", str(self.root), "--require-contract-classes", str(classes)]
        with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
            self.assertEqual(0, summarize.main())
        classes.write_text(classes.read_text() + self.WINDOW.split("#")[0] + "\n")
        with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
            self.assertEqual(1, summarize.main())

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
            f"{self.CALC}\t*\t!@MissingReason",
            f"{self.CALC}\t*\t!@:reason",
            f"{self.CALC}\t*\t!@NativeCalcOperator:",
        ):
            with self.subTest(content=content):
                path.write_text(content)
                with self.assertRaises(ValueError):
                    summarize.execution_contracts(path)

    def test_expected_fallback_is_counted_separately_and_requires_its_reason(self):
        self.record(
            self.FALLBACK,
            "",
            reasons=self.FALLBACK_REASON,
        )
        self.assertEqual((0, 1, []), self.check({self.FALLBACK: 1}))
        self.record(
            self.FALLBACK, "", reasons="different reason"
        )
        self.assertTrue(self.check({self.FALLBACK: 1})[2])
        self.record(
            self.FALLBACK,
            "NativeColumnarWindowAggregateOperator=1",
            reasons=self.FALLBACK_REASON,
        )
        self.assertTrue(self.check({self.FALLBACK: 1})[2])

    def test_family_fallback_allows_other_native_work_but_rejects_wrong_admission(self):
        self.contracts[self.FALLBACK] = {
            "*": f"!@NativeColumnarChangelogNormalizeOperator:{self.FALLBACK_REASON}"
        }
        self.record(self.FALLBACK, "NativeCalcOperator=3", reasons=self.FALLBACK_REASON)
        self.assertEqual((0, 1, []), self.check({self.FALLBACK: 1}))
        self.record(self.FALLBACK, "NativeColumnarChangelogNormalizeOperator=1",
                    reasons=self.FALLBACK_REASON)
        self.assertTrue(self.check({self.FALLBACK: 1})[2])
        self.record(self.FALLBACK, "NativeCalcOperator=3", reasons="different reason")
        self.assertTrue(self.check({self.FALLBACK: 1})[2])

    def test_split_distinct_requires_work_from_both_window_stages(self):
        for counts in ("", "NativeCalcOperator=3", "NativeColumnarWindowAggregateOperator=3",
                       "NativeColumnarLocalWindowAggregateOperator=3",
                       "NativeColumnarGlobalWindowAggregateOperator=2"):
            with self.subTest(counts=counts):
                self.record(self.WINDOW, counts, variant="splitDistinct=true")
                self.assertTrue(self.check({self.WINDOW: 1})[2])
        self.record(
            self.WINDOW,
            "NativeColumnarLocalWindowAggregateOperator=3,NativeColumnarGlobalWindowAggregateOperator=2",
            variant="splitDistinct=true",
        )
        self.assertEqual((1, 0, []), self.check({self.WINDOW: 1}))

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


class CanonicalRestoreSummaryTest(unittest.TestCase):
    TEST = "org.apache.flink.table.planner.plan.nodes.exec.stream.JoinRestoreTest#testRestore"
    FAMILY = "NativeColumnarUpdatingJoinOperator"

    def check(self, contract, counts="", imported=None, reasons="", stock=None):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            encoded = base64.b64encode(reasons.encode()).decode()
            suffix = "" if imported is None else "\t" + imported
            if stock is not None:
                suffix = "\t" + (imported or "") + "\t" + stock
            (root / "one.tsv").write_text(f"{self.TEST}\t*\t{counts}\t{encoded}{suffix}\n")
            observations = []
            result = summarize.check_execution(root, Counter({self.TEST: 1}),
                {self.TEST: {"*": contract}}, observations)
            return result, observations

    def test_mixed_sessions_require_native_work_and_real_stock_state(self):
        contract = "NativeColumnarGroupAggregateOperator+StockSessionTableFunction"
        for counts, stock in (("", "StockSessionTableFunction=3"),
                              ("NativeColumnarGroupAggregateOperator=3", ""),
                              ("NativeColumnarGroupAggregateOperator=3", "StockSessionTableFunction=0")):
            with self.subTest(counts=counts, stock=stock):
                self.assertTrue(self.check(contract, counts=counts, stock=stock)[0][2])
        result, observations = self.check(contract, counts="NativeColumnarGroupAggregateOperator=3",
                                           stock="StockSessionTableFunction=2")
        self.assertEqual((1, 0, []), result)
        self.assertEqual({"StockSessionTableFunction": 2}, observations[0]["stock_state_owner_rows"])
        self.assertNotIn("StockSessionTableFunction", observations[0]["native_input_rows"])
        self.assertEqual("mixed", observations[0]["observed_route"])

    def test_secondary_temporal_sort_requires_real_stock_owner_and_native_calc(self):
        absent = "NativeColumnarTemporalSortOperator"
        reason = "temporal sort: secondary order keys require the stock sorter"
        contract = "!@" + absent + ":" + reason + "&&NativeCalcOperator+StockTemporalSort"
        for fields in ({}, {"counts": "NativeCalcOperator=2"}, {"stock": "StockTemporalSort=2"},
                       {"counts": "NativeCalcOperator=2", "stock": "StockTemporalSort=2"},
                       {"counts": "NativeCalcOperator=2," + absent + "=1", "stock": "StockTemporalSort=2", "reasons": reason}):
            self.assertTrue(self.check(contract, **fields)[0][2])
        result, observations = self.check(contract, counts="NativeCalcOperator=2", stock="StockTemporalSort=2", reasons=reason)
        self.assertEqual((0, 1, []), result)
        self.assertEqual("mixed", observations[0]["observed_route"])

    def test_window_join_fallback_requires_the_real_stock_state_owner(self):
        reason = "window join: semi/anti retain the released stock state owner"
        contract = "!@NativeWindowJoinOperator:" + reason + "&&StockWindowJoin"
        for fields in ({}, {"counts": "NativeCalcOperator=2", "reasons": reason},
                       {"stock": "StockWindowJoin=2"},
                       {"stock": "StockWindowJoin=0", "reasons": reason},
                       {"stock": "StockWindowJoin=2", "counts": "NativeWindowJoinOperator=1", "reasons": reason},
                       {"stock": "StockWindowJoin=2", "imported": "NativeWindowJoinOperator=1", "reasons": reason}):
            self.assertTrue(self.check(contract, **fields)[0][2])
        result, observations = self.check(contract, stock="StockWindowJoin=2", reasons=reason)
        self.assertEqual((0, 1, []), result)
        self.assertEqual({"StockWindowJoin": 2}, observations[0]["stock_state_owner_rows"])
        self.assertNotIn("StockWindowJoin", observations[0]["native_input_rows"])

    def test_stock_work_cannot_be_counterfeited_as_native_rows(self):
        result, _ = self.check("NativeColumnarGroupAggregateOperator+StockSessionTableFunction",
            counts="NativeColumnarGroupAggregateOperator=3,StockSessionTableFunction=2")
        self.assertTrue(result[2])

    def test_session_fallback_requires_owner_calc_and_exact_reason(self):
        absent = "NativeColumnarSessionWindowAggregateOperator"
        reason = "window aggregate: retracting state contract is not bound"
        contract = "!@" + absent + ":" + reason + "&&NativeCalcOperator+StockSessionWindowAggregate"
        for counts, stock, reasons in (("", "StockSessionWindowAggregate=3", reason),
                                      ("NativeCalcOperator=2", "", reason),
                                      ("NativeCalcOperator=2", "StockSessionWindowAggregate=3", ""),
                                      ("NativeCalcOperator=2," + absent + "=1", "StockSessionWindowAggregate=3", reason)):
            with self.subTest(counts=counts, stock=stock, reasons=reasons):
                self.assertTrue(self.check(contract, counts=counts, stock=stock, reasons=reasons)[0][2])
        result, observations = self.check(contract, counts="NativeCalcOperator=2",
            stock="StockSessionWindowAggregate=3", reasons=reason)
        self.assertEqual((0, 1, []), result)
        self.assertEqual("mixed", observations[0]["observed_route"])

    def test_restore_only_proof_cannot_include_stock_input(self):
        result, _ = self.check("@restore:" + self.FAMILY, imported=self.FAMILY + "=3",
                               stock="StockSessionTableFunction=2")
        self.assertTrue(result[2])

    def test_restore_only_evidence_is_separate_from_input_rows(self):
        result, observations = self.check("@restore:" + self.FAMILY, imported=self.FAMILY + "=3")
        self.assertEqual((1, 0, []), result)
        self.assertEqual({}, observations[0]["native_input_rows"])
        self.assertEqual({self.FAMILY: 3}, observations[0]["native_canonical_import_rows"])
        self.assertEqual("native_restore", observations[0]["observed_route"])

    def test_empty_zero_or_another_family_cannot_prove_import(self):
        for imported in (None, "", self.FAMILY + "=0", "NativeCalcOperator=3"):
            with self.subTest(imported=imported):
                result, _ = self.check("@restore:" + self.FAMILY, imported=imported)
                self.assertTrue(result[2])

    def test_import_cannot_replace_consumed_input(self):
        result, _ = self.check(self.FAMILY, imported=self.FAMILY + "=3")
        self.assertTrue(result[2])

    def test_restore_only_contract_rejects_input_or_fallback(self):
        for counts, reasons in ((self.FAMILY + "=1", ""), ("", "join unsupported")):
            result, _ = self.check("@restore:" + self.FAMILY, counts=counts,
                imported=self.FAMILY + "=3", reasons=reasons)
            self.assertTrue(result[2])

    def test_import_does_not_satisfy_full_or_family_fallback(self):
        for contract in ("!join unsupported", "!@" + self.FAMILY + ":join unsupported"):
            result, _ = self.check(contract, imported=self.FAMILY + "=3", reasons="join unsupported")
            self.assertTrue(result[2])

    def test_invalid_import_counts_cannot_satisfy_restore(self):
        for imported in (self.FAMILY + "=-1", self.FAMILY + "=bad", self.FAMILY + "=3," + self.FAMILY + "=4"):
            result, _ = self.check("@restore:" + self.FAMILY, imported=imported)
            self.assertTrue(result[2])

    def test_recorded_import_does_not_weaken_an_input_contract(self):
        result, observations = self.check(self.FAMILY, counts=self.FAMILY + "=2", imported=self.FAMILY + "=3")
        self.assertEqual((1, 0, []), result)
        self.assertEqual("native", observations[0]["observed_route"])

class ElidedTemporalSortSummaryTest(unittest.TestCase):
    TEST = "org.apache.flink.table.planner.plan.nodes.exec.stream.TemporalSortRestoreTest#testRestore"
    VARIANT = "program=temporal_sort_proctime"
    CONTRACT = "@elided:TemporalSort"

    def evidence(self):
        return {"submitted_graphs": {"job": {"nodes": [
            {"id": 1, "operator": "SourceOperator", "inputs": [], "outputs": [2]},
            {"id": 2, "operator": "StreamSink", "inputs": [1], "outputs": []}]}},
            "native_opened": [], "native_imports": {}}

    def check(self, evidence=None, *, native="", imported="", stock="", reasons="", extra=True,
              test=None, variant=None, contract=None):
        test = test or self.TEST
        variant = variant or self.VARIANT
        contract = contract or self.CONTRACT
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reason = base64.b64encode(reasons.encode()).decode()
            fields = [test, variant, native, reason, imported, stock]
            if extra:
                import json
                fields.append(base64.b64encode(json.dumps(evidence).encode()).decode())
            (root / "case.tsv").write_text("\t".join(fields) + "\n")
            observations = []
            result = summarize.check_execution(root, Counter({test: 1}),
                {test: {variant: contract}}, observations)
            return result, observations

    def test_submitted_sort_free_graph_is_separate_from_native_and_fallback_credit(self):
        for native in ("", "NativeCalcOperator=3"):
            evidence = self.evidence()
            if native:
                evidence["native_opened"] = ["NativeCalcOperator"]
            result, observations = self.check(evidence, native=native)
            self.assertEqual((0, 0, []), result)
            self.assertEqual("ordering_elided", observations[0]["observed_route"])
            self.assertTrue(observations[0]["contract_satisfied"])

    def test_empty_or_malformed_graphs_cannot_prove_elision(self):
        import copy
        invalid = [None, {}, {"submitted_graphs": {}}, self.evidence() | {"extra": 1}]
        for mode in ("no_jobs", "single_node", "unknown_edge", "duplicate_node", "duplicate_edge", "unconnected_cycle",
                     "stock_sort", "native_sort", "missing_factory", "native_open", "native_import"):
            evidence = copy.deepcopy(self.evidence())
            graph = evidence["submitted_graphs"]["job"]
            if mode == "no_jobs": evidence["submitted_graphs"] = {}
            elif mode == "single_node": graph["nodes"] = graph["nodes"][:1]
            elif mode == "unknown_edge": graph["nodes"][0]["outputs"] = [999]
            elif mode == "duplicate_node": graph["nodes"].append(graph["nodes"][0])
            elif mode == "duplicate_edge": graph["nodes"][0]["outputs"] = [2, 2]
            elif mode == "unconnected_cycle":
                graph["nodes"].extend([
                    {"id": 3, "operator": "Map", "inputs": [4], "outputs": [4]},
                    {"id": 4, "operator": "Map", "inputs": [3], "outputs": [3]}])
            elif mode == "stock_sort": graph["nodes"][0]["operator"] = "RowTimeSortOperator"
            elif mode == "native_sort": graph["nodes"][0]["operator"] = "NativeColumnarTemporalSortOperator"
            elif mode == "missing_factory": graph["nodes"][0]["operator"] = ""
            elif mode == "native_open": evidence["native_opened"] = ["NativeColumnarTemporalSortOperator"]
            elif mode == "native_import": evidence["native_imports"] = {"NativeColumnarTemporalSortOperator": 3}
            invalid.append(evidence)
        for evidence in invalid:
            with self.subTest(evidence=evidence):
                self.assertTrue(self.check(evidence)[0][2])
        self.assertTrue(self.check(extra=False)[0][2])

    def test_stateful_work_and_fallbacks_cannot_be_hidden_as_elision(self):
        for fields in ({"native": "NativeColumnarTemporalSortOperator=3"},
                       {"imported": "NativeColumnarTemporalSortOperator=3"},
                       {"stock": "StockSessionTableFunction=3"}, {"reasons": "sort unsupported"}):
            with self.subTest(fields=fields):
                self.assertTrue(self.check(self.evidence(), **fields)[0][2])

    def test_elision_does_not_apply_to_rowtime_unknown_programs_or_other_classes(self):
        for fields in ({"variant": "program=temporal_sort_rowtime"}, {"variant": "program=unknown"},
                       {"test": "org.example.Other#testRestore"}):
            self.assertTrue(self.check(self.evidence(), **fields)[0][2])
        self.assertTrue(self.check(self.evidence(), contract="NativeColumnarTemporalSortOperator")[0][2])

    def test_registry_restricts_elision_to_the_original_stateless_program(self):
        for test, variant, valid in ((self.TEST, self.VARIANT, True),
                                     (self.TEST, "program=temporal_sort_rowtime", False),
                                     (self.TEST, "*", False), ("org.example.Other#testRestore", self.VARIANT, False)):
            with tempfile.TemporaryDirectory() as temporary:
                file = Path(temporary) / "contracts.tsv"
                file.write_text(f"{test}\t{variant}\t{self.CONTRACT}\n")
                if valid:
                    self.assertEqual(self.CONTRACT, summarize.execution_contracts(file)[test][variant])
                else:
                    with self.assertRaises(ValueError): summarize.execution_contracts(file)


if __name__ == "__main__":
    unittest.main()
