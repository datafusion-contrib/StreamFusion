import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location('sql_inventory', Path(__file__).with_name('sql_inventory.py'))
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)


def record(plans=(), translations=()):
    return dict(schema_version=1, flink_line='2.2', invocation_id='12345678-1234-1234-1234-123456789012',
                junit_id='unique-parameterized-invocation', display_name='test[backend=heap]',
                plans=list(plans), translations=list(translations), sql=[], planners=[], operation_failures=[])


def plan(substitutions, reasons=(), translated=True, operators=('StreamPhysicalCalc',)):
    return dict(substitutions=substitutions, fallback_reasons=list(reasons), during_translation=translated, operators=list(operators))


class SqlInventoryTest(unittest.TestCase):
    def test_execution_and_explain_are_distinct(self):
        self.assertEqual('accelerated', inventory.classify(record([plan(1)]), 'passed')[0])
        self.assertEqual('plan-only', inventory.classify(record([plan(1, translated=False)]), 'passed')[1])

    def test_multiple_queries_keep_the_remaining_gap(self):
        result = inventory.classify(record([plan(1), plan(0, ['Calc: unsupported function/operator: AS'])]), 'passed')
        self.assertEqual('should be accelerated', result[0])
        self.assertIn('1 native and 1 host', result[2])

    def test_source_only_and_batch_are_not_missing_native_operators(self):
        self.assertEqual('source-or-constant-only', inventory.classify(record([plan(0, operators=('StreamPhysicalSink', 'StreamPhysicalValues'))]), 'passed')[1])
        self.assertEqual('batch', inventory.classify(record(translations=['org.apache.flink.table.planner.delegation.BatchPlanner']), 'passed')[1])

    def test_deliberate_exclusion_does_not_hide_an_unrelated_gap(self):
        intentional = 'unsupported IncrementalGroupAggregate'
        self.assertEqual('not accelerated', inventory.classify(record([plan(0, [intentional])]), 'passed')[0])
        self.assertEqual('should be accelerated', inventory.classify(record([plan(0, [intentional, 'Calc: unsupported function/operator: AS'])]), 'passed')[0])

    def test_failure_never_earns_acceleration_credit(self):
        self.assertEqual('test-failure', inventory.classify(record([plan(1)]), 'failure')[1])

    def fixture(self, root, observation, marker=True):
        reports, evidence = root / 'reports', root / 'evidence'
        reports.mkdir(); evidence.mkdir()
        suite = ET.Element('testsuite', name='org.apache.flink.CalcITCase', tests='1')
        case = ET.SubElement(suite, 'testcase', classname='CalcITCase', name='test')
        if marker:
            ET.SubElement(case, 'system-out').text = 'StreamFusion SQL inventory: ' + observation['invocation_id']
        ET.ElementTree(suite).write(reports / 'TEST-case.xml')
        (evidence / (observation['invocation_id'] + '.json')).write_text(json.dumps(observation))
        return reports, evidence

    def test_exact_invocation_evidence_is_required(self):
        with tempfile.TemporaryDirectory() as directory:
            reports, evidence = self.fixture(Path(directory), record([plan(1)]))
            rows = inventory.collect(reports, evidence, '2.2')
            self.assertEqual('org.apache.flink.CalcITCase', rows[0]['test_class'])
            self.assertEqual('accelerated', rows[0]['label'])
            next(evidence.iterdir()).unlink()
            with self.assertRaisesRegex(ValueError, 'missing evidence'):
                inventory.collect(reports, evidence, '2.2')

    def test_missing_agent_and_stale_evidence_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            reports, evidence = self.fixture(Path(directory), record(), marker=False)
            with self.assertRaisesRegex(ValueError, 'expected one inventory identity'):
                inventory.collect(reports, evidence, '2.2')
        with tempfile.TemporaryDirectory() as directory:
            reports, evidence = self.fixture(Path(directory), record())
            extra = record(); extra['invocation_id'] = '22345678-1234-1234-1234-123456789012'
            (evidence / (extra['invocation_id'] + '.json')).write_text(json.dumps(extra))
            with self.assertRaisesRegex(ValueError, 'unmatched/stale'):
                inventory.collect(reports, evidence, '2.2')
