import importlib.util
import csv
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

    def test_expected_translation_errors_are_not_execution_coverage(self):
        observation = record([plan(1)], ['StreamPlanner'])
        observation['operation_failures'] = [dict(operation='translate', error='Expected validation error')]
        self.assertEqual('validation-or-host-error', inventory.classify(observation, 'passed')[1])

    def test_statement_set_roots_keep_a_host_gap(self):
        event = plan(2, ['Calc: unsupported function/operator: AS'])
        event['roots'] = [dict(operators=['StreamPhysicalNativeCalc'], native_operators=1),
                          dict(operators=['StreamPhysicalCalc'], native_operators=0)]
        result = inventory.classify(record([event]), 'passed')
        self.assertEqual('should be accelerated', result[0])
        self.assertIn('1 native and 1 host', result[2])

    def test_native_write_does_not_hide_a_host_connector_read(self):
        event = plan(1)
        event['roots'] = [dict(operators=['StreamPhysicalNativeFileSink', 'StreamPhysicalTableSourceScan'], native_operators=1,
                              host_boundaries=[dict(operator='StreamPhysicalTableSourceScan', implementation='org.apache.flink.table.planner.connectors.ExternalDynamicSource')]),
                          dict(operators=['StreamPhysicalTableSourceScan'], native_operators=0,
                              host_boundaries=[dict(operator='StreamPhysicalTableSourceScan', implementation='org.apache.flink.connector.file.table.FileSystemTableSource', connector='filesystem', format='parquet')])]
        observation = record([event])
        sql = inventory.classify(observation, 'passed')
        self.assertEqual('accelerated', sql[0])
        result = inventory.classify_connectors(observation, sql)
        self.assertEqual(('should be accelerated', 'connector-boundary'), result[:2])
        self.assertIn('filesystem/parquet source', result[2])
        self.assertNotIn('ExternalDynamicSource', result[2])
        self.assertEqual(('not accelerated', 'validation-or-host-error', 'expected'), inventory.classify_connectors(observation, ('not accelerated', 'validation-or-host-error', 'expected')))

    def test_parameter_variants_in_separate_report_directories_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = record([plan(1)])
            second = record([plan(0, ['unsupported function'])])
            second['invocation_id'] = '22345678-1234-1234-1234-123456789012'
            reports, evidence = self.fixture(root, first)
            nested = reports / 'flink-1.18'
            nested.mkdir()
            xml = (reports / 'TEST-case.xml').read_text().replace(first['invocation_id'], second['invocation_id'])
            (nested / 'TEST-case.xml').write_text(xml)
            (evidence / (second['invocation_id'] + '.json')).write_text(json.dumps(second))
            rows = inventory.collect(reports, evidence, '2.2', 'paimon')
            self.assertEqual({'TEST-case.xml', 'flink-1.18/TEST-case.xml'}, {r['report'] for r in rows})
            self.assertEqual({'paimon'}, {r['suite'] for r in rows})

    def test_non_execution_notes_do_not_override_executed_queries(self):
        observation = record()
        observation['junit_id'] = '[class:org.apache.flink.table.api.TableEnvironmentITCase]/[test-template:testExecuteInsertOverwrite()]/[test-template-invocation:#1]'
        self.assertEqual('upstream-early-return', inventory.classify(observation, 'passed')[1])
        observation['plans'] = [plan(1)]
        self.assertEqual('accelerated', inventory.classify(observation, 'passed')[0])

    def test_legacy_junit_parameter_names_receive_fixture_notes(self):
        observation = record()
        observation['junit_id'] = '[engine:junit-vintage]/[runner:org.apache.flink.table.planner.runtime.stream.sql.GroupWindowITCase]/[test:testProctimeCascadeWindowAgg%5BStateBackend=HEAP%5D(org.apache.flink.table.planner.runtime.stream.sql.GroupWindowITCase)]'
        self.assertEqual('schema-only', inventory.classify(observation, 'passed')[1])

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

    def test_duplicate_parameterized_names_keep_distinct_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            first = record([plan(1)])
            second = record([plan(0, ['state backend: not verified'])])
            second['invocation_id'] = '22345678-1234-1234-1234-123456789012'
            reports, evidence = self.fixture(Path(directory), first)
            path = reports / 'TEST-case.xml'
            tree = ET.parse(path)
            tree.getroot().set('tests', '2')
            case = ET.SubElement(tree.getroot(), 'testcase', classname='CalcITCase', name='test')
            ET.SubElement(case, 'system-out').text = 'StreamFusion SQL inventory: ' + second['invocation_id']
            tree.write(path)
            (evidence / (second['invocation_id'] + '.json')).write_text(json.dumps(second))
            rows = inventory.collect(reports, evidence, '2.2')
            self.assertEqual(['accelerated', 'should be accelerated'], [r['label'] for r in rows])
            self.assertEqual(2, len({r['invocation_id'] for r in rows}))

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

    def test_unicode_negative_fixtures_export_losslessly_in_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observation = record([plan(1)])
            observation['display_name'] = 'lone surrogate \ude00; valid emoji \U0001f600'
            reports, evidence = self.fixture(root, observation)
            rows = inventory.collect(reports, evidence, '2.2')
            inventory.write(rows, root / 'output', 'revision')
            exported = json.loads((root / 'output/inventory.json').read_text())
            self.assertEqual(observation['display_name'], exported['tests'][0]['display_name'])
            with (root / 'output/inventory.csv').open() as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual('lone surrogate \\ude00; valid emoji \U0001f600', row['display_name'])

    def test_incomplete_xml_counts_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            reports, evidence = self.fixture(Path(directory), record())
            path = reports / 'TEST-case.xml'
            tree = ET.parse(path)
            tree.getroot().set('tests', '2')
            tree.write(path)
            with self.assertRaisesRegex(ValueError, 'Incomplete or inconsistent'):
                inventory.collect(reports, evidence, '2.2')
