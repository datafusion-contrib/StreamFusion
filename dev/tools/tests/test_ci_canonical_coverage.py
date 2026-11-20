"""Reduced matrices must retain their contracts and execute every selected invocation."""

import copy
import hashlib
import itertools
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

from dev.tools.ci import canonical_coverage as coverage


NAME = "tech.streamfusion.CanonicalFixtureJobTest"


def inventory(name=NAME, method="handoff"):
    rows = list(map(list, itertools.product((False, True), repeat=3)))
    return dict(schema_version=1, line="2.2", jdk=17, full_cases=8, representative_cases=4,
                classes={name: dict(abstract=False, full=8, representative=4, methods={method:
                    dict(kind="parameterized", full=8, representative=4, full_arguments=rows,
                         selected_indices=[0, 3, 5, 6], recovery_axes=[])})})


class CanonicalCoverageTest(unittest.TestCase):
    def fixture(self, root, data=None):
        data = data or inventory()
        path = root / 'inventory.json'
        path.write_text(json.dumps(data))
        selection = dict(classes=[name.replace('.', '/') for name in data['classes']],
                         coverage='representative',
                         coverage_inventory_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        return data, path, selection

    def reports(self, root, count=4, name=NAME, method="handoff", outcome=None):
        reports = root / 'reports'
        reports.mkdir(exist_ok=True)
        suite = ET.Element('testsuite', tests=str(count))
        for index in range(1, count + 1):
            case = ET.SubElement(suite, 'testcase', classname=name,
                                 name=f'{method}(boolean, boolean, boolean)[{index}]')
            if outcome and index == 1:
                ET.SubElement(case, outcome[0], message=outcome[1])
        ET.ElementTree(suite).write(reports / 'TEST-fixture.xml')
        return reports

    def test_selected_invocations_are_required_and_wrong_inventory_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, path, selection = self.fixture(root)
            reports = self.reports(root)
            result = coverage.audit(selection, path, reports, 'representative')
            self.assertEqual(4, result['passed'])
            self.assertEqual(0, result['quarantined'])
            for count in (0, 3, 5):
                self.reports(root, count)
                with self.assertRaises(ValueError):
                    coverage.audit(selection, path, reports, 'representative')
            self.reports(root)
            selection['coverage_inventory_sha256'] = 'wrong'
            with self.assertRaisesRegex(ValueError, 'does not match'):
                coverage.audit(selection, path, reports, 'representative')

    def test_pairwise_and_required_three_way_interactions_are_checked_independently(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data, path, _ = self.fixture(root)
            self.assertEqual(data, coverage.load_inventory(path))
            for change in ('pair', 'triple', 'duplicate', 'empty'):
                broken = copy.deepcopy(data)
                method = broken['classes'][NAME]['methods']['handoff']
                if change == 'pair':
                    method['selected_indices'] = [0, 1, 2, 3]
                elif change == 'triple':
                    method['recovery_axes'] = [0, 1, 2]
                elif change == 'duplicate':
                    method['selected_indices'] = [0, 0, 5, 6]
                else:
                    method['representative'] = 0
                path.write_text(json.dumps(broken))
                with self.subTest(change=change), self.assertRaises(ValueError):
                    coverage.load_inventory(path)

    def test_failures_errors_and_unexpected_skips_remain_fatal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, path, selection = self.fixture(root)
            for kind in ('failure', 'error', 'skipped'):
                reports = self.reports(root, outcome=(kind, 'unexpected'))
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    coverage.audit(selection, path, reports, 'representative')

    def test_full_mode_cannot_reuse_representative_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, path, selection = self.fixture(root)
            selection['coverage'] = 'full'
            reports = self.reports(root)
            with self.assertRaises(ValueError):
                coverage.audit(selection, path, reports, 'full')
            reports = self.reports(root, 8)
            self.assertEqual(8, coverage.audit(selection, path, reports, 'full')['passed'])

    def test_only_the_exact_known_kafka_provider_can_be_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = inventory(coverage.KAFKA_CLASS, coverage.KAFKA_METHOD)
            data.update(line='1.18', full_cases=1, representative_cases=1)
            fixture = data['classes'][coverage.KAFKA_CLASS]
            fixture.update(full=1, representative=1)
            method = fixture['methods'][coverage.KAFKA_METHOD]
            method.update(full=1, representative=1, selected_indices=[0],
                          full_arguments=[['task-failure', False, False, 'incrementing']])
            _, path, selection = self.fixture(root, data)
            reports = self.reports(root, 1, coverage.KAFKA_CLASS, coverage.KAFKA_METHOD,
                                   ('skipped', coverage.QUARANTINE))
            self.assertEqual(1, coverage.audit(selection, path, reports, 'representative')['quarantined'])
            for jdk, line, reason in ((11, '1.18', coverage.QUARANTINE),
                                      (17, '2.2', coverage.QUARANTINE), (17, '1.18', 'other')):
                data.update(jdk=jdk, line=line)
                _, path, selection = self.fixture(root, data)
                self.reports(root, 1, coverage.KAFKA_CLASS, coverage.KAFKA_METHOD, ('skipped', reason))
                with self.assertRaises(ValueError):
                    coverage.audit(selection, path, reports, 'representative')

    def test_scheduling_uses_selected_work_instead_of_full_matrix_costs(self):
        data = inventory()
        name = NAME.replace('.', '/')
        self.assertEqual(20, coverage.costs({name: 40}, data, 'representative')[name])
        self.assertEqual(40, coverage.costs({name: 40}, data, 'full')[name])


if __name__ == '__main__':
    unittest.main()
