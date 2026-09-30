import copy
import json
from pathlib import Path
import tempfile
import unittest

import execution_scope


class ExecutionScopeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'scope.json'
        self.requirement = dict(test='Fixture#query', display_name='backend=heap', count=1, route='native')
        self.scope = dict(schema_version=1, flink_line='2.2', cases=[self.requirement])
        self.case = dict(test='Fixture#query[heap]', display_name='backend=heap', runtime_route='native',
                         report='TEST-fixture.xml', case_index=0, invocation_id='exact-id')
        self.audit = dict(scope={}, testcases=[self.case])

    def check(self):
        self.path.write_text(json.dumps(self.scope))
        return execution_scope.validate(self.audit, self.path, '2.2')

    def test_selects_exact_variant_and_retains_its_identity(self):
        self.audit['testcases'].append(dict(test='Fixture#query[rocks]', display_name='backend=rocks',
                                             runtime_route='unclassified'))
        self.assertEqual([], self.check())
        selected = self.audit['scope']['runtime_route_scope']['selected_cases']
        self.assertEqual([self.case], selected)
        self.assertEqual(2, len(self.audit['testcases']))

    def test_route_can_follow_an_exact_joined_execution_contract_variant(self):
        self.requirement.pop('route')
        self.requirement['route_by_contract_variant'] = {
            'changelog=false': 'native', 'changelog=true': 'full_fallback'}
        with self.assertRaises(ValueError):
            self.check()
        self.scope['schema_version'] = 2
        for variant, route in self.requirement['route_by_contract_variant'].items():
            self.case.update(execution_contract_variant=variant, runtime_route=route)
            self.assertEqual([], self.check())
            self.case['runtime_route'] = 'mixed'
            self.assertTrue(self.check())
        for variant in (None, 'changelog=unknown'):
            self.case['execution_contract_variant'] = variant
            self.assertTrue(self.check())
        for invalid in ({}, {'': 'native'}, {'changelog=true': 'unclassified'}, []):
            self.requirement['route_by_contract_variant'] = invalid
            with self.assertRaises(ValueError):
                self.check()

    def test_missing_duplicate_and_unclassified_cases_fail(self):
        for cases in ([], [self.case, copy.deepcopy(self.case)],
                      [dict(self.case, runtime_route='unclassified')],
                      [dict(self.case, display_name='wrong variant')],
                      [dict(self.case, runtime_route='skipped')]):
            with self.subTest(cases=cases):
                self.audit['testcases'] = cases
                self.assertTrue(self.check())

    def test_malformed_or_wrong_line_scope_is_rejected(self):
        for scope in ([], {}, dict(self.scope, flink_line='1.18'), dict(self.scope, cases=[]),
                      dict(self.scope, cases=[self.requirement, self.requirement]),
                      dict(self.scope, cases=[dict(self.requirement, count=True)]),
                      dict(self.scope, cases=[dict(self.requirement, route='unclassified')])):
            with self.subTest(scope=scope):
                self.scope = scope
                with self.assertRaises(ValueError):
                    self.check()
