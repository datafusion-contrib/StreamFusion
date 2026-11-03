import copy
import json
from pathlib import Path
import tempfile
import unittest

import execution_scope
from summarize import execution_contracts


class ExecutionScopeTest(unittest.TestCase):
    def test_jvm_aggregates_cover_the_complete_released_parameter_matrix(self):
        for method in ('testOverloadedAccumulator', 'testVarArgsNoGroupBy', 'testVarArgsWithGroupBy'):
            test = 'org.apache.flink.table.planner.runtime.stream.sql.AggregateITCase#' + method
            root = Path(__file__).parent
            for line, suffix in [('1.18', '-flink1.18'), ('2.2', '')]:
                contracts = execution_contracts(root / 'agent/src/main/resources' / ('native-execution' + suffix + '.tsv'))[test]
                if line == '1.18':
                    state_contracts = execution_contracts(root / 'agent/src/main/resources/native-execution-flink1.18-state.tsv')[test]
                    self.assertEqual(contracts, state_contracts)
                scope = json.loads((root / ('runtime-route-scope-' + line + '.json')).read_text())
                cases = {case['display_name']: case for case in scope['cases'] if case['test'] == test}
                configurations = [('OFF', 'OFF', backend, False) for backend in ['HEAP', 'ROCKSDB']]
                configurations += [(phase, 'ON', backend, False) for backend in ['HEAP', 'ROCKSDB'] for phase in ['OFF', 'ON']]
                if line == '2.2': configurations.append(('OFF', 'OFF', 'HEAP', True))
                self.assertEqual(len(configurations), len(cases))
                for phase, mini, backend, asynchronous in configurations:
                    display = f'LocalGlobal={phase}, MiniBatch={mini}, StateBackend={backend}'
                    if line == '2.2': display += f', EnableAsyncState={str(asynchronous).lower()}'
                    else: display = method + '[' + display + ']'
                    self.assertIn(display, cases)
                    for changelog in ([False, True] if line == '1.18' else [False]):
                        parameters = dict(aggMode=phase, miniBatch=str(mini == 'ON').lower(),
                            enableAsyncState=str(asynchronous).lower(), changelog=str(changelog).lower())
                        matched = [selector for selector in contracts if selector == '*' or all(
                            parameters.get(key) == value for key, value in (condition.split('=', 1) for condition in selector.split('&')))]
                        self.assertEqual(1, len(matched), (line, parameters, matched))
                        selector = matched[0]
                        expected = 'full_fallback' if changelog or asynchronous else 'native'
                        self.assertEqual(expected == 'full_fallback', contracts[selector].startswith('!'))
                        if expected == 'native':
                            operators = ('NativeColumnarJvmLocalGroupOperator+NativeColumnarJvmGlobalGroupOperator'
                                         if method != 'testOverloadedAccumulator' and phase == 'ON'
                                         else 'NativeColumnarJvmGroupOperator')
                            self.assertEqual(operators, contracts[selector])
                        declared = cases[display]
                        self.assertEqual(expected, declared.get('route', declared.get('route_by_contract_variant', {}).get(selector)))

    def test_jvm_over_preserves_both_backend_routes_and_requires_its_own_work(self):
        root = Path(__file__).parent
        prefix = 'org.apache.flink.table.planner.runtime.stream.table.OverAggregateITCase#'
        for line, suffix in [('1.18', '-flink1.18'), ('2.2', '')]:
            contracts = execution_contracts(root / 'agent/src/main/resources' / ('native-execution' + suffix + '.tsv'))
            scope = json.loads((root / ('runtime-route-scope-' + line + '.json')).read_text())
            tests = {test: variants for test, variants in contracts.items() if test.startswith(prefix)}
            self.assertEqual(11, len(tests))
            for test, variants in tests.items():
                cases = [case for case in scope['cases'] if case['test'] == test]
                self.assertEqual(2, len(cases))
                for case in cases:
                    for selector, required in variants.items():
                        builtin_only = 'DistinctPartitioned' in test
                        legacy_array = line == '1.18' and test.endswith('#testRowTimeUnBoundedPartitionedRangeOver')
                        fallback = builtin_only or legacy_array or selector == 'changelog=true'
                        self.assertEqual(fallback, required.startswith('!'))
                        if not fallback:
                            self.assertEqual('NativeColumnarJvmOverOperator', required)
                        elif builtin_only:
                            self.assertEqual('!OVER: expected COUNT(*) or one supported argument per aggregate; direct AVG requires DECIMAL', required)
                        elif legacy_array:
                            self.assertEqual('!collection value representation requires a final projection or fused consumer', required)
                        declared = case.get('route', case.get('route_by_contract_variant', {}).get(selector))
                        self.assertEqual('full_fallback' if fallback else 'native', declared)
            if line == '1.18':
                state = execution_contracts(root / 'agent/src/main/resources/native-execution-flink1.18-state.tsv')
                for test, variants in tests.items():
                    self.assertEqual(variants, state[test])

    def test_scalar_sql_over_requires_each_unbounded_event_time_implementation(self):
        root = Path(__file__).parent
        prefix = 'org.apache.flink.table.planner.runtime.stream.sql.OverAggregateITCase#'
        for line, suffix in [('1.18', '-flink1.18'), ('2.2', '')]:
            contracts = execution_contracts(root / 'agent/src/main/resources' / ('native-execution' + suffix + '.tsv'))
            scope = json.loads((root / ('runtime-route-scope-' + line + '.json')).read_text())
            for method in ['testRowTimeUnBoundedPartitionedRangeOver', 'testRowTimeUnBoundedPartitionedRowsOver']:
                test = prefix + method
                cases = [case for case in scope['cases'] if case['test'] == test]
                self.assertEqual(4 if line == '2.2' else 2, len(cases))
                for selector, required in contracts[test].items():
                    fallback = selector == 'changelog=true'
                    self.assertEqual(fallback, required.startswith('!'))
                    if not fallback:
                        self.assertEqual('NativeColumnarJvmOverOperator', required)
                    for case in cases:
                        declared = case.get('route', case.get('route_by_contract_variant', {}).get(selector))
                        self.assertEqual('full_fallback' if fallback else 'native', declared)

    def test_pinned_vintage_fixture_names_use_the_selected_method(self):
        scope = json.loads(Path(__file__).with_name('runtime-route-scope-1.18.json').read_text())
        for case in scope['cases']:
            name = case['display_name']
            if name.startswith('test') and '[' in name:
                with self.subTest(test=case['test'], display_name=name):
                    self.assertEqual(case['test'].split('#')[1], name.split('[', 1)[0])

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
