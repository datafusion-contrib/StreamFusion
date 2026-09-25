#!/usr/bin/env python3
"""Join upstream JUnit outcomes to per-invocation planner observations and classify SQL coverage."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

MARKER = re.compile(r"StreamFusion SQL inventory: ([0-9a-f-]{36})")
CONNECTOR_SUITES = {'formats', 'parquet', 'orc', 'kafka', 'paimon', 'delta'}
BOUNDARIES = {"Sink", "LegacySink", "TableSourceScan", "LegacyTableSourceScan", "DataStreamScan", "Values", "IntermediateTableScan", "PreparedLegacySink"}


def category(reason: str) -> str:
    text = reason.lower()
    if 'state backend:' in text:
        return 'state-backend'
    if 'python' in text:
        return 'python-runtime'
    if 'incrementalgroupaggregate' in text:
        return 'distinct-split-non-goal'
    if any(x in text for x in ('json', 'jackson')):
        return 'sql-json'
    if any(x in text for x in ('unsupported function', 'unsupported cast', 'calc:', 'filter:', 'expression')):
        return 'scalar-expression'
    if any(x in text for x in ('top-n', 'rank', 'dedup', 'limit:')):
        return 'rank-and-deduplication'
    if any(x in text for x in ('mlpredict', 'vectorsearch', 'asynccalc')):
        return 'async-and-model-functions'
    if 'watermark' in text:
        return 'watermark'
    if 'no native substitution' in text and 'sort' in text:
        return 'ordering'
    if 'window' in text:
        return 'window'
    if any(x in text for x in ('lookup', 'temporal join', 'join')):
        return 'join'
    if any(x in text for x in ('aggregate', 'aggregation', 'group by:')):
        return 'aggregation'
    if any(x in text for x in ('unnest', 'correlate')):
        return 'table-function'
    if any(x in text for x in ('match', 'pattern')):
        return 'pattern-matching'
    if any(x in text for x in ('type', 'arrow', 'column')):
        return 'data-type'
    if any(x in text for x in ('source', 'sink', 'connector', 'format')):
        return 'connector-boundary'
    if any(x in text for x in ('changelog', 'insert-only', 'retract', 'update')):
        return 'changelog'
    return 'operator-admission'


def execution_plans(record: dict) -> list[dict]:
    plans = []
    for event in record['plans']:
        if not event['during_translation']:
            continue
        roots = event.get('roots')
        if roots:
            for root in roots:
                plans.append(dict(event, operators=root['operators'], substitutions=root['native_operators'], host_boundaries=root.get('host_boundaries', [])))
        else:
            plans.append(event)
    return plans


def features(record: dict) -> list[str]:
    operators = {op for plan in record['plans'] for op in plan['operators']}
    operators.update(op for plan in record['plans'] for root in plan.get('roots', []) for op in root['operators'])
    families = []
    for family, patterns in (
        ('connector-boundary', ('NativeFileSink', 'NativeKafka', 'NativePaimon', 'NativeDelta')),
        ('async-and-model-functions', ('AsyncCalc', 'MLPredict', 'VectorSearch')),
        ('pattern-matching', ('Match',)),
        ('table-function', ('Correlate', 'Unnest', 'TableAggregate')),
        ('window', ('Window',)),
        ('over-aggregation', ('OverAggregate',)),
        ('join', ('Join',)),
        ('rank-and-deduplication', ('Rank', 'Deduplicate', 'SortLimit', 'Limit')),
        ('aggregation', ('Aggregate',)),
        ('scalar-expression', ('Calc', 'Filter')),
        ('set-operation', ('Union', 'Expand')),
        ('ordering', ('Sort',)),
        ('changelog', ('Changelog', 'DropUpdate')),
        ('watermark', ('Watermark',)),
    ):
        if any(pattern in operator for pattern in patterns for operator in operators):
            families.append(family)
    return families or ['source-or-constant-only']


def non_execution(record: dict) -> tuple[str, str, str]:
    identity = record['junit_id']
    def method_is(*names):
        return any(':' + name + delimiter in identity for name in names for delimiter in ('(', '%5B'))

    # These branches are taken only without a translated/optimized plan. The release
    # fixtures explicitly return early, check schemas, or assert API validation errors.
    early_return = (
        ('TableEnvironmentITCase', ('testExecuteInsertOverwrite', 'testExecuteSqlAndToDataStream',
                                   'testExecuteSqlWithInsertOverwrite', 'testFromToDataStreamAndExecuteSql',
                                   'testStatementSetWithOverwrite', 'testStatementSetWithSameSinkTableNames',
                                   'testToDataStreamAndExecuteSql')),
        ('GroupWindowITCase', ('testEventTimeSessionWindow', 'testEventTimeTumblingWindowWithAllowLateness',
                               'testDistinctAggWithMergeOnEventTimeSessionGroupWindow')),
        ('LookupJoinITCase', ('testLookupCacheSharingAcrossSubtasks',)),
        ('AsyncLookupJoinITCase', ('testLookupCacheSharingAcrossSubtasks',)),
        ('CatalogTableITCase', ('testInsertWithAggregateSource',)),
    )
    for cls, methods in early_return:
        if f'.{cls}]' in identity and method_is(*methods):
            return 'not accelerated', 'upstream-early-return', 'Upstream returns before query execution for this parameter variant; JUnit reports a pass.'
    if '.runtime.batch.' in identity or (record['planners'] and all(p['mode'] == 'BATCH' for p in record['planners'])):
        return 'not accelerated', 'batch', 'Batch-only fixture; no execution translation observed for this variant.'
    if '.planner.functions.' in identity:
        return 'not accelerated', 'api-validation', 'Built-in function fixture stops at Table API validation before execution translation.'
    if method_is('testNonStaticClassScalarFunction', 'testLeftOuterJoinWithPredicates', 'testLateralJoinWithScalarFunction'):
        return 'not accelerated', 'api-validation', 'Upstream asserts a Table API validation error before execution translation.'
    if method_is('testProctimeCascadeWindowAgg', 'testUnnestWithOrdinalityAliasColumnNames'):
        return 'not accelerated', 'schema-only', 'Upstream checks the resolved schema without executing the query.'
    if method_is('testFromAndToDataStreamBypassConversion'):
        return 'not accelerated', 'datastream-bypass', 'Upstream verifies that a DataStream round trip bypasses SQL planning; no SQL computation.'
    if method_is('testTableConfigInheritsEnvironmentSettings'):
        return 'not accelerated', 'configuration-only', 'Checks inherited TableEnvironment configuration; no query execution.'
    if method_is('testGetTablesFromGivenCatalogDatabase'):
        return 'not accelerated', 'catalog-or-metadata', 'Checks catalog table listings directly; no query execution.'
    statements = [s['statement'].lstrip().upper() for s in record['sql']]
    if statements and all(re.match(r'(CREATE|DROP|ALTER|SHOW|DESCRIBE|USE|EXPLAIN)\b', s) for s in statements):
        return 'not accelerated', 'catalog-or-metadata', 'Only DDL, metadata or explain statements observed; no execution translation.'
    if statements and any(s.startswith('CALL ') for s in statements):
        return 'not accelerated', 'procedure-call', 'Calls a host procedure; no relational SQL execution plan observed.'
    return 'not accelerated', 'no-execution-plan', 'No execution translation observed; catalog/API/validation or directly evaluated fixture. See SQL and invocation evidence.'


def classify(record: dict, outcome: str) -> tuple[str, str, str]:
    if outcome == 'skipped':
        return 'not accelerated', 'upstream-skip', 'Upstream skipped this invocation; no execution.'
    if outcome != 'passed':
        return 'not accelerated', 'test-failure', 'Test did not pass; do not credit acceleration coverage.'
    translations = record['translations']
    translation_failures = [e for e in record['operation_failures'] if e['operation'] == 'translate']
    if translations and len(translation_failures) == len(translations):
        return 'not accelerated', 'validation-or-host-error', 'Every execution translation failed as expected by this passing fixture: ' + translation_failures[0]['error']
    plans = execution_plans(record)
    native = [p for p in plans if p['substitutions'] > 0]
    host = [p for p in plans if p['substitutions'] == 0]
    gaps = []
    boundary = 0
    for plan in host:
        operators = {re.sub(r'^StreamPhysical', '', op) for op in plan['operators']}
        if operators and operators <= BOUNDARIES and not plan['fallback_reasons']:
            boundary += 1
        else:
            gaps.extend(plan['fallback_reasons'] or ['No native substitution for: ' + ', '.join(sorted(operators))])
    reasons = list(dict.fromkeys(gaps))
    if reasons:
        categories = sorted({category(r) for r in reasons})
        label = 'not accelerated' if all(c in ('python-runtime', 'distinct-split-non-goal') for c in categories) else 'should be accelerated'
        note = '; '.join(reasons)
        if native:
            note = f'{len(native)} native and {len(host)} host plans in this invocation. ' + note
        return label, ', '.join(categories), note
    if native:
        return 'accelerated', features(record)[0], f'{len(native)} execution plan(s) admitted native substitution ({", ".join(features(record))}); upstream assertions passed.' + (f' {boundary} additional source/constant-only plan(s).' if boundary else '')
    if translations and all('BatchPlanner' in p for p in translations):
        return 'not accelerated', 'batch', 'Executed with Flink BatchPlanner; StreamFusion targets streaming SQL.'
    if any(p['unmodified'] for p in record['planners']):
        return 'not accelerated', 'stock-plan-contract', 'The harness intentionally preserves stock Flink for compiled-plan/operator-name assertions.'
    if boundary:
        return 'not accelerated', 'source-or-constant-only', 'Translated plan contains only source/sink/constant boundaries; no interior computation to accelerate.'
    if record['operation_failures'] and not plans:
        return 'not accelerated', 'validation-or-host-error', 'No native execution plan; upstream negative/validation fixture observed: ' + record['operation_failures'][0]['error']
    if record['plans']:
        return 'not accelerated', 'plan-only', 'Native planning was observed outside execution translation (for example EXPLAIN); no execution credited.'
    if not translations:
        return non_execution(record)
    return 'should be accelerated', 'unobserved-streaming-plan', 'Streaming translation occurred without an observed StreamFusion admission decision; investigate harness or planner coverage.'


def classify_connectors(record: dict, result: tuple[str, str, str]) -> tuple[str, str, str]:
    label, group, note = result
    if label == 'not accelerated' and group != 'source-or-constant-only':
        return result
    retained = []
    for plan in execution_plans(record):
        for boundary in plan.get('host_boundaries', []):
            connector = boundary.get('connector', '')
            implementation = boundary['implementation']
            if not connector and implementation.startswith('org.apache.paimon.'):
                connector = 'paimon'
            if connector not in ('filesystem', 'kafka', 'upsert-kafka', 'paimon', 'delta'):
                continue
            format_name = boundary.get('format', boundary.get('value.format', boundary.get('file.format', '')))
            if format_name.startswith('test'):
                continue
            role = 'source' if boundary['operator'].endswith('Scan') else 'sink'
            retained.append(f'{connector}{"/" + format_name if format_name else ""} {role} ({implementation.rsplit(".", 1)[-1]})')
    if not retained:
        return result
    groups = set(group.split(', ')) if label == 'should be accelerated' else set()
    groups.add('connector-boundary')
    detail = '; '.join(dict.fromkeys(retained))
    return 'should be accelerated', ', '.join(sorted(groups)), note + ' Retained host connector boundary: ' + detail + '. Native admission of SQL computation does not cover this source/sink path.'


def collect(reports: Path, evidence: Path, line: str, suite_name: str = 'runtime') -> list[dict]:
    records = {}
    for path in sorted(evidence.glob('*.json')):
        record = json.loads(path.read_text())
        if record.get('schema_version') != 1 or record.get('flink_line') != line:
            raise ValueError(f'Wrong schema or Flink line: {path}')
        key = record['invocation_id']
        if key in records or path.stem != key:
            raise ValueError(f'Duplicate/misnamed invocation: {path}')
        records[key] = record
    used = set()
    rows = []
    for path in sorted(reports.rglob('TEST-*.xml')):
        suite = ET.parse(path).getroot()
        cases = suite.findall('testcase')
        declared = [int(suite.get(key, '0')) for key in ('tests', 'failures', 'errors', 'skipped')]
        observed = [len(cases)] + [sum(case.find(key) is not None for case in cases)
                                  for key in ('failure', 'error', 'skipped')]
        if suite.tag != 'testsuite' or declared != observed:
            raise ValueError(f'Incomplete or inconsistent JUnit report: {path}')
        for index, case in enumerate(cases):
            classname = case.get('classname', suite.get('name', ''))
            if '.' not in classname and suite.get('name', '').endswith('.' + classname):
                classname = suite.get('name')
            outcome = next((tag for tag in ('skipped', 'failure', 'error') if case.find(tag) is not None), 'passed')
            output = '\n'.join(child.text or '' for child in case if child.tag in ('system-out', 'system-err'))
            ids = set(MARKER.findall(output))
            if outcome == 'skipped' and not ids:
                record = {'invocation_id': '', 'junit_id': '', 'display_name': case.get('name', ''), 'plans': [], 'planners': [], 'translations': [], 'sql': [], 'operation_failures': []}
            else:
                if len(ids) != 1:
                    raise ValueError(f'{path.name} case {index}: expected one inventory identity, got {ids}')
                key = ids.pop()
                if key in used or key not in records:
                    raise ValueError(f'{path.name} case {index}: duplicate or missing evidence {key}')
                used.add(key)
                record = records[key]
            result = classify(record, outcome)
            sql_label = result[0]
            if suite_name in CONNECTOR_SUITES:
                result = classify_connectors(record, result)
            label, group, note = result
            rows.append({
                'flink_line': line, 'suite': suite_name, 'test_class': classname, 'test_name': case.get('name', ''),
                'display_name': record['display_name'], 'outcome': outcome, 'label': label, 'sql_label': sql_label,
                'category': group, 'features': ', '.join(features(record)) if record['plans'] else '', 'note': note, 'invocation_id': record['invocation_id'],
                'junit_id': record['junit_id'], 'report': path.relative_to(reports).as_posix(), 'case_index': index,
                'native_plans': sum(p['substitutions'] > 0 for p in execution_plans(record)),
                'host_plans': sum(p['substitutions'] == 0 for p in execution_plans(record)),
                'native_components': ', '.join(sorted({op.removeprefix('StreamPhysicalNative')
                    for plan in execution_plans(record) for op in plan['operators']
                    if op.startswith('StreamPhysicalNative')})),
                'sql': record['sql'], 'plans': record['plans'], 'operation_failures': record['operation_failures'],
                'planners': record['planners'], 'translations': record['translations'],
                'junit_source': record.get('source', ''), 'junit_status': record.get('junit_status', ''),
            })
    if not rows:
        raise ValueError('No upstream JUnit cases found')
    if set(records) != used:
        raise ValueError(f'{len(set(records) - used)} unmatched/stale invocation records')
    return rows


def write(rows: list[dict], output: Path, revision: str) -> None:
    output.mkdir(parents=True, exist_ok=True)
    summary = {'revision': revision, 'cases': len(rows), 'outcomes': dict(Counter(r['outcome'] for r in rows)),
               'labels_passed': dict(Counter(r['label'] for r in rows if r['outcome'] == 'passed')),
               'categories_passed': dict(Counter(r['category'] for r in rows if r['outcome'] == 'passed'))}
    (output / 'inventory.json').write_text(json.dumps({'schema_version': 1, 'summary': summary, 'tests': rows}, indent=2) + '\n')
    fields = [key for key in rows[0] if key not in ('sql', 'plans', 'operation_failures', 'planners', 'translations')]
    with (output / 'inventory.csv').open('w', newline='', encoding='utf-8', errors='backslashreplace') as stream:
        writer = csv.DictWriter(stream, fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reports', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--line', choices=['1.18', '2.2'], required=True)
    parser.add_argument('--suite', default='runtime', choices=['runtime', 'diagnostic', 'state', 'formats', 'parquet', 'orc', 'kafka', 'paimon', 'delta'])
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    write(collect(args.reports, args.evidence, args.line, args.suite), args.output, args.revision)


if __name__ == '__main__':
    main()
