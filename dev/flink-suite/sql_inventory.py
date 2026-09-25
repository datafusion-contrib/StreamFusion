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
BOUNDARIES = {"Sink", "LegacySink", "TableSourceScan", "LegacyTableSourceScan", "DataStreamScan", "Values", "IntermediateTableScan"}


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
    if any(x in text for x in ('top-n', 'rank', 'dedup')):
        return 'rank-and-deduplication'
    if 'window' in text:
        return 'window'
    if any(x in text for x in ('lookup', 'temporal join', 'join')):
        return 'join'
    if any(x in text for x in ('aggregate', 'aggregation')):
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
                plans.append(dict(event, operators=root['operators'], substitutions=root['native_operators']))
        else:
            plans.append(event)
    return plans


def classify(record: dict, outcome: str) -> tuple[str, str, str]:
    if outcome == 'skipped':
        return 'not accelerated', 'upstream-skip', 'Upstream skipped this invocation; no execution.'
    if outcome != 'passed':
        return 'not accelerated', 'test-failure', 'Test did not pass; do not credit acceleration coverage.'
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
        return 'accelerated', 'native-plan', f'{len(native)} execution plan(s) admitted native substitution; upstream assertions passed.' + (f' {boundary} additional source/constant-only plan(s).' if boundary else '')
    translations = record['translations']
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
        return 'not accelerated', 'no-execution-plan', 'No execution translation observed; catalog/API/validation or directly evaluated fixture. See SQL and invocation evidence.'
    return 'should be accelerated', 'unobserved-streaming-plan', 'Streaming translation occurred without an observed StreamFusion admission decision; investigate harness or planner coverage.'


def collect(reports: Path, evidence: Path, line: str) -> list[dict]:
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
        if len(cases) != int(suite.get('tests', '-1')):
            raise ValueError(f'Incomplete JUnit report: {path}')
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
            label, group, note = classify(record, outcome)
            rows.append({
                'flink_line': line, 'test_class': classname, 'test_name': case.get('name', ''),
                'display_name': record['display_name'], 'outcome': outcome, 'label': label,
                'category': group, 'note': note, 'invocation_id': record['invocation_id'],
                'junit_id': record['junit_id'], 'report': path.name, 'case_index': index,
                'native_plans': sum(p['substitutions'] > 0 for p in execution_plans(record)),
                'host_plans': sum(p['substitutions'] == 0 for p in execution_plans(record)),
                'sql': record['sql'], 'plans': record['plans'], 'operation_failures': record['operation_failures'],
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
    fields = [key for key in rows[0] if key not in ('sql', 'plans', 'operation_failures')]
    with (output / 'inventory.csv').open('w', newline='') as stream:
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
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    write(collect(args.reports, args.evidence, args.line), args.output, args.revision)


if __name__ == '__main__':
    main()
