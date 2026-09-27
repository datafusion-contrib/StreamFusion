"""Join validated execution witnesses through the inventory's exact JUnit identities."""
from pathlib import Path
import re

import sql_inventory


def attach(audit: dict, reports: Path, inventory: Path, line: str) -> None:
    rows = sql_inventory.collect(reports, inventory, line)
    cases = {(case['report'], case['case_index']): case for case in audit['testcases']}
    witnesses = {record['evidence_file']: record for record in audit['execution_evidence']}
    used = set()
    bindings = []
    if len(rows) != len(cases):
        raise ValueError('Inventory and execution audit have different testcase denominators')
    for row in rows:
        case = cases.get((row['report'], row['case_index']))
        if case is None or case['test'] != row['test_class'] + '#' + row['test_name']:
            raise ValueError('Inventory testcase identity does not match execution audit')
        work = row['native_work']
        links = row['execution_contracts']
        if row['outcome'] == 'skipped' and not row['invocation_id']:
            work, links = {}, []
        if not isinstance(work, dict) or any(
                not isinstance(name, str) or not re.fullmatch(r'\w+', name)
                or type(count) is not int or count <= 0 for name, count in work.items()):
            raise ValueError(f"{case['test']}: missing or invalid native-work observations")
        if not isinstance(links, list):
            raise ValueError(f"{case['test']}: missing execution-contract links")
        required = int(case['contracted'] and case['outcome'] != 'skipped')
        if len(links) != required:
            raise ValueError(f"{case['test']}: expected {required} exact contract links, got {len(links)}")
        bound = []
        for link in links:
            if not isinstance(link, dict) or not isinstance(link.get('record_id'), str):
                raise ValueError('Invalid contract witness link')
            filename = link['record_id'] + '.tsv'
            witness = witnesses.get(filename)
            if filename in used or witness is None:
                raise ValueError(f'Duplicate or missing linked witness: {filename}')
            if link.get('test') != case['contract_test'] or any(
                    link.get(key) != witness[key] for key in ('test', 'variant')):
                raise ValueError(f'Witness test or fixture variant mismatch: {filename}')
            if any(count > work.get(operator, 0)
                   for operator, count in witness['native_input_rows'].items()):
                raise ValueError(f'Witness exceeds invocation work: {filename}')
            used.add(filename)
            bound.append(witness)
        bindings.append((case, row, work, bound))
    if used != set(witnesses):
        raise ValueError(f'{len(set(witnesses) - used)} execution witnesses lack exact invocation links')
    # Do not mark a partial join as exact when a later record fails validation.
    for case, row, work, bound in bindings:
        case.update(invocation_id=row['invocation_id'], junit_id=row['junit_id'],
                    native_input_rows=work)
        for witness in bound:
            witness.update(invocation_id=row['invocation_id'], report=row['report'],
                           case_index=row['case_index'])
    audit['scope']['evidence_granularity'] = 'exact_junit_invocation'
