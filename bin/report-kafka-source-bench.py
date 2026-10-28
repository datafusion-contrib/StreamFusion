#!/usr/bin/env python3
"""Audit a Kafka source benchmark archive and report every route's repeated measurements.

The archive holds `observations.jsonl` (one line per run written by KafkaSourceBenchmark),
`method.json` (the warmup and trial counts) and each route's saved plan. Every run must time all
three routes over the same offsets with identical output digests, and each plan must show its
route: only the native plan decodes natively, and the previous route still runs native operators.
Writes `results.md` beside the observations; slow samples are kept, never dropped.
"""
import collections
import json
import math
from pathlib import Path
import statistics
import sys

ROUTES = ('stock', 'previous', 'native')


def audit(root):
    method = json.loads((root / 'method.json').read_text())
    rows = [json.loads(line) for line in (root / 'observations.jsonl').read_text().splitlines()]
    runs = collections.defaultdict(dict)
    for row in rows:
        key = (row['workload'], row['records'], row['run'])
        assert row['route'] not in runs[key], f'duplicate {row["route"]} in {key}'
        runs[key][row['route']] = row
    cells = collections.defaultdict(lambda: collections.defaultdict(list))
    for (workload, records, run), routes in sorted(runs.items()):
        assert set(routes) == set(ROUTES), f'{workload} {records} run {run} lacks a route'
        assert {r['position'] for r in routes.values()} == {0, 1, 2}
        assert len({r['digest'] for r in routes.values()}) == 1, f'{workload} run {run} differs'
        for route, row in routes.items():
            assert row['warmup'] == (run < method['warmups'])
            assert math.isfinite(row['seconds']) and row['seconds'] > 0
            if not row['warmup']:
                cells[workload, records][route].append(row['seconds'])
    for (workload, records), routes in cells.items():
        for route in ROUTES:
            assert len(routes[route]) == method['trials'], f'{workload} {records} {route} trials'
            plan = (root / f'{workload}-{records}-{route}.plan').read_text()
            assert ('NativeKafkaDecode' in plan) == (route == 'native'), f'{workload} {route} plan'
            if route == 'previous':
                assert 'Native' in plan, f'{workload} previous plan runs nothing natively'
    return cells


def report(cells):
    lines = [
        '# Kafka source repeated measurements', '',
        'Median [min–max] elapsed seconds over every measured trial; ratios divide medians.', '',
        '| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for (workload, records), routes in sorted(cells.items()):
        medians = {route: statistics.median(routes[route]) for route in ROUTES}
        spans = ' | '.join(
            f'{medians[r]:.3f} [{min(routes[r]):.3f}–{max(routes[r]):.3f}]' for r in ROUTES)
        lines.append(
            f'| {workload} | {records:,} | {spans} | '
            f'{medians["stock"] / medians["native"]:.2f}× | '
            f'{medians["previous"] / medians["native"]:.2f}× |')
    return '\n'.join(lines) + '\n'


def main(argv):
    root = Path(argv[1])
    (root / 'results.md').write_text(report(audit(root)))


if __name__ == '__main__':
    main(sys.argv)
