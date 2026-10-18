#!/usr/bin/env python3
"""Audit all matched Paimon trials and report repeated medians without dropping slow samples."""
import collections
import csv
import json
import math
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
method = json.loads((root / 'method.json').read_text())
rows = [json.loads(line) for line in (root / 'observations.jsonl').read_text().splitlines()]
measurements = [row for row in rows if row['kind'] == 'measurement']
pairs = [row for row in rows if row['kind'] == 'parity']
expected = {tuple(cell) for cell in method['expectedCells']}
assert expected, 'empty query selection'
trials = list(range(-method['warmups'], method['runs']))
index = {(r['query'], r['variant'], r['trial'], r['engine']): r for r in measurements}
parity = {(r['query'], r['variant'], r['trial']): r for r in pairs}
assert len(index) == len(measurements), 'duplicate measurements'
assert len(parity) == len(pairs), 'duplicate validations'
assert set(index) == {(q, v, t, e) for q, v in expected for t in trials for e in ('stock', 'native')}
assert set(parity) == {(q, v, t) for q, v in expected for t in trials}
for (query, variant, trial), pair in parity.items():
    assert pair['matched']
    stock, native = (index[query, variant, trial, e] for e in ('stock', 'native'))
    if query != 'q12':
        assert stock['fingerprint'] == native['fingerprint']
    for row in (stock, native):
        assert row['warmup'] == (trial < 0)
        assert math.isfinite(row['seconds']) and row['seconds'] > 0
    for row in (stock, native):
        evidence = root / row['evidence']
        if evidence.exists():
            plan = (evidence / 'plan.txt').read_text()
            checks = (evidence / 'checks.txt').read_text()
            assert "'bucket' = '4'" in (evidence / 'sink.sql').read_text()
            if query == 'q4':
                assert 'q4RocksDBObserved=true' in checks
            if row['engine'] == 'native':
                assert 'NativeKafkaDecode' in plan and 'native-kafka-source' in plan
                assert 'native-paimon-bucket-route' in plan
                assert 'RowDataToArrow' not in plan and 'ArrowToRowData' not in plan
            files = json.loads((evidence / 'files.json').read_text())
            assert len(files) == row['parquetFiles']
            assert sum('parquet-rs' in f['createdBy'] for f in files) == row['rustParquetFiles']
    assert stock['rustParquetFiles'] == 0
    if native['fingerprint']['count'] > 0:
        assert native['rustParquetFiles'] > 0
    for row in (stock, native):
        if query == 'q12':
            for bidder, count in row['fingerprint']['processingTimeCounts'].items():
                bidder = int(bidder)
                assert 4 <= bidder < 50
                assert 0 <= count <= method['events'] // 50 + (method['events'] % 50 > bidder)

cells = {}
lines = ['# Kafka to Paimon repeated measurements', '',
         '| Query | Variant | Stock median [range], s | Native median [range], s | Stock/native |',
         '|---|---|---:|---:|---:|']
for query, variant in sorted(expected, key=lambda x: (int(x[0][1:]), x[1])):
    samples = {e: [index[query, variant, t, e]['seconds'] for t in range(method['runs'])]
               for e in ('stock', 'native')}
    ratio = statistics.median(samples['stock']) / statistics.median(samples['native'])
    cells[query, variant] = ratio
    def fmt(e):
        s = samples[e]
        return f'{statistics.median(s):.3f} [{min(s):.3f}–{max(s):.3f}]'
    lines.append(f'| {query} | {variant} | {fmt("stock")} | {fmt("native")} | {ratio:.2f}× |')

def gm(values):
    values = list(values)
    return math.exp(statistics.mean(math.log(x) for x in values)) if values else None

exact = {q: r for (q, v), r in cells.items() if v == 'exact'}
selected = dict(exact)
selected.update({q: r for (q, v), r in cells.items() if v != 'exact'})
audit = {'queries': len(exact), 'cells': len(expected), 'allTrials': len(measurements),
         'measuredTrials': sum(not r['warmup'] for r in measurements), 'passingPairs': len(pairs),
         'exactGeomean': gm(exact.values()), 'readmeGeomean': gm(selected.values()),
         'exactWins': sum(r > 1 for r in exact.values()),
         'appendExactGeomean': gm(r for (q, v), r in cells.items() if v == 'exact' and index[q, v, 0, 'native']['sink'] == 'paimon-fixed-bucket'),
         'primaryKeyExactGeomean': gm(r for (q, v), r in cells.items() if v == 'exact' and index[q, v, 0, 'native']['sink'] == 'paimon-primary-key')}
lines += ['', f'Exact SQL geomean: **{audit["exactGeomean"]:.2f}×**; README selection: **{audit["readmeGeomean"]:.2f}×**.', '',
          'One ratio per query; medians use every measured repetition. All warmups and slow samples remain in observations.jsonl.', '']
(root / 'results.md').write_text('\n'.join(lines))
(root / 'audit.json').write_text(json.dumps(audit, indent=2) + '\n')
with (root / 'metrics.csv').open('w') as f:
    fields = ['query', 'variant', 'trial', 'warmup', 'engine', 'seconds', 'sink', 'parquetFiles', 'rustParquetFiles', 'parquetBytes']
    writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(measurements)
print(json.dumps(audit, indent=2))
