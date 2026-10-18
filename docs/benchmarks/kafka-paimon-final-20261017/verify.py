"""Recompute timings and validate the archived plan/table/file witnesses."""
import collections
import json
from pathlib import Path
import subprocess
import sys
import tarfile

root = Path(__file__).resolve().parent
repo = root.parents[2]
subprocess.run([sys.executable, str(repo / 'bin/report-paimon.py'), str(root)], check=True)
method = json.loads((root / 'method.json').read_text())
environment = json.loads((root / 'environment.json').read_text())
assert not environment['dirty']
assert method['events'] == 2000000
assert method['parallelism'] == method['kafkaPartitions'] == method['outputBuckets'] == method['clusterSlots'] == 4
assert method['managedMemory'] == '512 mb'
assert method['maxJavaHeapBytes'] == 4 * 1024**3
assert method['runtimeMode'] == 'STREAMING' and method['stateBackend'] == 'RocksDB'
assert method['sessionTimeZone'] == 'UTC' and not method['miniBatch']
assert method['warmups'] == 1 and method['runs'] == 3
assert len(method['expectedCells']) == 30
assert {c[0] for c in method['expectedCells']} == {f'q{i}' for i in range(24) if i != 6}
rows = [json.loads(line) for line in (root / 'observations.jsonl').read_text().splitlines()]
measurements = [r for r in rows if r['kind'] == 'measurement']
assert len(measurements) == 240
assert len({r['evidence'] for r in measurements}) == 240
keys = {'q4': ['id'], 'q9': ['id'], 'q15': ['day'], 'q16': ['channel', 'day'],
        'q17': ['auction', 'day'], 'q18': ['bidder', 'auction'], 'q19': ['auction', 'rank_number']}
counts = collections.Counter()
writer_parallelism = collections.defaultdict(set)
with tarfile.open(root / 'evidence.tar.gz') as archive:
    def text(path):
        return archive.extractfile(path).read().decode()
    for row in measurements:
        path = row['evidence']
        plan = text(path + '/plan.txt')
        checks = text(path + '/checks.txt')
        graph = json.loads(plan.split('== Physical Execution Plan ==', 1)[1])
        sources = [n for n in graph['nodes'] if n['pact'] == 'Data Source' and n['type'].startswith('Source:')]
        writers = [n for n in graph['nodes'] if n['type'].startswith('Writer :')]
        assert sources and all(n['parallelism'] == 4 for n in sources)
        assert writers
        writer_parallelism[row['query']].update(n['parallelism'] for n in writers)
        if row['engine'] == 'native':
            assert all(n['type'] == 'Source: native-kafka-source' for n in sources)
            assert 'NativePaimonSink' in plan and 'native-paimon-bucket-route' in plan
            assert 'RowDataToArrow' not in plan and 'ArrowToRowData' not in plan
            assert 'No operators fell back to Flink.' in checks
        else:
            assert 'native-kafka-source' not in plan and 'NativePaimonSink' not in plan
        if row['query'] == 'q4':
            assert 'q4RocksDBObserved=true' in checks
            counts['rocksDBWitnesses'] += 1
        if row['query'] == 'q0':
            assert row['fingerprint']['count'] == 1840000
        schema = json.loads(text(path + '/table/schema/schema-0'))
        assert schema['primaryKeys'] == keys.get(row['query'], [])
        assert schema['options']['bucket'] == '4'
        assert schema['options']['file.format'] == 'parquet'
        assert schema['options'].get('write-only', 'false') == 'false'
        if not schema['primaryKeys']:
            assert schema['options']['bucket-key'].strip('`') == schema['fields'][0]['name']
        files = json.loads(text(path + '/files.json'))
        assert len(files) == row['parquetFiles']
        assert sum('parquet-rs' in f['createdBy'] for f in files) == row['rustParquetFiles']
        assert all(f['codecs'] == ['ZSTD'] for f in files)
        counts['nativePlans' if row['engine'] == 'native' else 'stockPlans'] += 1
        counts['tableSchemas'] += 1
        counts['nativeTrialsWithRustFiles'] += row['engine'] == 'native' and row['rustParquetFiles'] > 0
        counts['parquetFooters'] += len(files)
assert counts['nativePlans'] == counts['stockPlans'] == 120
assert counts['tableSchemas'] == 240 and counts['rocksDBWitnesses'] == 8
assert all(v == {1 if q == 'q5' else 4} for q, v in writer_parallelism.items())
result = dict(counts, freshJvmCohorts=1, resumedCells=0, libraries=environment['libraries'],
              writerParallelism={q: sorted(v) for q, v in writer_parallelism.items()})
(root / 'evidence-audit.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))

subprocess.run([sys.executable, str(root / 'compare-iceberg.py')], check=True)
