"""Regenerate q23 post-fix samples and summaries from the archived release logs."""
import ast
import csv
import gzip
import re
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent
trials, summaries = [], []
pattern = re.compile(
    r'\[fluss\] (q23) rows=(\d+) parallelism=(\d+) stock=(\[[^\]]*\]) '
    r'native=(\[[^\]]*\]) primaryKeySink=(true|false) backend=(\w+) miniBatch=(true|false)'
)
for backend in ('memory', 'rocksdb'):
    log = gzip.decompress((root / f'{backend}-on.log.gz').read_bytes()).decode()
    assert 'BUILD SUCCESS' in log, backend
    matches = list(pattern.finditer(log))
    assert len(matches) == 1, backend
    query, rows, parallelism, stock, native, pk, actual_backend, mini = matches[0].groups()
    assert (rows, parallelism, pk, actual_backend, mini) == ('2000000', '4', 'false', backend, 'true')
    samples = {'stock': ast.literal_eval(stock), 'native': ast.literal_eval(native)}
    record = {'backend': backend, 'mini_batch': 'on', 'query': query}
    for engine, times in samples.items():
        assert len(times) == 3 and all(t > 0 for t in times)
        for repetition, seconds in enumerate(times, 1):
            trials.append(dict(backend=backend, mini_batch="on", query=query,
                               engine=engine, repetition=repetition, seconds=seconds))
        for stat, fn in [('min', min), ('median', statistics.median), ('max', max), ('stdev', statistics.stdev)]:
            record[f'{engine}_{stat}_seconds'] = fn(times)
    record['median_speedup'] = record['stock_median_seconds'] / record['native_median_seconds']
    summaries.append(record)
    print(backend, samples, f"{record['median_speedup']:.3f}x")
for name, records in [('trials.csv', trials), ('summary.csv', summaries)]:
    with (root / name).open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
