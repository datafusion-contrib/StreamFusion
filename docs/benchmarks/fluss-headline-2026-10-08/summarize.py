import ast, csv, gzip, math, re, statistics
from pathlib import Path
root = Path(__file__).resolve().parent
trials, summary = [], []
expected = {f'q{i}' for i in range(24) if i != 6}
for backend in ('memory', 'rocksdb'):
    for mini in ('off', 'on'):
        log = root / f'{backend}-{mini}.log.gz'
        if not log.exists():
            continue
        text = gzip.decompress(log.read_bytes()).decode()
        failed_q23 = 'BUILD SUCCESS' not in text and mini == 'on' and 'Append-only production received a changelog' in text
        if 'BUILD SUCCESS' not in text and not failed_q23:
            raise RuntimeError(f'{log} failed unexpectedly')
        seen = set()
        pattern = r'\[fluss\] (q\d+) rows=(\d+) parallelism=(\d+) stock=(\[[^\]]*\]) native=(\[[^\]]*\]) primaryKeySink=(true|false) backend=(\w+) miniBatch=(true|false)'
        for match in re.finditer(pattern, text):
            query, rows, parallelism, stock_text, native_text, primary_key, actual_backend, actual_mini = match.groups()
            assert query not in seen
            assert rows == '2000000' and parallelism == '4'
            assert actual_backend == backend and actual_mini == ('true' if mini == 'on' else 'false')
            seen.add(query)
            stock, native = ast.literal_eval(stock_text), ast.literal_eval(native_text)
            assert len(stock) == len(native) == 3
            record = dict(backend=backend, mini_batch=mini, query=query, primary_key_sink=primary_key)
            for engine, times in [('stock', stock), ('native', native)]:
                for repeat, seconds in enumerate(times, 1):
                    trials.append(dict(backend=backend, mini_batch=mini, query=query, primary_key_sink=primary_key, engine=engine, repetition=repeat, seconds=seconds))
                for stat, fn in [('min', min), ('median', statistics.median), ('max', max), ('stdev', statistics.stdev)]:
                    record[f'{engine}_{stat}_seconds'] = fn(times)
            record['median_speedup'] = statistics.median(stock) / statistics.median(native)
            record['best_speedup'] = min(stock) / min(native)
            summary.append(record)
        assert seen == (expected - {'q23'} if failed_q23 else expected), (log, seen ^ expected)
for name, records in [('trials.csv', trials), ('summary.csv', summary)]:
    if records:
        with (root / name).open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
for backend in ('memory', 'rocksdb'):
    for mini in ('off', 'on'):
        records = [r for r in summary if r['backend'] == backend and r['mini_batch'] == mini]
        if not records:
            continue
        geomean = math.exp(statistics.mean(math.log(r['median_speedup']) for r in records))
        print(backend, mini, len(records), f'{geomean:.3f}x', 'regressions', [(r['query'], round(r['median_speedup'], 3)) for r in records if r['median_speedup'] < 1])
