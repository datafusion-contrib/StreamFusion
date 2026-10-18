#!/usr/bin/env python3
"""Report only paired, validated measurements; retain every raw observation alongside them."""
import collections
import csv
import json
import math
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
method = json.loads((root / 'method.json').read_text())
baseline = method.get('baselineEngine', 'flink')
baseline_label = method.get('baselineLabel', 'Flink')
rows = [json.loads(line) for line in (root / 'observations.jsonl').read_text().splitlines()]
paired = {(row['query'], row['mode'], row['variant'], row['trial'])
          for row in rows if row['kind'] == 'parity' and row['matched']}
groups = collections.defaultdict(lambda: collections.defaultdict(list))
for row in rows:
    if row['kind'] != 'measurement' or row['warmup']:
        continue
    key = (row['query'], row['mode'], row['variant'])
    if (*key, row['trial']) not in paired:
        raise SystemExit(f'Unpaired measurement: {key}, trial {row["trial"]}')
    groups[key][row['engine']].append(row)
if 'expectedCells' in method and set(groups) != {tuple(cell) for cell in method['expectedCells']}:
    raise SystemExit('Missing or unexpected query/mode/variant cells')
lines = ['# Streaming Iceberg Nexmark observations', '',
         f'{method["events"]:,} deterministic events; parallelism {method["parallelism"]}; '
         f'{method.get("inputDescription", "four-bucket Iceberg input tables")}; four-bucket Iceberg output tables. '
         f'Completion: {method["completionProtocol"]}.', '',
         f'| Query | State / mini-batch | Variant | {baseline_label} median [range], s | StreamFusion median [range], s | Median ratio |',
         '|---|---|---|---:|---:|---:|']
for key, engines in sorted(groups.items(), key=lambda item: (int(item[0][0][1:]), item[0][1:])):
    samples = {}
    for engine in (baseline, 'streamfusion'):
        observations = sorted(engines[engine], key=lambda row: row['trial'])
        if [row['trial'] for row in observations] != list(range(method['runs'])):
            raise SystemExit(f'Incomplete measured repetitions: {key}, {engine}')
        samples[engine] = [row['seconds'] for row in observations]
    def cell(values):
        return f'{statistics.median(values):.3f} [{min(values):.3f}–{max(values):.3f}]'
    ratio = statistics.median(samples[baseline]) / statistics.median(samples['streamfusion'])
    lines.append(f'| {key[0]} | {key[1]} | {key[2]} | {cell(samples[baseline])} | {cell(samples["streamfusion"])} | {ratio:.2f}× |')
lines += ['', '## Exact-query aggregates', '',
          'Geometric means use every exact-query cell in each mode; labelled expression variants '
          'remain separate above. Throughput is nominal corpus events divided by median end-to-end '
          'time, including startup, checkpoints and drain.', '',
          f'| State / mini-batch | Queries | Geomean ratio | {baseline_label} corpus events/s, range | StreamFusion corpus events/s, range |',
          '|---|---:|---:|---:|---:|']
for mode in sorted({key[1] for key in groups}):
    ratios = []
    throughput = {baseline: [], 'streamfusion': []}
    for key, engines in groups.items():
        if key[1] != mode or key[2] != 'exact':
            continue
        medians = {engine: statistics.median(row['seconds'] for row in engines[engine])
                   for engine in throughput}
        ratios.append(medians[baseline] / medians['streamfusion'])
        for engine in throughput:
            throughput[engine].append(method['events'] / medians[engine])
    if ratios:
        gm = math.exp(statistics.mean(math.log(ratio) for ratio in ratios))
        def rate(engine):
            values = throughput[engine]
            return f'{min(values):,.0f}–{max(values):,.0f}'
        lines.append(f'| {mode} | {len(ratios)} | {gm:.2f}× | {rate(baseline)} | {rate("streamfusion")} |')
lines += ['', '## README query selection', '',
          'Each query contributes once per mode, using its existing labelled variant when '
          'present. Median ratios retain all repetitions. The secondary headline sample '
          'uses the first memory sample or the best of the first two disk samples.', '',
          '| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |',
          '|---|---:|---:|---:|']
preferred = {}
for key in groups:
    query_mode = key[:2]
    if query_mode not in preferred or key[2] != 'exact':
        if key[2] != 'exact' and preferred.get(query_mode, key)[2] != 'exact' and preferred.get(query_mode) not in (None, key):
            raise SystemExit(f'Ambiguous README variant: {query_mode}')
        preferred[query_mode] = key
for mode in sorted({key[1] for key in groups}):
    median_ratios, headline_ratios = [], []
    for (query, selected_mode), key in preferred.items():
        if selected_mode != mode:
            continue
        samples = {engine: [r['seconds'] for r in sorted(groups[key][engine], key=lambda r: r['trial'])]
                   for engine in (baseline, 'streamfusion')}
        median_ratios.append(statistics.median(samples[baseline]) / statistics.median(samples['streamfusion']))
        selected = {engine: min(values[:2]) if mode.startswith('disk') else values[0]
                    for engine, values in samples.items()}
        headline_ratios.append(selected[baseline] / selected['streamfusion'])
    gm = lambda values: math.exp(statistics.mean(math.log(v) for v in values))
    lines.append(f'| {mode} | {len(median_ratios)} | {gm(median_ratios):.2f}× | {gm(headline_ratios):.2f}× |')
if method['runs'] >= 2:
    lines += ['', '## Headline sampling convention', '',
              'Memory uses the first measured sample; disk uses the best of the first two. '
              'The repeated medians and complete ranges above remain the primary results.', '',
              f'| Query | State / mini-batch | Variant | {baseline_label}, s | StreamFusion, s | Ratio |',
              '|---|---|---|---:|---:|---:|']
    for key, engines in sorted(groups.items(), key=lambda item: (int(item[0][0][1:]), item[0][1:])):
        selected = {}
        for engine in (baseline, 'streamfusion'):
            values = [row['seconds'] for row in sorted(engines[engine], key=lambda row: row['trial'])]
            selected[engine] = min(values[:2]) if key[1].startswith('disk') else values[0]
        ratio = selected[baseline] / selected['streamfusion']
        lines.append(f'| {key[0]} | {key[1]} | {key[2]} | {selected[baseline]:.3f} | {selected["streamfusion"]:.3f} | {ratio:.2f}× |')
lines += ['', 'Raw samples, warmups, parity checks, plans, committed table locations and native/fallback '
          'task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder '
          + ('emitted counts do not exceed input counts; unfinished processing-time windows remain open.'
             if method.get('source') == 'kafka-json' else
             'count conservation and includes waiting for the final processing-time window.'), '']
lines += ['[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain '
          'separate phase medians. Phase medians need not sum to the total median. '
          'Memory peaks are sampled during progress polling, not exhaustive job maxima.', '']

def median_field(observations, *path):
    values = []
    for row in observations:
        value = row
        for name in path:
            value = value.get(name) if isinstance(value, dict) else None
        if value is None:
            return None
        values.append(value)
    return statistics.median(values)

metrics = []
for key, engines in sorted(groups.items()):
    for engine in (baseline, 'streamfusion'):
        observations = engines[engine]
        seconds = [r['seconds'] for r in observations]
        metrics.append(dict(
            query=key[0], mode=key[1], variant=key[2], engine=engine,
            repetitions=len(seconds), median_seconds=statistics.median(seconds),
            min_seconds=min(seconds), max_seconds=max(seconds),
            nominal_corpus_events_per_second=method['events'] / statistics.median(seconds),
            corpus_window_median_seconds=median_field(observations, 'timingPhases', 'submissionToCorpusAndWindowSeconds'),
            settling_checkpoint_median_seconds=median_field(observations, 'timingPhases', 'settleCheckpointSeconds'),
            drain_commit_median_seconds=median_field(observations, 'timingPhases', 'drainCommitSeconds'),
            checkpoint_state_median_bytes=median_field(observations, 'settlingCheckpoint', 'stateBytes'),
            checkpoint_reported_median_millis=median_field(observations, 'settlingCheckpoint', 'durationMillis'),
            sampled_task_peak_median_bytes=median_field(observations, 'resources', 'sampledTaskPeakBytes'),
            sampled_arrow_peak_median_bytes=median_field(observations, 'resources', 'sampledArrowPeakBytes'),
            sampled_rss_peak_median_bytes=median_field(observations, 'resources', 'sampledRssPeakBytes'),
            java_heap_limit_bytes=median_field(observations, 'resources', 'maxJavaHeapBytes'),
            data_files_median=median_field(observations, 'dataFiles'),
            delete_files_median=median_field(observations, 'deleteFiles')))
with (root / 'metrics.csv').open('w', newline='') as output:
    writer = csv.DictWriter(output, fieldnames=list(metrics[0]))
    writer.writeheader()
    writer.writerows(metrics)
(root / 'results.md').write_text('\n'.join(lines))
print('\n'.join(lines))
