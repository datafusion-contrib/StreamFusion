#!/usr/bin/env python3
"""Retain complete benchmark cells in Git and resume without selecting fast samples."""
import argparse
import collections
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import time


def read_rows(path):
    if not path.exists():
        return []
    # The Java process may be in the middle of appending the last line.
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
            if line.endswith('\n')]


def key(row):
    return row['query'], row['mode'], row['variant']


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')


def archive_cell(artifacts, destination, observations):
    checkpoint_sizes = []
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('wb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            for row in observations:
                if row['kind'] != 'measurement':
                    continue
                token = row['table'].rstrip('/').rsplit('/', 1)[-1]
                trial = artifacts / token
                table = artifacts / 'warehouse' / 'bench' / token
                paths = [trial / 'plan.txt', trial / 'graph.json']
                paths.extend(sorted(trial.glob('*.collapsed')))
                versions = list((table / 'metadata').glob('v*.metadata.json'))
                if not versions:
                    raise RuntimeError(f'No committed table metadata for {token}')
                paths.append(max(versions, key=lambda p: int(p.name.split('.')[0][1:])))
                for path in paths:
                    data = path.read_bytes().replace(str(artifacts).encode(), b'artifact:')
                    data = data.replace(str(Path(__file__).resolve().parent.parent).encode(), b'repo:')
                    info = tarfile.TarInfo(str(path.relative_to(artifacts)))
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                for phase in ('streaming-checkpoint', 'savepoint'):
                    if not (trial / phase).exists():
                        continue
                    sizes = [p.stat().st_size for p in (trial / phase).rglob('*') if p.is_file()]
                    checkpoint_sizes.append(dict(trial=token, phase=phase,
                                                 files=len(sizes), bytes=sum(sizes)))
            data = (json.dumps(checkpoint_sizes, indent=2) + '\n').encode()
            info = tarfile.TarInfo('checkpoint-sizes.json')
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--push', metavar='REMOTE', help='commit and push complete cells')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    evidence = args.evidence.resolve()
    relative = evidence.relative_to(repo)
    evidence.mkdir(parents=True, exist_ok=True)
    retained = read_rows(evidence / 'observations.jsonl')
    completed = {key(r) for r in retained if r['kind'] == 'parity'}
    if completed:
        method = json.loads((evidence / 'method.json').read_text())
        if completed == {tuple(c) for c in method['expectedCells']}:
            subprocess.run(['python3', str(repo / 'bin/report-iceberg.py'), str(evidence)], check=True)
            return
    cohort = f'cohort-{len(list((evidence / "cohorts").glob("*"))) + 1:03d}'
    artifacts = repo / 'target' / 'iceberg-checkpointed' / evidence.name / cohort
    artifacts.mkdir(parents=True, exist_ok=False)
    resume = artifacts / 'completed.jsonl'
    resume.write_text(''.join(json.dumps(c) + '\n' for c in sorted(completed)))
    acknowledgements = artifacts / 'acknowledgements'
    acknowledgements.mkdir()
    env = dict(os.environ, SF_ICEBERG_ARTIFACTS=str(artifacts),
               SF_ICEBERG_COMPLETED_CELLS=str(resume),
               SF_ICEBERG_CELL_ACK_DIRECTORY=str(acknowledgements))
    cohort_path = evidence / 'cohorts' / cohort
    cohort_path.mkdir(parents=True)
    with (artifacts / 'driver.log').open('w') as log:
        process = subprocess.Popen(['bash', str(repo / 'bin/bench-iceberg.sh')],
                                   cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT)
        while True:
            if (artifacts / 'method.json').exists():
                method = json.loads((artifacts / 'method.json').read_text())
                prior = evidence / 'method.json'
                if prior.exists():
                    previous = json.loads(prior.read_text())
                    if previous.get('snapshotCompression', False) != method.get('snapshotCompression', False):
                        process.terminate()
                        raise RuntimeError('Resume changed snapshot compression')
                    for setting in ('events', 'parallelism', 'stripeEvents', 'warmups', 'runs',
                                    'fileSettings', 'expectedCells', 'maxJavaHeapBytes', 'baselineEngine',
                                    'completionProtocol', 'checkpointStorage', 'objectReuse'):
                        if previous[setting] != method[setting]:
                            process.terminate()
                            raise RuntimeError(f'Resume changed {setting}')
                    if previous.get('managedMemory', 'cluster default') != method.get('managedMemory', 'cluster default'):
                        process.terminate()
                        raise RuntimeError('Resume changed managed memory')
                    if previous.get('sessionTimeZone') != method.get('sessionTimeZone'):
                        process.terminate()
                        raise RuntimeError('Resume changed session time-zone configuration')
                    if previous.get('source', 'iceberg') != method.get('source', 'iceberg'):
                        process.terminate()
                        raise RuntimeError('Resume changed source')
                else:
                    write_json(prior, method)
                rows = read_rows(artifacts / 'observations.jsonl')
                groups = collections.defaultdict(list)
                for row in rows:
                    if row['kind'] in ('measurement', 'parity'):
                        groups[key(row)].append(row)
                for cell, records in groups.items():
                    if cell in completed:
                        continue
                    trials = list(range(-method['warmups'], method['runs']))
                    parity = [r for r in records if r['kind'] == 'parity' and r['matched']]
                    if sorted(r['trial'] for r in parity) != trials:
                        continue
                    measurements = [r for r in records if r['kind'] == 'measurement']
                    counts = collections.Counter((r['engine'], r['trial']) for r in measurements)
                    if counts != collections.Counter((engine, trial) for engine in
                                                     (method['baselineEngine'], 'streamfusion') for trial in trials):
                        raise RuntimeError(f'Invalid paired observations: {cell}')
                    for row in measurements:
                        assert row['resources']['maxJavaHeapBytes'] == method['maxJavaHeapBytes']
                        if row['engine'] == 'streamfusion':
                            assert row['sourceCount'] == row['nativeSourceCount'] > 0
                            assert row['nativeWriterCount'] > 0
                            assert row['stockFallbackTasks'] == 0
                            if method.get('source', 'iceberg') == 'iceberg':
                                assert row['nativeParquetTasks'] > 0
                        else:
                            assert row['nativeSourceCount'] == row['nativeWriterCount'] == 0
                    for trial in trials:
                        fingerprints = [r['fingerprint'] for r in measurements if r['trial'] == trial]
                        assert fingerprints[0] == fingerprints[1], (cell, trial)
                    archive_cell(artifacts, cohort_path / ('-'.join(cell) + '.tar.gz'), records)
                    for name in ('method.json', 'environment.json'):
                        (cohort_path / name).write_text((artifacts / name).read_text())
                    seeds = [r for r in rows if r['kind'] == 'seed']
                    write_json(cohort_path / 'seed.json', seeds)
                    with (evidence / 'observations.jsonl').open('a') as output:
                        for row in records:
                            row = dict(row, cohort=cohort)
                            output.write(json.dumps(row).replace(str(artifacts), f'artifact:{cohort}') + '\n')
                    completed.add(cell)
                    write_json(evidence / 'progress.json', dict(completedCells=len(completed),
                               expectedCells=len(method['expectedCells']), lastCell=cell))
                    print(f'{len(completed)}/{len(method["expectedCells"])} complete: {cell}', flush=True)
                    if args.push:
                        subprocess.run(['git', 'add', '--', str(relative)], cwd=repo, check=True)
                        subprocess.run(['git', 'commit', '-m',
                                        f'Retain validated Iceberg benchmark cell {"/".join(cell)}'],
                                       cwd=repo, check=True)
                        subprocess.run(['git', 'push', args.push, 'HEAD'], cwd=repo, check=True)
                    acknowledgement = acknowledgements / '-'.join(cell)
                    acknowledgement.parent.mkdir(parents=True, exist_ok=True)
                    acknowledgement.touch()
            if process.poll() is not None:
                break
            time.sleep(1)
    if process.returncode:
        raise SystemExit(f'Benchmark failed ({process.returncode}); see {artifacts / "driver.log"}')
    method = json.loads((evidence / 'method.json').read_text())
    if completed != {tuple(c) for c in method['expectedCells']}:
        raise SystemExit('Benchmark exited without completing the matrix')
    subprocess.run(['python3', str(repo / 'bin/report-iceberg.py'), str(evidence)], check=True)
    if args.push:
        subprocess.run(['git', 'add', '--', str(relative)], cwd=repo, check=True)
        subprocess.run(['git', 'commit', '-m', 'Report the complete paired Iceberg Nexmark matrix'],
                       cwd=repo, check=True)
        subprocess.run(['git', 'push', args.push, 'HEAD'], cwd=repo, check=True)


if __name__ == '__main__':
    main()
