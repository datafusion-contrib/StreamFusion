#!/usr/bin/env python3
"""Quick release Iceberg file-task comparison against the stock Flink connector."""
import argparse
import datetime
import fnmatch
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / 'target/iceberg-io'
MODULE = ROOT / 'streamfusion-iceberg'
BENCHMARK = MODULE / 'src/test/java/tech/streamfusion/iceberg/IcebergIoBenchmark.java'


def files(*directories, glob=None):
    if not shutil.which('rg'):
        result = []
        for directory in directories:
            for root, children, names in os.walk(directory):
                children[:] = [name for name in children if name not in {'target', '.git'}]
                result.extend(Path(root) / name for name in names if not glob or fnmatch.fnmatch(name, glob))
        return result
    command = ['rg', '--files', *map(str, directories)]
    if glob:
        command += ['-g', glob]
    return [ROOT / p for p in subprocess.check_output(command, cwd=ROOT, text=True).splitlines()]


def digest(paths):
    value = hashlib.sha256()
    for path in sorted(paths):
        value.update(str(path.relative_to(ROOT)).encode())
        value.update(path.read_bytes())
    return value.hexdigest()


def run(command, log, env=None):
    with log.open('w') as output:
        result = subprocess.run(list(map(str, command)), cwd=ROOT, env=env,
                                stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        raise SystemExit(f'Command failed; see {log}\n{log.read_text()[-6000:]}')


def build(force):
    CACHE.mkdir(parents=True, exist_ok=True)
    state_path = CACHE / 'build.json'
    previous = json.loads(state_path.read_text()) if state_path.exists() else {}
    current = {
        'native': digest(files(ROOT / 'native', glob='*.rs') + files(ROOT / 'native', glob='Cargo.*')),
        'module': digest(files(MODULE / 'src/main/java')),
        'benchmark': digest([BENCHMARK]),
        'support': digest(files(ROOT / 'src/main') + files(MODULE / 'src/main/resources') + [ROOT / 'pom.xml', MODULE / 'pom.xml', ROOT / 'streamfusion-core/pom.xml']),
    }
    native_changed = force or previous.get('native') != current['native'] or not Path(previous.get('nativeDirectory', '/nonexistent')).is_dir()
    if native_changed:
        print('Building release native libraries (incremental).', flush=True)
        run(['cargo', 'build', '--manifest-path', ROOT / 'native/Cargo.toml', '--release',
             '--features', 'mimalloc', '-p', 'streamfusion', '-p', 'streamfusion-iceberg'], CACHE / 'cargo.log')
        metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--manifest-path', str(ROOT / 'native/Cargo.toml'), '--no-deps', '--format-version', '1'], text=True))
        release = Path(metadata['target_directory']) / 'release'
        extension = 'dylib' if os.uname().sysname == 'Darwin' else 'so'
        libraries = [release / f'lib{name}.{extension}' for name in ['streamfusion', 'streamfusion_iceberg']]
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in libraries}
        native = CACHE / ('native-' + hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()[:16])
        native.mkdir(exist_ok=True)
        for p in libraries:
            if not (native / p.name).exists(): shutil.copy2(p, native / p.name)
        current.update(nativeDirectory=str(native), libraries=hashes)
    else:
        current.update(nativeDirectory=previous['nativeDirectory'], libraries=previous['libraries'])
    classpath_file = CACHE / 'classpath.txt'
    prepare = force or previous.get('support') != current['support'] or not classpath_file.exists() or not (MODULE / 'target/classes').is_dir() or not (MODULE / 'target/test-classes/tech/streamfusion/iceberg/IcebergIoBenchmark.class').is_file() or not (ROOT / 'streamfusion-core/target/classes').is_dir()
    if prepare:
        print('Preparing Java classes and dependency classpath (once per core/dependency change).', flush=True)
        environment = dict(os.environ, MAVEN_OPTS=os.environ.get('MAVEN_OPTS', '-Xmx512m'),
                           SF_ICEBERG_IO_PREPARE='true', SF_ICEBERG_IO_CLASSPATH=str(classpath_file))
        run(['mvn', '-B', '-ntp', '-Piceberg,bench', '-pl', 'streamfusion-iceberg', '-am',
             '-Dtest=IcebergIoBenchmark', '-Dsurefire.failIfNoSpecifiedTests=false',
             '-Dmaven.javadoc.skip=true', '-Dnative.build.skip=true',
             '-Dnative.resource.directory=' + current['nativeDirectory'],
             '-Dsf.extraJvmArgs=-Djava.library.path=' + current['nativeDirectory'] + ' -Xmx4g', 'test'], CACHE / 'prepare.log', environment)
    else:
        classpath = classpath_file.read_text().strip()
        if previous.get('module') != current['module']:
            print('Compiling connector Java directly.', flush=True)
            run(['javac', '--release', '17', '-cp', classpath, '-d', MODULE / 'target/classes',
                 *files(MODULE / 'src/main/java', glob='*.java')], CACHE / 'javac.log')
        if previous.get('benchmark') != current['benchmark']:
            run(['javac', '--release', '17', '-cp', classpath, '-d', MODULE / 'target/test-classes', BENCHMARK], CACHE / 'benchmark-javac.log')
    state_path.write_text(json.dumps(current, indent=2) + '\n')
    return current, classpath_file.read_text().strip()


def measurements(path):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    return rows[0], [r for r in rows if r['kind'] == 'measurement' and not r['warmup']]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare', action='store_true', help='Refresh the full Java/dependency build')
    parser.add_argument('--rows', type=int, default=262144)
    parser.add_argument('--batch-rows', type=int, default=8192)
    parser.add_argument('--width', type=int, default=48, help='String padding characters')
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--stripes', type=int, default=4, help='Seed files per partition')
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--target-bytes', type=int, default=536870912)
    parser.add_argument('--cases', default='read,write', help='read,write and optional route,dynamic-route (destination calculation only)')
    parser.add_argument('--profile', choices=['itimer', 'alloc', 'wall'], help='Diagnostic profiles, excluded from performance claims')
    parser.add_argument('--profiler', type=Path, help='Released async-profiler profiler.sh')
    parser.add_argument('--baseline', type=Path, help='Previous observations.jsonl; compare native medians')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if bool(args.profile) != bool(args.profiler): parser.error('--profile and --profiler must be supplied together')
    version = subprocess.run(['java', '-version'], text=True, capture_output=True).stderr
    if 'version "17.' not in version: parser.error('Use JDK 17 (JAVA_HOME and PATH).')
    start = time.monotonic()
    state, classpath = build(args.prepare)
    prepared = time.monotonic()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    output = (args.output or CACHE / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, SF_ICEBERG_IO_DIRECTORY=str(output), SF_ICEBERG_IO_FIXTURES=str(CACHE / 'fixtures'))
    for key in ['rows', 'batch_rows', 'width', 'partitions', 'stripes', 'warmup', 'runs', 'target_bytes', 'cases']:
        environment['SF_ICEBERG_IO_' + key.upper()] = str(getattr(args, key))
    if args.profile:
        environment['SF_ICEBERG_IO_PROFILER'] = str(args.profiler.resolve())
        environment['SF_ICEBERG_IO_PROFILE'] = args.profile
    command = ['java', '-Xmx4g', '-Djava.library.path=' + state['nativeDirectory'],
               '-Dstreamfusion.native.development=true', '-Darrow.enable_unsafe_memory_access=true',
               '-Darrow.enable_null_check_for_get=false',
               '-Dlog4j.configurationFile=' + str(MODULE / 'src/test/resources/log4j2-test.xml')]
    for package in ['java.nio', 'java.lang', 'java.util', 'java.io', 'java.net', 'java.util.concurrent',
                    'java.util.concurrent.atomic', 'java.lang.reflect', 'sun.nio.ch']:
        command += ['--add-opens=java.base/' + package + '=ALL-UNNAMED']
    command += ['-cp', classpath, 'tech.streamfusion.iceberg.IcebergIoBenchmark']
    run(command, output / 'run.log', environment)
    method, observations = measurements(output / 'observations.jsonl')
    baseline = None
    if args.baseline:
        old_method, baseline = measurements(args.baseline)
        if method != old_method: raise SystemExit('Baseline method differs; compare matching fixtures/settings.')
    if args.profile: print('Profiled timings are diagnostic; use unprofiled runs for performance comparisons.')
    report = ['| Operation | Stock Flink Iceberg median [range], ms | Native median [range], ms | Stock/native | Previous native/new |',
              '|---|---:|---:|---:|---:|']
    for operation in args.cases.split(','):
        values = {n: [r['seconds'] * 1000 for r in observations if r['operation'] == operation and r['native'] == n] for n in [False, True]}
        if any(len(v) != args.runs for v in values.values()): raise SystemExit('Incomplete repetitions')
        def cell(v): return f'{statistics.median(v):.2f} [{min(v):.2f}–{max(v):.2f}]'
        comparison = '—'
        if baseline:
            old = [r['seconds'] * 1000 for r in baseline if r['operation'] == operation and r['native']]
            if not old: raise SystemExit(f'Baseline has no native measurements for {operation}')
            comparison = f'{statistics.median(old) / statistics.median(values[True]):.2f}×'
        report.append(f'| {operation} | {cell(values[False])} | {cell(values[True])} | {statistics.median(values[False]) / statistics.median(values[True]):.2f}× | {comparison} |')
    state.update(commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                 dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
                 java=version, platform=platform.platform(), cpuCount=os.cpu_count(), arguments=vars(args) | {'output': str(output), 'baseline': str(args.baseline) if args.baseline else None},
                 prepareSeconds=prepared - start, benchmarkProcessSeconds=time.monotonic() - prepared)
    (output / 'environment.json').write_text(json.dumps(state, indent=2, default=str) + '\n')
    (output / 'results.md').write_text('\n'.join(report) + '\n')
    print('\n'.join(report))
    print(f'Preparation {prepared-start:.2f}s; benchmark process including validation {time.monotonic()-prepared:.2f}s. Artifacts: {output}')


if __name__ == '__main__':
    main()
