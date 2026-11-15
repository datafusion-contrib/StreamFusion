#!/usr/bin/env python3
"""Retrieve hash-pinned historical evidence without adding run outputs to Git."""
import argparse
import hashlib
import json
import logging
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'docs/benchmarks/artifacts.json'
LOG = logging.getLogger('mkdocs.streamfusion.evidence')


def verify(path, record):
    if not path.is_file() or path.stat().st_size != record['size']:
        return False
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest() == record['sha256']


def evidence_path(name):
    path = PurePosixPath(name)
    if (path.is_absolute() or '..' in path.parts or '\\' in name
            or path.as_posix() != name or not name.startswith('docs/benchmarks/')):
        raise ValueError(f'Invalid evidence path: {name}')
    return path


def load_manifest(path=MANIFEST):
    manifest = json.loads(path.read_text())
    if manifest.get('schema_version') != 1:
        raise ValueError('Unsupported benchmark artifact manifest')
    for name, record in manifest['files'].items():
        evidence_path(name)
        if record['bundle'] not in manifest['bundles']:
            raise ValueError(f'Unknown evidence bundle: {name}')
    return manifest


def download(record, cache):
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / (record['sha256'] + '.tar.gz')
    if verify(archive, record):
        return archive
    LOG.info('Downloading historical evidence (%.1f MiB): %s', record['size'] / 1024**2, record['url'])
    request = urllib.request.Request(record['url'], headers={'User-Agent': 'StreamFusion-evidence'})
    for attempt in range(3):
        try:
            with tempfile.NamedTemporaryFile(dir=cache, delete=False) as output:
                temporary = Path(output.name)
                try:
                    with urllib.request.urlopen(request, timeout=60) as response:
                        size = 0
                        for chunk in iter(lambda: response.read(1024 * 1024), b''):
                            size += len(chunk)
                            if size > record['size']:
                                raise ValueError('Evidence download exceeds the recorded size')
                            output.write(chunk)
                    output.close()
                    if not verify(temporary, record):
                        raise ValueError(f'Evidence bundle size/SHA-256 mismatch: {record["url"]}')
                    temporary.replace(archive)
                finally:
                    temporary.unlink(missing_ok=True)
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if attempt == 2:
                raise
            LOG.warning('Evidence download attempt %s failed: %s; retrying', attempt + 1, error)
            time.sleep(2**attempt)
    return archive


def fetch(prefixes, destination, cache, manifest_path=MANIFEST):
    manifest = load_manifest(manifest_path)
    wanted = {}
    for prefix in prefixes:
        prefix = prefix.rstrip('/')
        matches = {name: record for name, record in manifest['files'].items()
                   if name == prefix or name.startswith(prefix + '/')}
        if not matches:
            raise ValueError(f'No archived evidence matches: {prefix}')
        wanted.update(matches)
    destination = destination.resolve()
    pending = {}
    for name, record in wanted.items():
        path = destination / name
        if not path.resolve().is_relative_to(destination):
            raise ValueError(f'Evidence destination escapes the output directory: {name}')
        if path.exists() or path.is_symlink():
            if not verify(path, record):
                raise ValueError(f'Refusing to replace changed local evidence: {path}')
        else:
            pending[name] = record
    for bundle in sorted({record['bundle'] for record in pending.values()}):
        archive = download(manifest['bundles'][bundle], cache)
        seen = set()
        with tarfile.open(archive, 'r:gz') as source:
            for member in source:
                name = member.name
                evidence_path(name)
                record = manifest['files'].get(name)
                if (name in seen or not member.isfile() or record is None
                        or record['bundle'] != bundle or member.size != record['size']):
                    raise ValueError(f'Unexpected evidence archive member: {name}')
                seen.add(name)
                if name not in pending:
                    continue
                path = destination / name
                path.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as output:
                    temporary = Path(output.name)
                    try:
                        with source.extractfile(member) as stream:
                            shutil.copyfileobj(stream, output)
                        output.close()
                        if not verify(temporary, record):
                            raise ValueError(f'Evidence file SHA-256 mismatch: {name}')
                        temporary.replace(path)
                    finally:
                        temporary.unlink(missing_ok=True)
        expected = {name for name, record in manifest['files'].items() if record['bundle'] == bundle}
        if seen != expected:
            raise ValueError(f'Evidence archive is incomplete: {bundle}')
    return [destination / name for name in sorted(wanted)]


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('prefix', nargs='*', default=['docs/benchmarks'], help='Original repository path or directory')
    parser.add_argument('--destination', type=Path, default=ROOT, help='Restore paths beneath this directory')
    parser.add_argument('--cache', type=Path, default=ROOT / 'target/benchmark-artifacts')
    args = parser.parse_args()
    paths = fetch(args.prefix, args.destination, args.cache)
    print(f'Verified {len(paths)} historical evidence files')


if __name__ == '__main__':
    main()
