import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from benchmark_artifacts import fetch


class BenchmarkArtifactsTest(unittest.TestCase):
    def fixture(self, root, members=None):
        payloads = {'docs/benchmarks/one/evidence.tar.gz': b'first archive',
                    'docs/benchmarks/two/profile.cpu.gz': b'profile bytes'}
        archive = root / 'remote.tar.gz'
        with tarfile.open(archive, 'w:gz') as output:
            for name, data in (members if members is not None else payloads.items()):
                if isinstance(data, tarfile.TarInfo):
                    output.addfile(data)
                else:
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    output.addfile(info, io.BytesIO(data))
        record = lambda data: dict(size=len(data), sha256=hashlib.sha256(data).hexdigest())
        manifest = dict(schema_version=1,
                        bundles={'run': dict(url=archive.as_uri(), **record(archive.read_bytes()))},
                        files={name: dict(bundle='run', **record(data)) for name, data in payloads.items()})
        path = root / 'artifacts.json'
        path.write_text(json.dumps(manifest))
        return path, manifest, payloads, archive

    def test_prefix_restoration_and_verified_offline_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _, payloads, archive = self.fixture(root)
            destination, cache = root / 'out', root / 'cache'
            restored = fetch(['docs/benchmarks/one/'], destination, cache, manifest)
            self.assertEqual([destination.resolve() / 'docs/benchmarks/one/evidence.tar.gz'], restored)
            self.assertFalse((destination / 'docs/benchmarks/two').exists())
            archive.unlink()
            restored = fetch(['docs/benchmarks'], destination, cache, manifest)
            self.assertEqual(2, len(restored))
            for name, data in payloads.items():
                self.assertEqual(data, (destination / name).read_bytes())

    def test_changed_local_evidence_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _, _, _ = self.fixture(root)
            path = root / 'out/docs/benchmarks/one/evidence.tar.gz'
            path.parent.mkdir(parents=True)
            path.write_bytes(b'local investigation')
            with self.assertRaisesRegex(ValueError, 'Refusing to replace changed'):
                fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)
            self.assertEqual(b'local investigation', path.read_bytes())

    def test_corrupt_cache_is_refetched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, data, _, _ = self.fixture(root)
            cache = root / 'cache'
            cache.mkdir()
            cached = cache / (data['bundles']['run']['sha256'] + '.tar.gz')
            cached.write_bytes(b'corrupt')
            fetch(['docs/benchmarks'], root / 'out', cache, manifest)
            self.assertEqual(data['bundles']['run']['sha256'], hashlib.sha256(cached.read_bytes()).hexdigest())

    def test_transient_network_failure_is_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _, _, _ = self.fixture(root)
            original = urllib.request.urlopen
            attempts = []

            def request(*args, **kwargs):
                attempts.append(1)
                if len(attempts) == 1:
                    raise urllib.error.URLError('temporary connection failure')
                return original(*args, **kwargs)

            with patch('benchmark_artifacts.urllib.request.urlopen', side_effect=request), \
                    patch('benchmark_artifacts.time.sleep'):
                restored = fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)
            self.assertEqual(2, len(restored))
            self.assertEqual(2, len(attempts))

    def test_bundle_and_file_corruption_fail(self):
        for corrupt in ('bundle', 'file', 'size'):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest, data, _, _ = self.fixture(root)
                if corrupt == 'bundle':
                    data['bundles']['run']['sha256'] = '0' * 64
                elif corrupt == 'file':
                    data['files']['docs/benchmarks/one/evidence.tar.gz']['sha256'] = '0' * 64
                else:
                    data['bundles']['run']['size'] = 1
                manifest.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, 'mismatch|exceeds'):
                    fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)
                self.assertFalse((root / 'out/docs/benchmarks/one/evidence.tar.gz').exists())

    def test_unsafe_unknown_duplicate_and_missing_members_are_rejected(self):
        for name in ('../escaped', '/absolute', 'docs/benchmarks/../../escaped', 'docs/benchmarks/unknown',
                     'docs/benchmarks/one/evidence.tar.gz', 'missing'):
            with self.subTest(member=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                original = [('docs/benchmarks/one/evidence.tar.gz', b'first archive')]
                members = original if name == 'missing' else original + [(name, b'first archive')]
                manifest, _, _, _ = self.fixture(root, members)
                with self.assertRaisesRegex(ValueError, 'Invalid|Unexpected|incomplete'):
                    fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)
                self.assertFalse((root / 'escaped').exists())

    def test_archive_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            name = 'docs/benchmarks/one/evidence.tar.gz'
            link = tarfile.TarInfo(name)
            link.type = tarfile.SYMTYPE
            link.linkname = '/tmp/elsewhere'
            manifest, _, _, _ = self.fixture(root, [(name, link)])
            with self.assertRaisesRegex(ValueError, 'Unexpected'):
                fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)

    def test_destination_symlinks_cannot_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _, _, _ = self.fixture(root)
            destination = root / 'out'
            destination.mkdir()
            outside = root / 'outside'
            outside.mkdir()
            (destination / 'docs').symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'escapes'):
                fetch(['docs/benchmarks'], destination, root / 'cache', manifest)
            self.assertFalse(list(outside.iterdir()))

    def test_missing_prefix_and_invalid_manifest_paths_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, data, _, _ = self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'No archived evidence'):
                fetch(['docs/benchmarks/missing'], root / 'out', root / 'cache', manifest)
            data['files']['../escaped'] = next(iter(data['files'].values()))
            manifest.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'Invalid evidence path'):
                fetch(['docs/benchmarks'], root / 'out', root / 'cache', manifest)
