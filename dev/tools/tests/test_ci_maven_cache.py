"""Dependency fan-out is offline and cannot publish incomplete or unsafe repositories."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
from unittest.mock import patch

from dev.tools.ci import maven_cache as cache


class MavenCacheTest(unittest.TestCase):
    def test_mediated_graph_fills_missing_winners_and_classified_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / 'm2'
            graph = root / 'target/ci-maven-graph.json'
            graph.parent.mkdir()
            (repository / 'test/parent/1').mkdir(parents=True)
            (repository / 'test/parent/1/parent-1.jar').touch()
            winner = {'groupId': 'test', 'artifactId': 'winner', 'version': '2', 'type': 'jar'}
            fixture = {'groupId': 'test', 'artifactId': 'fixture', 'version': '1', 'type': 'test-jar'}
            tree = {'groupId': 'tech.streamfusion', 'artifactId': 'reactor', 'version': '1',
                    'type': 'jar', 'children': [
                        {'groupId': 'test', 'artifactId': 'parent', 'version': '1', 'type': 'jar',
                         'children': [winner, fixture]}, winner]}
            calls = []

            def run(command, **kwargs):
                calls.append(command)
                if '--graph' in command:
                    graph.write_text(json.dumps(tree))
                else:
                    for relative in ('test/fixture/1/fixture-1-tests.jar', 'test/winner/2/winner-2.jar'):
                        path = repository / relative
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.touch()
                return subprocess.CompletedProcess(command, 0)

            with patch.object(cache, 'ROOT', root), patch.object(cache.subprocess, 'run', run):
                cache.complete_graph(repository, ['--model-only', '--profile', 'connector'])
            self.assertEqual(2, len(calls))
            self.assertIn('--graph', calls[0])
            self.assertNotIn('--model-only', calls[0])
            self.assertIn('--no-transitive', calls[1])
            self.assertEqual(['--artifact', 'test:fixture:1:jar:tests', '--artifact', 'test:winner:2:jar'],
                             calls[1][calls[1].index('--artifact'):])
            self.assertFalse(graph.exists())

    def test_standalone_fixture_graph_preserves_mediated_exclusions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / 'm2'
            graph = root / 'dev/flink-suite/hudi-fixtures/target/ci-maven-graph.json'
            artifact = repository / 'org/apache/hive/hive-metastore/2.3.1/hive-metastore-2.3.1.jar'
            calls = []

            def run(command, **kwargs):
                calls.append(command)
                if '--graph' in command:
                    graph.parent.mkdir(parents=True)
                    graph.write_text(json.dumps({'groupId': 'org.apache.hive',
                        'artifactId': 'hive-metastore', 'version': '2.3.1', 'type': 'jar'}))
                else:
                    artifact.parent.mkdir(parents=True)
                    artifact.touch()
                return subprocess.CompletedProcess(command, 0)

            with patch.object(cache, 'ROOT', root), patch.object(cache.subprocess, 'run', run):
                cache.complete_graph(repository, ['--model-only', '--pom', 'dev/flink-suite/hudi-fixtures/pom.xml'])
            self.assertEqual(2, len(calls))
            self.assertIn('--no-transitive', calls[1])
            self.assertNotIn('jdk.tools', ' '.join(calls[1]))
            self.assertFalse(graph.exists())

    def test_missing_mediated_graph_cannot_publish_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(cache, 'ROOT', root), patch.object(cache, 'recipes', return_value=[[]]), \
                    patch.object(cache.subprocess, 'run'):
                with self.assertRaisesRegex(ValueError, 'no external dependency graph'):
                    cache.prepare(root / 'm2', 'key', '17')
            self.assertFalse((root / 'm2' / cache.MARKER).exists())

    def test_failed_preparation_invalidates_completion_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            marker = repository / cache.MARKER
            marker.write_text(json.dumps(cache.metadata('old', '11')))
            with patch.object(cache.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'mvn')):
                with self.assertRaises(subprocess.CalledProcessError):
                    cache.prepare(repository, 'new', '11')
            self.assertFalse(marker.exists())

    def test_archive_checks_inputs_integrity_and_preserves_existing_repositories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            (source / cache.MARKER).write_text(json.dumps(cache.metadata('key', '11')))
            (source / 'library.jar').write_bytes(b'jar')
            (source / 'missing.jar.lastUpdated').touch()
            own = source / 'tech/streamfusion'
            own.mkdir(parents=True)
            (own / 'old-project.jar').touch()
            archive = root / 'm2.tar'
            cache.pack(source, archive, 'key', '11')
            with tarfile.open(archive) as contents:
                self.assertNotIn('missing.jar.lastUpdated', contents.getnames())
                self.assertNotIn('tech/streamfusion/old-project.jar', contents.getnames())
            for key, jdk in [('different', '11'), ('key', '17')]:
                with self.subTest(key=key, jdk=jdk), self.assertRaises(ValueError):
                    cache.restore(root / 'wrong', archive, key, jdk)
            self.assertFalse((root / 'wrong').exists())
            existing = root / 'existing'
            existing.mkdir()
            sentinel = existing / 'preserve.jar'
            sentinel.write_bytes(b'keep')
            with self.assertRaises(ValueError):
                cache.restore(existing, archive, 'key', '11')
            self.assertEqual(b'keep', sentinel.read_bytes())
            with archive.open('ab') as output:
                output.write(b'corrupt')
            with self.assertRaises(ValueError):
                cache.restore(root / 'corrupt', archive, 'key', '11')

    def test_archive_rejects_traversal_even_with_valid_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / 'unsafe.tar'
            with tarfile.open(archive, 'w') as output:
                for name, body in [(cache.MARKER, json.dumps(cache.metadata('key', '11')).encode()),
                                   ('../escaped.jar', b'escape')]:
                    entry = tarfile.TarInfo(name)
                    entry.size = len(body)
                    output.addfile(entry, io.BytesIO(body))
            archive.with_name(archive.name + '.sha256').write_text(cache.digest(archive))
            with self.assertRaises(ValueError):
                cache.restore(root / 'consumer', archive, 'key', '11')
            self.assertFalse((root / 'escaped.jar').exists())

    @unittest.skipUnless(shutil.which('mvn'), 'Maven is required for offline fan-out')
    def test_one_download_serves_multiple_offline_consumers(self):
        requests = []
        pom = (b'<project><modelVersion>4.0.0</modelVersion><groupId>test.cache</groupId>'
               b'<artifactId>parent</artifactId><version>1</version><packaging>pom</packaging></project>')

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                if self.path.endswith('.sha1'):
                    body = hashlib.sha1(pom).hexdigest().encode()
                elif self.path.endswith('parent-1.pom'):
                    body = pom
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                project = root / 'pom.xml'
                project.write_text('<project><modelVersion>4.0.0</modelVersion><parent>'
                    '<groupId>test.cache</groupId><artifactId>parent</artifactId><version>1</version>'
                    '<relativePath/></parent><artifactId>consumer</artifactId><repositories><repository>'
                    f'<id>central</id><url>http://127.0.0.1:{server.server_port}</url>'
                    '</repository></repositories></project>')
                settings = root / 'settings.xml'
                settings.write_text('<settings/>')

                def validate(repository, offline=False):
                    return subprocess.run(['mvn', '-B', '-ntp', *(['-o'] if offline else []),
                        '-s', str(settings), '-gs', str(settings), '-f', str(project),
                        '-Dmaven.repo.local=' + str(repository), 'validate'],
                        capture_output=True, text=True, timeout=30)

                source = root / 'producer'
                result = validate(source)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                downloads = len(requests)
                self.assertGreater(downloads, 0)
                (source / cache.MARKER).write_text(json.dumps(cache.metadata('key', '17')))
                archive = root / 'shared.tar'
                cache.pack(source, archive, 'key', '17')
                server.shutdown()
                for index in range(2):
                    repository = root / f'consumer-{index}'
                    cache.restore(repository, archive, 'key', '17')
                    result = validate(repository, offline=True)
                    self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual(downloads, len(requests))
                result = validate(root / 'empty', offline=True)
                self.assertNotEqual(0, result.returncode)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()
