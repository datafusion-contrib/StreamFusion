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

import ci_maven_cache as cache


class MavenCacheTest(unittest.TestCase):
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
