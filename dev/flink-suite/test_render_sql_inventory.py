import csv
import gzip
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RENDER = Path(__file__).with_name('render_sql_inventory.py')


class RenderSqlInventoryTest(unittest.TestCase):
    def fixture(self, root):
        row = dict(flink_line='2.2', test_class='org.apache.flink.TestITCase', test_name='test',
                   display_name='</script><script>alert(1)</script>\ude00', outcome='passed',
                   label='should be accelerated', category='window', features='window',
                   note='Unsupported window', invocation_id='invocation', native_plans=0, host_plans=1)
        data = dict(schema_version=1, tests=[row], summary=dict(revision='revision', cases=1,
                    outcomes={'passed': 1}, labels_passed={'should be accelerated': 1}))
        path = root / 'inventory.json'
        path.write_text(json.dumps(data))
        return path, row

    def test_report_escapes_html_and_preserves_downloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, row = self.fixture(root)
            output = root / 'report'
            subprocess.run([sys.executable, str(RENDER), '--inventory', str(path), '--output', str(output)], check=True)
            html = (output / 'report.html').read_text()
            self.assertNotIn('</script><script>alert(1)', html)
            self.assertIn('\\u003c/script>', html)
            raw = json.loads(gzip.decompress((output / 'flink-2.2.json.gz').read_bytes()))
            self.assertEqual(row['display_name'], raw['tests'][0]['display_name'])
            with (output / 'flink-2.2.csv').open() as stream:
                exported = next(csv.DictReader(stream))
            self.assertTrue(exported['display_name'].endswith('\\ude00'))
            self.assertIn('| [2.2](flink-2.2.csv) | 1 | 0 | 1 | 0 |', (output / 'index.md').read_text())

    def test_duplicate_or_incomplete_inputs_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, _ = self.fixture(root)
            command = [sys.executable, str(RENDER), '--inventory', str(path), '--output', str(root / 'report')]
            result = subprocess.run(command + ['--inventory', str(path)], capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn('Duplicate Flink line', result.stderr)
            data = json.loads(path.read_text())
            data['summary']['cases'] = 2
            path.write_text(json.dumps(data))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn('Incomplete inventory', result.stderr)
