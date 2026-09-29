"""Keep tagged tests covered and preserve failures when package checks are grouped."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/ci.yml'


class ModuleGroupsTest(unittest.TestCase):
    def test_every_module_tag_in_the_test_sources_is_selected(self):
        workflow = WORKFLOW.read_text()
        expressions = re.findall(r'^            tags: (.+)$', workflow, re.MULTILINE)
        selected = [tag.strip() for expression in expressions for tag in expression.split('|')]
        declared = {tag for source in (ROOT / 'src/test').rglob('*.java')
                    for tag in re.findall(r'@Tag\("(streamfusion-[^\"]+)"\)', source.read_text())}
        self.assertTrue(declared)
        self.assertEqual(declared, set(selected))
        self.assertEqual(len(selected), len(set(selected)))
        packages = set(' '.join(re.findall(r'^            packages: (.+)$', workflow, re.MULTILINE)).split())
        expected = {'streamfusion-avro' if tag == 'streamfusion-avro-confluent' else tag for tag in declared}
        self.assertEqual(expected, packages)

    def test_standalone_checks_continue_and_fail_the_group_if_any_package_fails(self):
        lines = WORKFLOW.read_text().splitlines()
        start = lines.index('      - name: Check every standalone native package')
        start = lines.index('        run: |', start) + 1
        end = start
        while end < len(lines) and (not lines[end] or lines[end].startswith('          ')):
            end += 1
        script = textwrap.dedent('\n'.join(lines[start:end]))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'native').mkdir()
            cargo = root / 'cargo'
            cargo.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$CALLS"\n[[ "$3" != "$FAIL_PACKAGE" ]]\n')
            cargo.chmod(0o755)
            for failing in ('', 'a', 'b', 'c'):
                calls = root / 'calls'
                calls.unlink(missing_ok=True)
                result = subprocess.run(['bash', '-e', '-c', script], cwd=root, capture_output=True,
                                        env={**os.environ, 'PATH': f'{root}:{os.environ["PATH"]}',
                                             'CALLS': str(calls), 'FAIL_PACKAGE': failing,
                                             'NATIVE_PACKAGES': 'a b c'})
                self.assertEqual(bool(failing), result.returncode != 0, result.stderr)
                self.assertEqual(['check -p a --all-targets', 'check -p b --all-targets',
                                  'check -p c --all-targets'], calls.read_text().splitlines())


if __name__ == '__main__':
    unittest.main()
