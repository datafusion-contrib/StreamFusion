"""Dependency failures must stop the compatibility lane before its native build."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


def workflow_commands():
    workflow = (ROOT / '.github/workflows/paimon1.yml').read_text()
    names = ('Prepare legacy Maven dependencies before native compilation',
             'Build release native libraries once for the complete reactor')
    commands = []
    for name in names:
        block = re.search(r'^      - name: ' + re.escape(name)
                          + r'\n(.*?)(?=^      - |\Z)', workflow, re.MULTILINE | re.DOTALL)
        if block is None:
            raise AssertionError(f'Missing compatibility step: {name}')
        lines = block.group(1).splitlines()
        index = next(i for i, line in enumerate(lines) if line.startswith('        run: '))
        command = lines[index].split('run: ', 1)[1]
        if command == '>-':
            parts = []
            for line in lines[index + 1:]:
                if not line.startswith('          '):
                    break
                parts.append(line[10:])
            command = ' '.join(parts)
        commands.append((block.start(), command))
    return [command for position, command in sorted(commands)]


class PaimonDependencySetupTest(unittest.TestCase):
    def test_resolution_failure_prevents_native_compilation(self):
        for preparation_status in (0, 7):
            with self.subTest(preparation_status=preparation_status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, script in {
                    'python3': ('printf "prepare\\n" >> "$CI_WORKFLOW_CALLS"\n'
                                'printf "%s\\n" "$@" > "$CI_DEPENDENCY_ARGS"\n'
                                'exit "$CI_PREPARE_EXIT"\n'),
                    'cargo': 'printf "native\\n" >> "$CI_WORKFLOW_CALLS"\n',
                }.items():
                    executable = root / name
                    executable.write_text('#!/bin/sh\n' + script)
                    executable.chmod(0o755)
                calls = root / 'calls'
                arguments = root / 'arguments'
                environment = {**os.environ, 'PATH': str(root),
                               'CI_WORKFLOW_CALLS': str(calls),
                               'CI_DEPENDENCY_ARGS': str(arguments),
                               'CI_PREPARE_EXIT': str(preparation_status)}
                for command in workflow_commands():
                    result = subprocess.run(['/bin/bash', '-e', '-o', 'pipefail', '-c', command],
                                            cwd=root, env=environment, capture_output=True, text=True)
                    if result.returncode:
                        break
                self.assertEqual(preparation_status, result.returncode, result.stderr)
                self.assertEqual(['prepare', 'native'] if preparation_status == 0 else ['prepare'],
                                 calls.read_text().splitlines())
                options = arguments.read_text().splitlines()
                self.assertEqual('bin/ci-maven-dependencies.py', options[0])
                profiles = options[options.index('--profile') + 1].split(',')
                self.assertEqual({'flink-1.18', 'paimon1', 'bench', 'tpcds'}, set(profiles))
                self.assertEqual('streamfusion-paimon1', options[options.index('--projects') + 1])
                self.assertIn('--also-make', options)


if __name__ == '__main__':
    unittest.main()
