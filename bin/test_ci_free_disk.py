import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('ci_free_disk', Path(__file__).with_name('ci-free-disk.py'))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


class FreeDiskTest(unittest.TestCase):
    def run_cleanup(self, free_space):
        return patch.object(cleanup.shutil, 'disk_usage',
                            side_effect=[SimpleNamespace(free=gib * cleanup.GIB) for gib in free_space])

    def test_sufficient_space_does_not_delete_anything(self):
        with self.run_cleanup([30, 30]), patch.object(cleanup.subprocess, 'run') as run:
            cleanup.reclaim(30, Path('/workspace'))
            run.assert_not_called()

    def test_stops_after_reclaiming_enough_space(self):
        with self.run_cleanup([12, 35, 35]), patch.object(Path, 'exists', return_value=True), \
                patch.object(cleanup.subprocess, 'run') as run:
            cleanup.reclaim(30, Path('/workspace'))
            run.assert_called_once_with(['sudo', 'rm', '-rf', '--', '/usr/local/lib/android'], check=True)

    def test_missing_toolchains_and_insufficient_space(self):
        with self.run_cleanup([12] * 5), patch.object(Path, 'exists', return_value=False), \
                patch.object(cleanup.subprocess, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'Insufficient'):
                cleanup.reclaim(30, Path('/workspace'))
            run.assert_not_called()

    def test_refuses_local_or_self_hosted_cleanup(self):
        for environment in ({}, {'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'self-hosted',
                                 'RUNNER_OS': 'Linux'}):
            with patch.dict(cleanup.os.environ, environment, clear=True), \
                    patch('sys.argv', ['ci-free-disk.py']), patch.object(cleanup, 'reclaim') as reclaim:
                with self.assertRaises(SystemExit):
                    cleanup.main()
                reclaim.assert_not_called()


if __name__ == '__main__':
    unittest.main()
