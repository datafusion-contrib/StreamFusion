"""Keep failed-process evidence, paired order and child CPU accounting intact."""

from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dev.tools.profiling import process


class ProfileProcessTest(unittest.TestCase):
    def test_failure_retains_output_and_reports_child_cpu_separately_from_elapsed_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "failure.log"
            with patch.object(process.time, "monotonic", side_effect=[10.0, 14.0]), \
                    patch.object(process.resource, "getrusage", side_effect=[
                        SimpleNamespace(ru_utime=2.0, ru_stime=1.0),
                        SimpleNamespace(ru_utime=3.5, ru_stime=1.5)]):
                result, timing = process.run_logged(
                    [sys.executable, "-c", "import sys; print('retained evidence'); sys.exit(23)"],
                    root=root, log=log, environment=None)
            self.assertEqual(23, result.returncode)
            self.assertEqual("retained evidence\n", log.read_text())
            self.assertEqual({"elapsed_seconds": 4.0, "cpu_seconds": 2.0}, timing)

    def test_trial_order_alternates_without_mutating_the_requested_modes(self):
        modes = ["stock", "candidate", "reference"]
        self.assertEqual(("stock", "candidate", "reference"), process.alternating(modes, 0))
        self.assertEqual(("reference", "candidate", "stock"), process.alternating(modes, 1))
        self.assertEqual(("stock", "candidate", "reference"), process.alternating(modes, 2))
        self.assertEqual(["stock", "candidate", "reference"], modes)


if __name__ == "__main__":
    unittest.main()
