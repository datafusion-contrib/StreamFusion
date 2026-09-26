"""Keep upstream test artifacts and native witnesses isolated across Flink lines."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[2] / "bin/flink-suite.sh"


class FlinkLineSelectionTest(unittest.TestCase):
    def invoke(self, root, version, mode="config"):
        return subprocess.run(
            ["bash", str(RUNNER), mode],
            env={**os.environ, "FLINK_SUITE_ROOT": str(root), "FLINK_VERSION": version},
            capture_output=True,
            text=True,
        )

    def test_every_mutable_output_is_isolated_by_line(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "suite with spaces"
            configurations = []
            for version, line, kafka, paimon in (
                ("2.2.1", "2.2", "5.0.0", "flink2"),
                ("1.18.1", "1.18", "3.2.0", "flink1"),
            ):
                result = self.invoke(root, version)
                self.assertEqual(0, result.returncode, result.stderr)
                config = dict(row.split("=", 1) for row in result.stdout.splitlines())
                self.assertEqual(kafka, config["kafka.version"])
                self.assertEqual(paimon, config["paimon.profile"])
                for key in ("suite.root", "streamfusion.source", "maven.repo", "agent.jar", "classpath"):
                    path = Path(config[key])
                    self.assertTrue(path == root / line or root / line in path.parents, key)
                self.assertTrue(Path(config["contracts"]).is_file())
                configurations.append(config)
            for key in ("suite.root", "streamfusion.source", "maven.repo", "agent.jar", "classpath", "contracts"):
                self.assertNotEqual(configurations[0][key], configurations[1][key], key)
            self.assertFalse(root.exists(), "inspecting configuration must not clone or build")

    def test_unavailable_line_and_delta_fail_before_building(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "suite"
            for version, mode, diagnostic in (
                ("1.19.0", "config", "Unsupported Flink suite version"),
                ("1.18.1", "delta", "Delta acceleration is not admitted"),
            ):
                result = self.invoke(root, version, mode)
                self.assertEqual(2, result.returncode)
                self.assertIn(diagnostic, result.stderr)
                self.assertFalse(root.exists())

    def test_invalid_shard_selection_fails_before_building(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "suite"
            for mode, shard, selector in (("runtime", "0", ""), ("state", "1", ""),
                                          ("runtime", "1", "CalcITCase")):
                result = subprocess.run(
                    ["bash", str(RUNNER), mode], capture_output=True, text=True,
                    env={**os.environ, "FLINK_SUITE_ROOT": str(root), "FLINK_SUITE_SHARD": shard,
                         "FLINK_SUITE_TEST": selector},
                )
                self.assertEqual(2, result.returncode, result.stderr)
                self.assertIn("FLINK_SUITE_SHARD", result.stderr)
                self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
