"""Keep new developer-tool tests selected and reject vacuous successful discovery."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dev.tools.tests.__main__ import cases, selection


class ToolDiscoveryTest(unittest.TestCase):
    def test_every_tool_test_module_is_selected(self):
        directory = Path(__file__).parent
        selected = {case.__class__.__module__.rsplit(".", 1)[-1] for case in cases(selection(directory))}
        self.assertEqual({path.stem for path in directory.glob("test_*.py")}, selected)

    def test_empty_selections_and_unknown_exclusions_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "selection is empty"):
                selection(Path(temporary))
        with self.assertRaisesRegex(ValueError, "Unknown.*missing.py"):
            selection(Path(__file__).parent, ["missing.py"])

    def test_a_test_file_without_cases_cannot_silently_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "test_empty.py").touch()
            with patch.object(unittest.TestLoader, "loadTestsFromName", return_value=unittest.TestSuite()):
                with self.assertRaisesRegex(ValueError, "has no test cases"):
                    selection(directory)

    def test_workflow_exclusions_keep_the_previously_unselected_validation_tests(self):
        root = Path(__file__).resolve().parents[3]
        workflow = (root / ".github/workflows/ci-test-suites.yml").read_text()
        command = workflow.split("python3 -m dev.tools.tests", 1)[1].split("\n\n", 1)[0]
        excludes = [row.strip().removeprefix("--exclude ").rstrip("\\").strip()
                    for row in command.splitlines() if row.strip().startswith("--exclude ")]
        selected = {case.__class__.__module__.rsplit(".", 1)[-1]
                    for case in cases(selection(Path(__file__).parent, excludes))}
        self.assertTrue({"test_bench_hudi_java", "test_bench_hudi_matrix",
                         "test_bench_jvm_aggregates", "test_benchmark_test_sources"} <= selected)


if __name__ == "__main__":
    unittest.main()
