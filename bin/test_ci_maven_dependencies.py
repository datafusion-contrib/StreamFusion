"""Dependency setup can recover without ever invoking or retrying a test lifecycle."""

import argparse
import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("ci_maven_dependencies", ROOT / "bin/ci-maven-dependencies.py")
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


class MavenDependenciesTest(unittest.TestCase):
    def options(self):
        return argparse.Namespace(maven="mvn", profile="flink-1.18", projects="streamfusion-runtime",
                                  also_make=True, pom=Path("checkout with spaces/pom.xml"),
                                  repository=Path("repository with spaces"), artifact=[], model_only=False)

    def invoke(self, statuses):
        results = [subprocess.CompletedProcess([], status) if isinstance(status, int) else status
                   for status in statuses]
        with patch.object(PREPARE.subprocess, "run", side_effect=results) as run, \
                patch.object(PREPARE.time, "sleep") as sleep:
            status = PREPARE.resolve(self.options())
        return status, run.call_args_list, sleep.call_args_list

    def test_success_needs_no_retry_and_preserves_resolution_scope(self):
        status, calls, sleeps = self.invoke([0])
        self.assertEqual(0, status)
        self.assertFalse(sleeps)
        self.assertEqual(1, len(calls))
        command = calls[0].args[0]
        self.assertIn("-U", command)
        self.assertIn("-Pflink-1.18", command)
        self.assertIn("streamfusion-runtime", command)
        self.assertIn("-am", command)
        self.assertIn("checkout with spaces/pom.xml", command)
        self.assertIn("-Dmaven.repo.local=repository with spaces", command)
        self.assertEqual(PREPARE.GOAL, command[-1])
        self.assertFalse({"test", "verify", "package", "install"} & set(command))
        for option in PREPARE.TRANSPORT_OPTIONS:
            self.assertIn(option, command)

    def test_project_models_retry_without_building_or_resolving_reactor_artifacts(self):
        options = self.options()
        options.model_only = True
        with patch.object(PREPARE.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], status) for status in (1, 0)]) as run, \
                patch.object(PREPARE.time, "sleep"):
            self.assertEqual(0, PREPARE.resolve(options))
        command = run.call_args_list[0].args[0]
        self.assertEqual(command, run.call_args_list[1].args[0])
        self.assertIn(PREPARE.MODEL_GOAL, command)
        self.assertIn("-Dexpression=project.id", command)
        self.assertIn("streamfusion-runtime", command)
        self.assertIn("-am", command)
        self.assertNotIn(PREPARE.GOAL, command)
        self.assertFalse({"test", "verify", "package", "install"} & set(command))

    def test_missing_download_is_rechecked_and_can_recover(self):
        status, calls, sleeps = self.invoke([1, 1, 0])
        self.assertEqual(0, status)
        self.assertEqual(3, len(calls))
        self.assertTrue(all(call.args[0] == calls[0].args[0] for call in calls))
        self.assertEqual([5, 15], [call.args[0] for call in sleeps])

    def test_persistent_failure_and_timeout_remain_blocking(self):
        for statuses, expected in (([7, 7, 7], 7),
                                   ([subprocess.TimeoutExpired("mvn", 600)] * 3, 124)):
            with self.subTest(expected=expected):
                status, calls, sleeps = self.invoke(statuses)
                self.assertEqual(expected, status)
                self.assertEqual(3, len(calls))
                self.assertTrue(all(call.kwargs["timeout"] == 600 for call in calls))
                self.assertEqual(2, len(sleeps))

    def test_test_goals_cannot_be_passed_to_the_retry_stage(self):
        result = subprocess.run(["python3", str(ROOT / "bin/ci-maven-dependencies.py"), "test"],
                                capture_output=True, text=True)
        self.assertEqual(2, result.returncode)
        self.assertIn("unrecognized arguments", result.stderr)

    def test_runtime_graphs_resolve_separately_and_retry_only_the_failed_download(self):
        options = self.options()
        options.artifact = ["org.apache.maven.surefire:surefire-junit-platform:3.2.2",
                            "org.junit.platform:junit-platform-launcher:1.11.4"]
        with patch.object(PREPARE.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], status) for status in (0, 1, 0)]) as run, \
                patch.object(PREPARE.time, "sleep") as sleep:
            self.assertEqual(0, PREPARE.resolve(options))
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn("-Dartifact=" + options.artifact[0], commands[0])
        self.assertIn("-Dartifact=" + options.artifact[1], commands[1])
        self.assertEqual(commands[1], commands[2])
        self.assertEqual([5], [call.args[0] for call in sleep.call_args_list])
        for command in commands:
            self.assertIn(PREPARE.ARTIFACT_GOAL, command)
            self.assertIn("-Dtransitive=true", command)
            self.assertEqual(str(PREPARE.ARTIFACT_POM), command[command.index("-f") + 1])
            self.assertNotIn("-pl", command)
            self.assertNotIn("-am", command)
            self.assertFalse(any(arg.startswith("-P") for arg in command))
            self.assertFalse({"test", "verify", "package", "install"} & set(command))

    def test_failed_runtime_graph_blocks_later_downloads(self):
        options = self.options()
        options.artifact = ["group:provider:1", "group:launcher:1"]
        with patch.object(PREPARE.subprocess, "run", return_value=subprocess.CompletedProcess([], 7)) as run, \
                patch.object(PREPARE.time, "sleep"):
            self.assertEqual(7, PREPARE.resolve(options))
        self.assertEqual(3, run.call_count)
        self.assertTrue(all("-Dartifact=group:provider:1" in call.args[0] for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
