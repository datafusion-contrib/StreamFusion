"""A documentation fast path must not conceal source changes or incomplete baseline runs."""

import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("ci_plan", Path(__file__).with_name("ci-plan.py"))
plan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan)


class CiPlanTest(unittest.TestCase):
    def test_documentation_whitelist_excludes_build_config_and_other_data(self):
        self.assertTrue(plan.documentation_only(["readme.md", "docs/operators/over.md",
                                                 "docs/benchmarks/ci-profile-20261107.json"]))
        for name in ("pom.xml", "CLAUDE.md", ".github/workflows/ci.yml", "bin/ci-plan.py",
                     "docs/benchmarks/input.json", "docs/hook.py", "src/Foo.java"):
            self.assertFalse(plan.documentation_only(["docs/ci.md", name]), name)
        self.assertFalse(plan.documentation_only([]))

    def test_real_git_diff_preserves_deletions_and_renames_out_of_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()
            git("init", "-q")
            git("config", "user.name", "CI fixture")
            git("config", "user.email", "ci@example.invalid")
            (root / "src").mkdir()
            (root / "docs").mkdir()
            (root / "src/Engine.java").write_text("class Engine {}\n")
            (root / "docs/ci.md").write_text("before\n")
            git("add", ".")
            git("commit", "-qm", "fixture")
            before = git("rev-parse", "HEAD")
            (root / "docs/ci.md").write_text("after\n")
            git("commit", "-qam", "docs")
            self.assertTrue(plan.documentation_only(plan.changed_paths(before, "HEAD", cwd=root)))
            (root / "src/Engine.java").rename(root / "docs/Engine.md")
            git("add", "-A")
            git("commit", "-qm", "move source")
            actual = plan.changed_paths(before, "HEAD", cwd=root)
            self.assertIn("src/Engine.java", actual)
            self.assertFalse(plan.documentation_only(actual))

    def test_only_latest_completed_push_on_exact_sha_and_branch_is_coverage(self):
        good = {"id": 1, "head_sha": "sha", "head_branch": "feature", "event": "push",
                "status": "completed", "conclusion": "success"}
        self.assertTrue(plan.proven_push([good], "sha", "feature"))
        for change in ({"head_sha": "old"}, {"head_branch": "other"},
                       {"event": "workflow_dispatch"}, {"status": "in_progress", "conclusion": None},
                       {"conclusion": "failure"}, {"conclusion": "cancelled"}, {"conclusion": "skipped"}):
            self.assertFalse(plan.proven_push([{**good, **change}], "sha", "feature"))
        self.assertFalse(plan.proven_push([good, {**good, "id": 2, "conclusion": "failure"}], "sha", "feature"))

    def test_both_required_workflows_must_have_proven_pushes(self):
        sha = "a" * 40
        good = {"id": 1, "head_sha": sha, "head_branch": "feature", "event": "push",
                "status": "completed", "conclusion": "success"}
        for second in ([good], [], [{**good, "conclusion": "failure"}]):
            with patch.object(plan.subprocess, "run"), patch.object(plan, "changed_paths", return_value=["docs/ci.md"]), \
                    patch.object(plan, "api", side_effect=[{"workflow_runs": [good]}, {"workflow_runs": second}]):
                tests, base, reason = plan.plan("owner/repo", "b" * 40, sha, "feature")
                self.assertEqual(not bool(second and second[0]["conclusion"] == "success"), tests)
                self.assertEqual(sha, base)

    def test_network_failure_requires_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / "event.json"
            event.write_text(json.dumps({"before": "a" * 40}))
            env = {"GITHUB_EVENT_NAME": "push", "GITHUB_EVENT_PATH": str(event),
                   "GITHUB_REPOSITORY": "owner/repo", "GITHUB_SHA": "b" * 40,
                   "GITHUB_REF_NAME": "feature", "GITHUB_STEP_SUMMARY": ""}
            with patch.dict(os.environ, env), patch.object(plan, "plan", side_effect=OSError("offline")), \
                    patch("sys.stdout", new_callable=io.StringIO) as output:
                plan.main()
                self.assertIn("tests=true", output.getvalue())

    def test_manual_dispatch_always_requires_tests(self):
        with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_STEP_SUMMARY": ""}), \
                patch.object(plan, "plan") as decision, patch("sys.stdout", new_callable=io.StringIO) as output:
            plan.main()
            decision.assert_not_called()
            self.assertIn("tests=true", output.getvalue())

    def test_new_branches_require_both_canonical_main_push_results(self):
        sha = "a" * 40
        good = {"id": 1, "head_sha": sha, "head_branch": "main", "event": "push",
                "status": "completed", "conclusion": "success"}
        with patch.object(plan.subprocess, "run") as fetch, \
                patch.object(plan, "changed_paths", return_value=["docs/ci.md"]), \
                patch.object(plan, "api", side_effect=[{"sha": sha}, {"workflow_runs": [good]},
                                                       {"workflow_runs": [good]}]) as api:
            tests, base, reason = plan.plan("owner/fork", "b" * 40, "0" * 40, "new")
            self.assertFalse(tests)
            self.assertEqual(sha, base)
            self.assertEqual(3, api.call_count)
            self.assertIn("https://github.com/datafusion-contrib/StreamFusion.git", fetch.call_args.args[0])

    def test_aggregate_scripts_reject_unexpected_skips_failures_and_unproven_plans(self):
        root = Path(__file__).resolve().parents[1]
        for filename, step in (("ci.yml", "Require every test job to succeed"),
                               ("flink-suite.yml", "Require every upstream suite to succeed")):
            lines = (root / ".github/workflows" / filename).read_text().splitlines()
            start = lines.index("      - name: " + step)
            start = lines.index("        run: |", start) + 1
            end = start
            while end < len(lines) and (not lines[end] or lines[end].startswith("          ")):
                end += 1
            script = textwrap.dedent("\n".join(lines[start:end]))
            for docs in (False, True):
                for plan_result in ("success", "failure", "skipped"):
                    for tests_result in ("success", "failure", "skipped", "cancelled"):
                        with self.subTest(file=filename, docs=docs, plan=plan_result, tests=tests_result):
                            needs = {"plan": {"result": plan_result}, "tests": {"result": tests_result}}
                            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True,
                                                    env={**os.environ, "NEEDS_JSON": json.dumps(needs),
                                                         "DOCS_ONLY": str(docs).lower(), "INVENTORY": "false"})
                            expected = plan_result == "success" and tests_result == ("skipped" if docs else "success")
                            self.assertEqual(expected, result.returncode == 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
