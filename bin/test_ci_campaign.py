"""Campaign submission must not duplicate work or qualify superseded candidates."""

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("ci_campaign", Path(__file__).with_name("ci-campaign.py"))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)
SHA = "a" * 40
REPO = "owner/fork"
BRANCH = "feature/change"


def run(workflow="ci.yml", *, status="completed", conclusion="success", run_id=1, sha=SHA):
    return {"id": run_id, "head_sha": sha, "head_branch": BRANCH,
            "path": ".github/workflows/" + workflow, "status": status,
            "conclusion": conclusion, "event": "workflow_dispatch",
            "display_title": "Full upstream Flink suite (merge gate)",
            "html_url": f"https://github.com/{REPO}/actions/runs/{run_id}"}


class CampaignTest(unittest.TestCase):
    def setUp(self):
        base = patch.object(campaign, "require_current_base")
        self.base = base.start()
        self.addCleanup(base.stop)

    def test_real_git_readiness_rejects_uncommitted_and_unpushed_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.name", "CI fixture")
            git("config", "user.email", "ci@example.invalid")
            (root / "source.txt").write_text("candidate")
            git("add", ".")
            git("commit", "-qm", "ready")
            sha = git("rev-parse", "HEAD")
            with patch.object(campaign, "command", side_effect=lambda *args: subprocess.check_output(args, cwd=root, text=True).strip()), \
                    patch.object(campaign, "api", side_effect=[{"object": {"sha": sha}},
                        {"fork": True, "source": {"full_name": campaign.CANONICAL}}]), \
                    patch.object(campaign, "pages", return_value=iter([])):
                self.assertEqual(sha, campaign.candidate(REPO, BRANCH))
            with patch.object(campaign, "command", side_effect=lambda *args: subprocess.check_output(args, cwd=root, text=True).strip()), \
                    patch.object(campaign, "api", return_value={"object": {"sha": "b" * 40}}):
                with self.assertRaisesRegex(ValueError, "not the pushed branch tip"):
                    campaign.candidate(REPO, BRANCH)
            (root / "uncommitted.java").write_text("not ready")
            with patch.object(campaign, "command", side_effect=lambda *args: subprocess.check_output(args, cwd=root, text=True).strip()), \
                    patch.object(campaign, "api") as api:
                with self.assertRaisesRegex(ValueError, "Commit the complete"):
                    campaign.candidate(REPO, BRANCH)
                api.assert_not_called()

    def test_submission_lock_is_shared_and_released_after_failure(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(campaign, "command", return_value=directory):
            with self.assertRaisesRegex(RuntimeError, "submission failure"):
                with campaign.campaign_lock():
                    with self.assertRaisesRegex(ValueError, "Another local agent"):
                        with campaign.campaign_lock():
                            self.fail("A second submitter acquired the same lock")
                    raise RuntimeError("submission failure")
            with campaign.campaign_lock():
                self.assertTrue((Path(directory) / "streamfusion-ci-campaign.lock").exists())

    def test_green_and_running_exact_head_workflows_are_reused_without_mutation(self):
        with patch.object(campaign, "candidate", return_value=SHA), \
                patch.object(campaign, "latest_run", side_effect=[run(), run("flink-suite.yml", status="in_progress", conclusion=None)]), \
                patch.object(campaign, "ensure_capacity") as capacity, patch.object(campaign, "api") as api:
            campaign.qualify(REPO, BRANCH, campaign.REQUIRED, start=True, validation="focused tests passed")
            capacity.assert_not_called()
            api.assert_not_called()
            self.base.assert_not_called()

    def test_unsuccessful_head_requires_targeted_retry_not_another_dispatch(self):
        for conclusion in ("failure", "cancelled", "timed_out"):
            with self.subTest(conclusion=conclusion), patch.object(campaign, "candidate", return_value=SHA), \
                    patch.object(campaign, "latest_run", return_value=run(conclusion=conclusion)), \
                    patch.object(campaign, "api") as api:
                with self.assertRaisesRegex(ValueError, "do not restart the matrix"):
                    campaign.qualify(REPO, BRANCH, campaign.REQUIRED, start=True, validation="checks passed")
                api.assert_not_called()

    def test_only_missing_workflow_is_dispatched_after_capacity_and_head_checks(self):
        with patch.object(campaign, "candidate", return_value=SHA) as head, \
                patch.object(campaign, "latest_run", side_effect=[run(), None]), \
                patch.object(campaign, "ensure_capacity") as capacity, \
                patch.object(campaign, "wait_for_dispatch", return_value=run("flink-suite.yml", run_id=2)), \
                patch.object(campaign, "api") as api:
            campaign.qualify(REPO, BRANCH, campaign.REQUIRED, start=True, validation="local regression: 8 passed")
            capacity.assert_called_once()
            self.assertEqual(2, head.call_count)
            api.assert_called_once_with(f"repos/{REPO}/actions/workflows/flink-suite.yml/dispatches",
                                        data={"ref": BRANCH})

    def test_busy_account_or_head_change_cannot_submit_a_new_run(self):
        with patch.object(campaign, "candidate", return_value=SHA), \
                patch.object(campaign, "latest_run", return_value=None), \
                patch.object(campaign, "ensure_capacity", side_effect=ValueError("busy")), \
                patch.object(campaign, "api") as api:
            with self.assertRaisesRegex(ValueError, "busy"):
                campaign.qualify(REPO, BRANCH, campaign.REQUIRED, start=True, validation="tests passed")
            api.assert_not_called()
        with patch.object(campaign, "candidate", side_effect=[SHA, "b" * 40]), \
                patch.object(campaign, "latest_run", return_value=None), \
                patch.object(campaign, "ensure_capacity"), patch.object(campaign, "api") as api:
            with self.assertRaisesRegex(ValueError, "head changed"):
                campaign.qualify(REPO, BRANCH, campaign.REQUIRED, start=True, validation="tests passed")
            api.assert_not_called()

    def test_retry_rejects_old_heads_and_newer_runs_and_uses_failed_jobs_endpoint(self):
        with patch.object(campaign, "candidate", return_value=SHA), \
                patch.object(campaign, "api", return_value=run(sha="b" * 40)) as api:
            with self.assertRaisesRegex(ValueError, "superseded"):
                campaign.retry(REPO, BRANCH, 1, "network failure; regression passed")
            self.assertEqual(1, api.call_count)
        failed = run(conclusion="failure")
        with patch.object(campaign, "candidate", return_value=SHA), \
                patch.object(campaign, "api", return_value=failed) as api, \
                patch.object(campaign, "latest_run", return_value=run(run_id=2)):
            with self.assertRaisesRegex(ValueError, "newer run"):
                campaign.retry(REPO, BRANCH, 1, "network failure; regression passed")
            self.assertEqual(1, api.call_count)
        with patch.object(campaign, "candidate", return_value=SHA), \
                patch.object(campaign, "api", side_effect=[failed, None]) as api, \
                patch.object(campaign, "latest_run", return_value=failed), \
                patch.object(campaign, "ensure_capacity"):
            campaign.retry(REPO, BRANCH, 1, "network failure; focused setup passed")
            self.assertEqual(f"repos/{REPO}/actions/runs/1/rerun-failed-jobs", api.call_args.args[0])
            self.assertEqual({}, api.call_args.kwargs["data"])

    def test_latest_run_excludes_inventory_wrong_sha_and_wrong_branch(self):
        good = run("flink-suite.yml")
        candidates = [good, {**good, "id": 2, "display_title": "SQL admission inventory (all)"},
                      {**good, "id": 3, "head_branch": "other"},
                      {**good, "id": 4, "head_sha": "b" * 40}]
        with patch.object(campaign, "pages", return_value=iter(candidates)):
            self.assertEqual(good, campaign.latest_run(REPO, BRANCH, SHA, "flink-suite.yml"))

    def test_capacity_does_not_confuse_current_campaign_with_other_branches(self):
        current = run(status="in_progress", conclusion=None)
        other = {**run(run_id=2, status="in_progress", conclusion=None),
                 "head_branch": "other", "name": "CI", "repository_name": REPO}
        with patch.object(campaign, "competitors", return_value=[other]) as competitors, \
                patch.object(campaign, "pages", return_value=iter([{"status": "queued"}])):
            with self.assertRaisesRegex(ValueError, "do not cancel another task"):
                campaign.ensure_capacity(REPO, [current])
            self.assertEqual({(REPO, 1)}, competitors.call_args.args[1])

    def test_tail_work_uses_idle_slots_but_full_budget_and_synthetic_starts_block(self):
        other = {**run(run_id=2, status="in_progress", conclusion=None),
                 "name": "CI", "repository_name": REPO}
        executing = {"status": "in_progress", "steps": [{"started_at": "now", "conclusion": "success"}]}
        for jobs, capacity, allowed in (([executing], 2, True), ([executing], 1, False),
                                        ([{"status": "in_progress", "started_at": "preallocated", "steps": []}], 2, False)):
            with self.subTest(jobs=jobs, capacity=capacity), \
                    patch.object(campaign, "competitors", return_value=[other]), \
                    patch.object(campaign, "pages", return_value=iter(jobs)):
                if allowed:
                    campaign.ensure_capacity(REPO, [], capacity)
                else:
                    with self.assertRaisesRegex(ValueError, "queued jobs or fills"):
                        campaign.ensure_capacity(REPO, [], capacity)

    def test_pagination_includes_work_past_the_first_hundred(self):
        with patch.object(campaign, "api", side_effect=[{"workflow_runs": list(range(100))},
                                                        {"workflow_runs": [100]}]) as api:
            self.assertEqual(list(range(101)), list(campaign.pages("runs?status=queued", "workflow_runs")))
            self.assertEqual("runs?status=queued&per_page=100&page=2", api.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
