"""Exercise timing accounting with dependencies, queued jobs, retries and failed steps."""

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("profile_ci", Path(__file__).with_name("profile-ci.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class ProfileCiTest(unittest.TestCase):
    def test_parallel_costs_are_distinct_from_workflow_latency_and_start_delays(self):
        run = {"created_at": "2026-11-07T00:00:00Z", "status": "completed",
               "conclusion": "failure", "html_url": "run", "head_sha": "sha"}
        def job(name, start, end, conclusion="success"):
            return {"name": name, "status": "completed", "conclusion": conclusion,
                    "started_at": f"2026-11-07T00:{start}:00Z",
                    "completed_at": f"2026-11-07T00:{end}:00Z", "html_url": name,
                    "steps": [{"name": "tests", "conclusion": conclusion,
                               "started_at": f"2026-11-07T00:{start}:00Z",
                               "completed_at": f"2026-11-07T00:{end}:00Z"}]}
        jobs = [job("Shared debug native libraries", "00", "02"),
                job("Canonical savepoint jobs A", "02", "07"),
                job("Canonical savepoint jobs B", "04", "09", "failure")]
        result = profile.summarize(run, jobs)
        self.assertEqual(540, result["elapsed_seconds"])
        self.assertEqual(720, result["runner_seconds"])
        self.assertEqual(600, result["groups"]["canonical"]["runner_seconds"])
        self.assertEqual(300, result["groups"]["canonical"]["max_job_seconds"])
        self.assertEqual(240, result["jobs"][2]["start_delay_seconds"])
        self.assertEqual(["tests"], result["jobs"][2]["failed_steps"])

    def test_queued_and_skipped_jobs_do_not_receive_fictitious_execution_time(self):
        run = {"created_at": "2026-11-07T00:00:00Z", "status": "in_progress",
               "html_url": "run", "head_sha": "sha"}
        queued = {"name": "queued", "status": "queued", "started_at": run["created_at"],
                  "html_url": "job", "steps": []}
        skipped = {"name": "skipped", "status": "completed", "conclusion": "skipped",
                   "started_at": None, "completed_at": None, "html_url": "skip"}
        canceled = {**queued, "status": "completed", "conclusion": "cancelled",
                    "completed_at": "2026-11-07T00:04:00Z"}
        result = profile.summarize(run, [queued, skipped, canceled])
        self.assertFalse(result["complete"])
        self.assertIsNone(result["elapsed_seconds"])
        self.assertEqual(0, result["runner_seconds"])
        self.assertIsNone(result["jobs"][0]["execution_seconds"])
        self.assertIsNone(result["jobs"][0]["start_delay_seconds"])
        self.assertIsNone(result["jobs"][2]["execution_seconds"])


if __name__ == "__main__":
    unittest.main()
