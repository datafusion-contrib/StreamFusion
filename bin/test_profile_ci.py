"""Exercise timing accounting with dependencies, queued jobs, retries and failed steps."""

import importlib.util
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import xml.etree.ElementTree as ET

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
        jobs = [job("Shared debug native libraries", "00", "01"),
                job("Canonical savepoint jobs A", "02", "07"),
                job("Canonical savepoint jobs B", "04", "09", "failure"),
                job("Prepare canonical Java (Flink 2.2, JDK 17)", "01", "02")]
        result = profile.summarize(run, jobs)
        self.assertEqual(540, result["elapsed_seconds"])
        self.assertEqual(720, result["runner_seconds"])
        self.assertEqual(600, result["groups"]["canonical"]["runner_seconds"])
        self.assertEqual(60, result["groups"]["canonical preparation"]["runner_seconds"])
        self.assertEqual(60, result["groups"]["other"]["runner_seconds"])
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


class CanonicalCoverageTest(unittest.TestCase):
    def fixture(self, root):
        classes = {"A": [("parameter[1]", False), ("parameter[2]", False)],
                   "B": [("known fallback", True)], "Empty": []}
        inventory = hashlib.sha256(json.dumps(sorted(
            "tech/streamfusion/Canonical" + name + "JobTest" for name in (*classes, "Abstract"))).encode()).hexdigest()
        identities, outcomes = [], {}
        for shard, names in ((1, ("A", "Empty")), (2, ("B",))):
            directory = root / f"canonical-job-results-flink2.2-jdk11-shard{shard}"
            directory.mkdir()
            selection = ["tech/streamfusion/Canonical" + name + "JobTest" for name in names]
            if shard == 1:
                selection.append("tech/streamfusion/CanonicalAbstractJobTest")
            (directory / "canonical-job-includes.json").write_text(json.dumps({
                "shard": shard, "count": 2, "suite": "canonical", "flink_line": "2.2",
                "classes": selection, "inventory_sha256": inventory}))
            for name in names:
                qualified = "tech.streamfusion.Canonical" + name + "JobTest"
                values = {"tests": len(classes[name]), "errors": 0, "failures": 0,
                          "skipped": sum(skipped for _, skipped in classes[name])}
                outcomes[qualified] = values
                suite = ET.Element("testsuite", name=qualified, **{k: str(v) for k, v in values.items()})
                for case_name, skipped in classes[name]:
                    case = ET.SubElement(suite, "testcase", classname=qualified, name=case_name)
                    if skipped:
                        ET.SubElement(case, "skipped")
                    identities.append((qualified, case_name, skipped, 1))
                ET.ElementTree(suite).write(directory / f"TEST-{qualified}.xml")
        return {"canonical_baseline": {"flink2.2-jdk11": {
            "classes": outcomes, "inventory_sha256": [inventory],
            "case_identities_sha256": hashlib.sha256(json.dumps(sorted(identities)).encode()).hexdigest()}}}

    def test_complete_comparison_preserves_empty_suites_parameterized_cases_and_known_skips(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = self.fixture(root)
            report = profile.canonical_coverage(root, baseline)["flink2.2-jdk11"]
            self.assertEqual(2, report["shards"])
            self.assertEqual(4, report["selected_classes"])
            self.assertEqual(3, report["reported_classes"])
            self.assertEqual({"tests": 3, "errors": 0, "failures": 0, "skipped": 1}, report["outcomes"])

    def test_unchanged_totals_cannot_hide_a_duplicate_and_missing_parameter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = self.fixture(root)
            report = next(root.rglob("TEST-*CanonicalAJobTest.xml"))
            tree = ET.parse(report)
            cases = tree.getroot().findall("testcase")
            cases[1].set("name", cases[0].get("name"))
            tree.write(report)
            with self.assertRaisesRegex(ValueError, "inventory or invocations"):
                profile.canonical_coverage(root, baseline)

    def test_missing_shards_missing_empty_suites_and_changed_skips_are_rejected(self):
        for change in ("shard", "empty", "abstract", "skip", "duplicate", "manifest", "hidden failure"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                baseline = self.fixture(root)
                if change == "shard":
                    shutil.rmtree(root / "canonical-job-results-flink2.2-jdk11-shard2")
                elif change == "empty":
                    next(root.rglob("TEST-*CanonicalEmptyJobTest.xml")).unlink()
                elif change == "duplicate":
                    original = next(root.rglob("TEST-*CanonicalAJobTest.xml"))
                    shutil.copyfile(original, original.with_name("TEST-duplicate.xml"))
                elif change in ("manifest", "abstract"):
                    path = root / "canonical-job-results-flink2.2-jdk11-shard1/canonical-job-includes.json"
                    data = json.loads(path.read_text())
                    if change == "manifest":
                        data["inventory_sha256"] = "other"
                    else:
                        data["classes"].remove("tech/streamfusion/CanonicalAbstractJobTest")
                    path.write_text(json.dumps(data))
                else:
                    path = next(root.rglob("TEST-*CanonicalAJobTest.xml"))
                    tree = ET.parse(path)
                    ET.SubElement(tree.getroot().find("testcase"), "skipped" if change == "skip" else "failure")
                    tree.write(path)
                with self.assertRaises(ValueError):
                    profile.canonical_coverage(root, baseline)


if __name__ == "__main__":
    unittest.main()
