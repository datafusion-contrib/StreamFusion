"""Keep paired worker comparisons independent and reject incomplete test evidence."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET


spec = importlib.util.spec_from_file_location(
    "profile_canonical", Path(__file__).with_name("profile-canonical-ci.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class CanonicalProfileTest(unittest.TestCase):
    def run_comparison(self, missing_case=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "streamfusion-runtime/target/surefire-reports"
            reports.mkdir(parents=True)
            calls = []

            def run(command, **kwargs):
                environment = kwargs["env"]
                suffix = next(v.split("=", 1)[1] for v in command
                              if v.startswith("-Dsurefire.reportNameSuffix="))
                cache = Path(environment["SF_CANONICAL_CDS_DIR"])
                calls.append((environment["JAVA_TOOL_OPTIONS"], cache,
                              len(list(cache.glob("*.jsa"))), command))
                cache.mkdir(exist_ok=True)
                for name in ("stock", "native"):
                    (cache / (name + ".jsa")).touch()
                for name, count in (("CanonicalRegularJoinJobTest", 4),
                                    ("CanonicalManagedUdfDecimalKeyJobTest", 1)):
                    suite = ET.Element("testsuite", name="tech.streamfusion." + name,
                                       tests=str(count), errors="0", failures="0", skipped="0")
                    for index in range(count - int(missing_case)):
                        ET.SubElement(suite, "testcase", classname="tech.streamfusion." + name,
                                      name=f"case-{index}")
                    ET.ElementTree(suite).write(reports / f"TEST-{name}-{suffix}.xml")
                return type("Result", (), {"returncode": 0})()

            with patch.dict(os.environ, {}, clear=True), patch.object(profile, "ROOT", root), \
                    patch.object(profile.subprocess, "run", side_effect=run):
                if missing_case:
                    with self.assertRaisesRegex(RuntimeError, "Incomplete or inconsistent"):
                        profile.profile(root / "results", 3, cds=True, gc=True)
                else:
                    profile.profile(root / "results", 3, cds=True, gc=True)
            self.assertFalse((root / "results/tmp").exists())
            return calls, json.loads((root / "results/summary.json").read_text())

    def test_collectors_have_independent_preparation_and_alternating_complete_trials(self):
        calls, trials = self.run_comparison()
        self.assertEqual(["preparation"] * 2 + ["measurement"] * 6,
                         [trial["phase"] for trial in trials])
        self.assertEqual(["gc-g1", "gc-serial", "gc-g1", "gc-serial",
                          "gc-serial", "gc-g1", "gc-g1", "gc-serial"],
                         [trial["mode"] for trial in trials])
        self.assertEqual([0, 0, 2, 2, 2, 2, 2, 2], [count for _, _, count, _ in calls])
        self.assertNotEqual(calls[0][1], calls[1][1])
        for (options, cache, _, command), trial in zip(calls, trials):
            self.assertIn("-XX:TieredStopAtLevel=1", options)
            self.assertIn("-XX:ReservedCodeCacheSize=512m", options)
            self.assertEqual(trial["mode"] == "gc-serial", "-XX:+UseSerialGC" in options)
            self.assertEqual(trial["mode"] == "gc-g1", "-XX:+UseG1GC" in options)
            self.assertIn("-Dsf.testForks=4", command)
            self.assertIn("-Dsf.extraJvmArgs=-Xmx1g", command)
            self.assertEqual(5, trial["outcomes"]["tests"])

    def test_report_totals_cannot_hide_missing_invocations(self):
        _, trials = self.run_comparison(missing_case=True)
        self.assertEqual(1, len(trials))


if __name__ == "__main__":
    unittest.main()
