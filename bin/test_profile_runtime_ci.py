"""Reject altered or incomplete runtime profiling evidence, including existing skips."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


spec = importlib.util.spec_from_file_location("profile_runtime_ci", Path(__file__).with_name("profile-runtime-ci.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class RuntimeProfileTest(unittest.TestCase):
    def reports(self, root, line, selection=profile.CLASSES):
        for name, (tests, skips) in selection.items():
            name = "tech.streamfusion." + name
            suite = ET.Element("testsuite", name=name + "(trial)", tests=str(tests),
                               failures="0", errors="0", skipped=str(skips[line]), time="1")
            for index in range(tests):
                case = ET.SubElement(suite, "testcase", classname=name, name=f"case-{index}")
                if index < skips[line]:
                    ET.SubElement(case, "skipped")
            ET.ElementTree(suite).write(root / f"TEST-{name}-trial.xml")

    def test_hotspots_retain_all_cases_including_qualified_planner_classes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.reports(root, "2.2", profile.HOTSPOTS)
            cases, totals, classes = profile.outcomes(root, "trial", "2.2", profile.HOTSPOTS)
            self.assertEqual(2759, sum(cases.values()))
            self.assertEqual(2759, totals["tests"])
            self.assertEqual(0, totals["skipped"])
            self.assertEqual(5, len(classes))

    def test_both_lines_retain_every_invocation_and_existing_skip(self):
        for line, skips in (("2.2", 2), ("1.18", 58)):
            with self.subTest(line=line), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.reports(root, line)
                cases, totals, classes = profile.outcomes(root, "trial", line)
                self.assertEqual(305, totals["tests"])
                self.assertEqual(skips, totals["skipped"])
                self.assertEqual(305, sum(cases.values()))
                self.assertEqual(4, len(classes))

    def test_missing_classes_and_new_skips_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.reports(root, "2.2")
            report = next(root.glob("*StatefulSqlRescaleTest*"))
            tree = ET.parse(report)
            tree.getroot().set("skipped", "3")
            ET.SubElement(tree.getroot().findall("testcase")[-1], "skipped")
            tree.write(report)
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                profile.outcomes(root, "trial", "2.2")
            report.unlink()
            with self.assertRaisesRegex(ValueError, "selection"):
                profile.outcomes(root, "trial", "2.2")

    def test_duplicate_reports_and_hidden_failures_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.reports(root, "2.2")
            report = next(root.glob("*StatefulSqlRescaleTest*"))
            duplicate = root / "TEST-duplicate-trial.xml"
            duplicate.write_bytes(report.read_bytes())
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                profile.outcomes(root, "trial", "2.2")
            duplicate.unlink()
            tree = ET.parse(report)
            ET.SubElement(tree.getroot().findall("testcase")[-1], "failure")
            tree.write(report)
            with self.assertRaisesRegex(ValueError, "counts"):
                profile.outcomes(root, "trial", "2.2")

    def test_case_identity_and_skipped_identity_are_compared(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.reports(root, "2.2")
            original, _, _ = profile.outcomes(root, "trial", "2.2")
            report = next(root.glob("*StatefulSqlRescaleTest*"))
            tree = ET.parse(report)
            cases = tree.getroot().findall("testcase")
            cases[-1].set("name", "different-case")
            tree.write(report)
            changed, _, _ = profile.outcomes(root, "trial", "2.2")
            self.assertNotEqual(original, changed)
            cases[0].remove(cases[0].find("skipped"))
            ET.SubElement(cases[-1], "skipped")
            tree.write(report)
            changed_skips, _, _ = profile.outcomes(root, "trial", "2.2")
            self.assertNotEqual(changed, changed_skips)


if __name__ == "__main__":
    unittest.main()
