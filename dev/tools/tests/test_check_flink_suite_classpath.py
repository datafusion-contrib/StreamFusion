import os
from pathlib import Path
import tempfile
import unittest
import zipfile


from dev.tools.checks import flink_suite_classpath as checker


class SuiteClasspathTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def jar(self, name, module=None, line=None):
        path = self.directory / name
        with zipfile.ZipFile(path, "w") as archive:
            manifest = "Manifest-Version: 1.0\r\n"
            if module:
                manifest += f"StreamFusion-Module: {module}\r\n"
            if line:
                manifest += f"StreamFusion-Flink-Line: {line}\r\n"
            archive.writestr("META-INF/MANIFEST.MF", manifest)
        return str(path)

    def test_matching_lines_and_unrelated_dependencies(self):
        for line, suffix in [("2.2", ""), ("1.18", "-flink1.18")]:
            with self.subTest(line=line):
                core = "streamfusion-core" + suffix
                json = "streamfusion-json" + suffix
                paths = [self.jar("core.jar", core, line), self.jar("json.jar", json, line),
                         self.jar("arrow.jar")]
                self.assertEqual({core, json}, checker.validate(os.pathsep.join(paths), line))

    def test_renaming_a_payload_cannot_hide_its_line(self):
        core = self.jar("core.jar", "streamfusion-core-flink1.18", "1.18")
        wrong = self.jar("renamed.jar", "streamfusion-json", "2.2")
        with self.assertRaisesRegex(ValueError, "expected Flink 1.18"):
            checker.validate(os.pathsep.join([core, wrong]), "1.18")

    def test_source_suite_requires_identified_core(self):
        with self.assertRaisesRegex(ValueError, "missing streamfusion-core"):
            checker.validate(self.jar("arrow.jar"), "2.2")
        with self.assertRaisesRegex(ValueError, "payload identity"):
            checker.validate(self.jar("streamfusion-core.jar"), "2.2")

    def test_line_and_artifact_identity_must_agree(self):
        with self.assertRaisesRegex(ValueError, "payload identity"):
            checker.validate(self.jar("core.jar", "streamfusion-core", "1.18"), "1.18")

    def test_duplicate_payloads_are_rejected(self):
        paths = [self.jar("a.jar", "streamfusion-core", "2.2"),
                 self.jar("b.jar", "streamfusion-core", "2.2")]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            checker.validate(os.pathsep.join(paths), "2.2")


if __name__ == "__main__":
    unittest.main()
