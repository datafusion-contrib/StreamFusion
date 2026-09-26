import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import shared_build


class SharedBuildTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "original build"
        for name in (
            "flink-1.18.1/.git/HEAD", "flink-1.18.1/mvnw",
            "flink-1.18.1/target/test-classes/QueryITCase.class",
            "flink-1.18.1/target/surefire-reports/TEST-stale.xml",
            "m2/module/current.jar", "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
            "streamfusion-source/native/target/debug/libstreamfusion.so",
            "streamfusion-source/native/target/debug/deps/not-needed.rlib",
            "diagnostics/runtime/old-evidence.json", "runtime-shards.json",
        ):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture")
        (self.root / "flink-1.18.1/mvnw").chmod(0o755)
        (self.root / "streamfusion-classpath.txt").write_text(str(self.root / "m2/module/current.jar"))
        identity = {"revision": "commit", "flink_version": "1.18.1", "platform": "test", "architecture": "test"}
        mock = patch.object(shared_build, "identity", return_value=identity)
        mock.start()
        self.addCleanup(mock.stop)

    def test_relocation_preserves_only_current_build_inputs(self):
        entries = shared_build.prepare(self.root, self.root, "1.18.1")
        destination = self.root.parent / "other checkout"
        destination.mkdir()
        archive = self.root.parent / "build.tar"
        subprocess.run(["tar", "-cf", str(archive), "--exclude=surefire-reports",
                        "-C", str(self.root), *entries], check=True)
        subprocess.run(["tar", "-xf", str(archive), "-C", str(destination)], check=True)
        shared_build.restore(destination, destination, "1.18.1")
        self.assertEqual(str(destination.resolve() / "m2/module/current.jar"),
                         (destination / "streamfusion-classpath.txt").read_text())
        self.assertTrue((destination / "flink-1.18.1/mvnw").stat().st_mode & 0o111)
        self.assertTrue((destination / "flink-1.18.1/target/test-classes/QueryITCase.class").exists())
        self.assertFalse((destination / "flink-1.18.1/target/surefire-reports").exists())
        self.assertFalse((destination / "diagnostics").exists())
        self.assertFalse((destination / "streamfusion-source/native/target/debug/deps").exists())

    def test_wrong_revision_or_line_is_rejected(self):
        shared_build.prepare(self.root, self.root, "1.18.1")
        path = self.root / "shared-build.json"
        original = json.loads(path.read_text())
        for key in ("revision", "flink_version", "architecture"):
            with self.subTest(key=key):
                path.write_text(json.dumps({**original, key: "wrong"}))
                with self.assertRaisesRegex(ValueError, "mismatch"):
                    shared_build.restore(self.root, self.root, "1.18.1")

    def test_external_or_missing_classpath_entry_is_rejected(self):
        shared_build.prepare(self.root, self.root, "1.18.1")
        for entry in ("/outside/build.jar", str(self.root / "m2/missing.jar")):
            with self.subTest(entry=entry):
                (self.root / "streamfusion-classpath.txt").write_text(entry)
                with self.assertRaises(ValueError):
                    shared_build.restore(self.root, self.root, "1.18.1")

    def test_missing_native_build_is_rejected(self):
        shutil.rmtree(self.root / "streamfusion-source/native")
        with self.assertRaisesRegex(ValueError, "native libraries"):
            shared_build.prepare(self.root, self.root, "1.18.1")


if __name__ == "__main__":
    unittest.main()
