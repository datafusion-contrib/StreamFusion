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
        self.root = Path(self.temp.name).resolve() / "original build"
        for name in (
            "flink-1.18.1/.git/HEAD", "flink-1.18.1/mvnw",
            "flink-1.18.1/target/test-classes/QueryITCase.class",
            "flink-1.18.1/target/surefire-reports/TEST-stale.xml",
            "maven-wrapper/wrapper/dists/apache-maven-3.8.6-bin/hash/apache-maven-3.8.6/bin/mvn",
            "m2/module/current.jar", "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
            "streamfusion-source/native/target/debug/libstreamfusion.so",
            "streamfusion-source/native/target/debug/deps/not-needed.rlib",
            "diagnostics/runtime/old-evidence.json", "runtime-shards.json",
        ):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture")
        (self.root / "flink-1.18.1/mvnw").chmod(0o755)
        self.maven = self.root / "maven-wrapper/wrapper/dists/apache-maven-3.8.6-bin/hash/apache-maven-3.8.6/bin/mvn"
        self.maven.chmod(0o755)
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
        self.assertTrue((destination / self.maven.relative_to(self.root)).stat().st_mode & 0o111)
        self.assertTrue((destination / "flink-1.18.1/target/test-classes/QueryITCase.class").exists())
        self.assertFalse((destination / "flink-1.18.1/target/surefire-reports").exists())
        self.assertFalse((destination / "diagnostics").exists())
        self.assertFalse((destination / "streamfusion-source/native/target/debug/deps").exists())

    def pack(self):
        archive = self.root.parent / "build.tar.zst"
        shared_build.pack(self.root, self.root, "1.18.1", archive)
        return archive

    def test_checked_archive_round_trip_relocates_to_final_directory(self):
        archive = self.pack()
        destination = self.root.parent / "restored build"
        shared_build.unpack(destination, destination, "1.18.1", archive)
        self.assertEqual(str(destination / "m2/module/current.jar"),
                         (destination / "streamfusion-classpath.txt").read_text())
        self.assertEqual(str(destination),
                         json.loads((destination / "shared-build.json").read_text())["suite_root"])
        self.assertTrue((destination / self.maven.relative_to(self.root)).stat().st_mode & 0o111)
        self.assertFalse((destination / "flink-1.18.1/target/surefire-reports").exists())

    def test_truncated_transfer_is_rejected_before_extraction(self):
        archive = self.pack()
        archive.write_bytes(archive.read_bytes()[:40])
        destination = self.root.parent / "restored build"
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            shared_build.unpack(destination, destination, "1.18.1", archive)
        self.assertFalse(destination.exists())

    def test_invalid_archive_does_not_leave_partial_restore(self):
        archive = self.pack()
        archive.write_bytes(archive.read_bytes()[:40])
        shared_build.checksum_path(archive).write_text(shared_build.archive_digest(archive))
        destination = self.root.parent / "restored build"
        with self.assertRaises(subprocess.CalledProcessError):
            shared_build.unpack(destination, destination, "1.18.1", archive)
        self.assertFalse(destination.exists())
        self.assertFalse(list(destination.parent.glob("shared-build-restore-*")))

    def test_wrong_revision_does_not_publish_restore(self):
        archive = self.pack()
        destination = self.root.parent / "restored build"
        with patch.object(shared_build, "identity", return_value={"revision": "other"}):
            with self.assertRaisesRegex(ValueError, "revision mismatch"):
                shared_build.unpack(destination, destination, "1.18.1", archive)
        self.assertFalse(destination.exists())

    def test_existing_evidence_is_never_overwritten(self):
        archive = self.pack()
        with self.assertRaisesRegex(ValueError, "empty suite directory"):
            shared_build.unpack(self.root, self.root, "1.18.1", archive)
        self.assertTrue((self.root / "diagnostics/runtime/old-evidence.json").exists())

    def test_corrupt_download_retries_transfer_into_clean_directory(self):
        original = self.pack()
        archive = original.parent / "download" / original.name
        directories = []

        def transfer(command, **kwargs):
            self.assertEqual(["gh", "run", "download", "123", "--repo", "owner/repo",
                              "--name", "build", "--dir"], command[:-1])
            directory = Path(command[-1])
            self.assertEqual(str(directory), kwargs["env"]["TMPDIR"])
            self.assertEqual(shared_build.DOWNLOAD_TIMEOUT_SECONDS, kwargs["timeout"])
            self.assertEqual([], list(directory.iterdir()))
            directories.append(directory)
            candidate = directory / original.name
            candidate.write_bytes(b"truncated" if len(directories) == 1 else original.read_bytes())
            shutil.copy(shared_build.checksum_path(original), shared_build.checksum_path(candidate))

        with patch.object(shared_build.subprocess, "run", side_effect=transfer), \
                patch.object(shared_build.time, "sleep") as sleep:
            shared_build.download(archive, "build", "123", "owner/repo")
        self.assertEqual(2, len(directories))
        self.assertTrue(all(not directory.exists() for directory in directories))
        sleep.assert_called_once_with(shared_build.DOWNLOAD_DELAYS[0])
        shared_build.verify_archive(archive)

    def test_download_failure_is_bounded_and_preserves_previous_archive(self):
        archive = self.pack()
        original = archive.read_bytes()

        def timeout(command, **kwargs):
            # A killed gh process cannot run its deferred ZIP-file cleanup.
            temporary_zip = Path(kwargs["env"]["TMPDIR"]) / "gh-artifact.partial.zip"
            temporary_zip.write_bytes(b"partial artifact")
            raise subprocess.TimeoutExpired("gh", shared_build.DOWNLOAD_TIMEOUT_SECONDS)

        with patch.object(shared_build.subprocess, "run", side_effect=timeout) as run, \
                patch.object(shared_build.time, "sleep") as sleep:
            with self.assertRaises(subprocess.TimeoutExpired):
                shared_build.download(archive, "build", "123", "owner/repo")
        self.assertEqual(3, run.call_count)
        self.assertEqual(2, sleep.call_count)
        self.assertEqual(original, archive.read_bytes())
        self.assertFalse(list(archive.parent.glob("shared-build-*")))

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

    def test_missing_or_nonexecutable_maven_is_rejected(self):
        shared_build.prepare(self.root, self.root, "1.18.1")
        for missing in (False, True):
            with self.subTest(missing=missing):
                self.maven.chmod(0o644)
                if missing:
                    self.maven.unlink()
                with self.assertRaisesRegex(ValueError, "Maven wrapper distribution"):
                    shared_build.prepare(self.root, self.root, "1.18.1")
                with self.assertRaisesRegex(ValueError, "Maven wrapper distribution"):
                    shared_build.restore(self.root, self.root, "1.18.1")


if __name__ == "__main__":
    unittest.main()
