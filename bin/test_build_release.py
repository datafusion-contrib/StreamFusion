"""Exercise native build grouping and release staging through the actual shell script."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("build-release.sh")
EXTENSIONS = "kafka fluss json csv raw avro protobuf parquet orc paimon".split()


class BuildReleaseTest(unittest.TestCase):
    def run_build(self, root, line, platform, *options, fail=False):
        repository = root / "repository"
        (repository / "bin").mkdir(parents=True)
        (repository / "native").mkdir()
        shutil.copy2(SCRIPT, repository / "bin/build-release.sh")
        commands = root / "commands"
        commands.mkdir()
        programs = {
            "uname": 'print(os.environ["PLATFORM"] if sys.argv[1] == "-s" else "arm64")',
            "nm": "pass",
            "rustup": "pass",
            "mvn": '(root / "maven-ran").touch()',
            "docker": '''
if sys.argv[1] == "create": print("fixture-container")
if sys.argv[1] == "cp": Path(sys.argv[-1]).write_text("linux library")
''',
            "cargo": '''
args = sys.argv[1:]
with (root / "cargo.jsonl").open("a") as target: target.write(json.dumps(args) + "\\n")
if os.environ.get("FAIL_BUILD") == "true": raise SystemExit(17)
destination = Path(args[args.index("--target-dir") + 1])
if "--target" in args: destination /= args[args.index("--target") + 1]
destination /= "release"
destination.mkdir(parents=True, exist_ok=True)
suffix = "dylib" if os.environ["PLATFORM"] == "Darwin" else "so"
for index, arg in enumerate(args):
 if arg == "-p":
  library = args[index + 1].replace("-", "_")
  (destination / ("lib" + library + "." + suffix)).write_text(library)
''',
        }
        for name, body in programs.items():
            path = commands / name
            path.write_text("#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\n"
                            "root = Path(os.environ['FIXTURE_ROOT'])\n" + body + "\n")
            path.chmod(0o755)
        result = subprocess.run(
            [str(repository / "bin/build-release.sh"), "--flink-line", line, *options],
            env={**os.environ, "PATH": str(commands) + os.pathsep + os.environ["PATH"],
                 "PLATFORM": platform, "FIXTURE_ROOT": str(root), "FAIL_BUILD": str(fail).lower()},
            capture_output=True, text=True, timeout=30,
        )
        calls = [json.loads(value) for value in (root / "cargo.jsonl").read_text().splitlines()]
        return result, repository, calls

    def assert_packages(self, calls, line):
        expected = {"streamfusion", *("streamfusion-" + extension for extension in EXTENSIONS)}
        if line == "2.2": expected.add("streamfusion-iceberg")
        if line == "1.18": expected.add("streamfusion-starrocks")
        for args in calls:
            self.assertEqual(expected, {args[index + 1] for index, arg in enumerate(args) if arg == "-p"})
            self.assertIn("--release", args)
            self.assertIn("--lib", args)
            self.assertEqual("mimalloc", args[args.index("--features") + 1])

    def test_host_builds_once_and_stages_every_selected_library(self):
        for line in ("2.2", "1.18"):
            for platform in ("Linux", "Darwin"):
                with self.subTest(line=line, platform=platform), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    result, repository, calls = self.run_build(root, line, platform, "--host-only")
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertEqual(1, len(calls))
                    self.assert_packages(calls, line)
                    staged = list((repository / "native/target/universal").rglob("libstreamfusion*"))
                    self.assertEqual(12, len(staged))
                    self.assertEqual(line == "2.2", any("iceberg" in path.name for path in staged))
                    self.assertEqual(line == "1.18", any("starrocks" in path.name for path in staged))
                    self.assertTrue((root / "maven-ran").exists())

    def test_universal_builds_once_per_darwin_target(self):
        for line in ("2.2", "1.18"):
            with self.subTest(line=line), tempfile.TemporaryDirectory() as directory:
                result, _, calls = self.run_build(Path(directory), line, "Darwin")
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual(2, len(calls))
                self.assert_packages(calls, line)
                self.assertEqual({"aarch64-apple-darwin", "x86_64-apple-darwin"},
                                 {args[args.index("--target") + 1] for args in calls})

    def test_failed_native_build_does_not_package_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, _, calls = self.run_build(root, "2.2", "Linux", "--host-only", fail=True)
            self.assertEqual(17, result.returncode)
            self.assertEqual(1, len(calls))
            self.assertFalse((root / "maven-ran").exists())


if __name__ == "__main__":
    unittest.main()
