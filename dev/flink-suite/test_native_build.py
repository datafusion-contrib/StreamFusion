"""Stage only the current Cargo artifacts, including when reusing a dependency cache."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import native_build


class NativeBuildTest(unittest.TestCase):
    def test_current_linux_and_darwin_artifacts_are_staged_without_copying_cached_extras(self):
        for suffix in ("so", "dylib"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory(prefix="native build ") as directory:
                root = Path(directory).resolve()
                source, target = root / "isolated source", root / "shared target"
                (source / "native").mkdir(parents=True)
                debug = target / "debug"
                debug.mkdir(parents=True)
                libraries = [debug / f"libstreamfusion.{suffix}", debug / f"libstreamfusion_json.{suffix}"]
                for library in libraries:
                    library.write_text("current build")
                (debug / f"libstreamfusion_removed.{suffix}").write_text("stale cache output")
                messages = [{"reason": "compiler-artifact", "target": {"crate_types": ["cdylib"]},
                             "filenames": [str(path)], "fresh": True} for path in libraries]
                messages.append({"reason": "build-finished", "success": True})
                result = subprocess.CompletedProcess([], 0, stdout="\n".join(map(json.dumps, messages)))
                with patch.object(native_build.subprocess, "run", return_value=result) as run:
                    self.assertEqual(set(libraries), native_build.build(source, target))
                command = run.call_args.args[0]
                self.assertEqual(["cargo", "build", "--workspace", "--lib", "--locked",
                                  "--message-format=json-render-diagnostics", "--target-dir", str(target)], command)
                self.assertEqual(source / "native", run.call_args.kwargs["cwd"])
                output = source / "native/target/debug"
                self.assertEqual({path.name for path in libraries}, {path.name for path in output.iterdir()})
                self.assertTrue(all(path.read_text() == "current build" for path in output.iterdir()))
                self.assertTrue((debug / f"libstreamfusion_removed.{suffix}").exists())

    def test_failed_or_empty_build_cannot_stage_an_old_library(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "source", root / "target"
            (source / "native").mkdir(parents=True)
            (target / "debug").mkdir(parents=True)
            (target / "debug/libstreamfusion.so").write_text("old library")
            with patch.object(native_build.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["cargo"])):
                with self.assertRaises(subprocess.CalledProcessError):
                    native_build.build(source, target)
            with patch.object(native_build.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="")):
                with self.assertRaisesRegex(ValueError, "no StreamFusion native libraries"):
                    native_build.build(source, target)
            self.assertFalse((source / "native/target").exists())


if __name__ == "__main__":
    unittest.main()
