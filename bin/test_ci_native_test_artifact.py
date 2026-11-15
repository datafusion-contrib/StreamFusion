"""Reject stripping that changes loadable code, native exports or library identity."""

from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import ci_native_test_artifact as artifact


def library(path, *, code=b"code", exports=b"JNI exports", machine=62, debug=b"debug info"):
    names = b"\0.text\0.dynsym\0.shstrtab\0.debug_info\0"
    values = [(b".text", 1, 6, 4096, code), (b".dynsym", 11, 2, 8192, exports),
              (b".shstrtab", 3, 0, 0, names), (b".debug_info", 1, 0, 0, debug)]
    offset = 64 + 5 * 64
    entries = [b"\0" * 64]
    payload = b""
    for name, kind, flags, address, data in values:
        entries.append(struct.pack("<IIQQQQIIQQ", names.index(name), kind, flags, address,
                                   offset, len(data), 0, 0, 1, 0))
        offset += len(data)
        payload += data
    header = struct.pack("<16sHHIQQQIHHHHHH", b"\x7fELF\x02\x01\x01" + b"\0" * 9,
                         3, machine, 1, 0, 0, 64, 0, 64, 56, 0, 64, 5, 3)
    path.write_bytes(header + b"".join(entries) + payload)


class NativeTestArtifactTest(unittest.TestCase):
    def test_only_debug_data_changes_and_original_build_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original libraries"
            source.mkdir()
            original = source / "libstreamfusion.so"
            library(original)
            before = original.read_bytes()

            def strip(command, **kwargs):
                self.assertEqual(["strip", "--strip-debug", "-o"], command[:3])
                library(Path(command[3]), debug=b"")

            output = root / "test libraries"
            with patch.object(artifact.subprocess, "run", side_effect=strip):
                result = artifact.stage(source, output)
            self.assertEqual(before, original.read_bytes())
            self.assertLess(result[original.name]["test_bytes"], result[original.name]["original_bytes"])
            with self.assertRaises(FileExistsError):
                artifact.stage(source, output)
            self.assertTrue((output / original.name).exists())

    def test_runtime_code_exports_or_architecture_changes_fail_and_clean_partial_outputs(self):
        for change in ({"code": b"bad!"}, {"exports": b"bad exports"}, {"machine": 0}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "original"
                source.mkdir()
                library(source / "libstreamfusion.so")

                def strip(command, **kwargs):
                    library(Path(command[3]), **change)

                output = root / "partial"
                with patch.object(artifact.subprocess, "run", side_effect=strip):
                    with self.assertRaisesRegex(ValueError, "changed"):
                        artifact.stage(source, output)
                self.assertFalse(output.exists())
                self.assertTrue((source / "libstreamfusion.so").exists())

    def test_failed_tool_or_missing_source_cannot_produce_an_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Missing"):
                artifact.stage(root / "missing", root / "output")
            library(root / "libstreamfusion.so")
            with patch.object(artifact.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "strip")):
                with self.assertRaises(subprocess.CalledProcessError):
                    artifact.stage(root, root / "output")
            self.assertFalse((root / "output").exists())


if __name__ == "__main__":
    unittest.main()
