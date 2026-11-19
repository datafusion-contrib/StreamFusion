"""Keep historical observations and published report bytes compatible across evidence schemas."""

import copy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from benchmark_reports import SCHEMA_KEY, load_report, original_report


class BenchmarkReportsTest(unittest.TestCase):
    def fixture(self, root):
        original = {"observations": [{"nanos": [13, 17], "nativePlan": "NativeCalc\n  scan\n"}]}
        payload = json.dumps(original, indent=4).encode() + b"\n"
        name = "docs/benchmarks/embedded-plans/report.json"
        archive = root / "remote.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            header = tarfile.TarInfo(name)
            header.size = len(payload)
            output.addfile(header, io.BytesIO(payload))
        digest = lambda data: dict(size=len(data), sha256=hashlib.sha256(data).hexdigest())
        record = digest(payload)
        pointer = "/observations/0/nativePlan"
        manifest = {"schema_version": 1,
                    "bundles": {"test": dict(url=archive.as_uri(), **digest(archive.read_bytes()))},
                    "files": {name: dict(bundle="test", source=dict(json_pointers=[pointer]), **record)}}
        path = root / "manifest.json"
        path.write_text(json.dumps(manifest))
        report = copy.deepcopy(original)
        report["observations"][0]["nativePlan"] = {
            "$plan": dict(path=name, pointer=pointer, sha256=record["sha256"])}
        report[SCHEMA_KEY] = dict(version=2, original=name)
        maintained = root / "report.json"
        maintained.write_text(json.dumps(report))
        return maintained, path, payload, original, report

    def test_original_bytes_and_types_survive_online_and_offline_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report, manifest, payload, expected, _ = self.fixture(root)
            args = (report, root / "evidence", root / "cache", manifest)
            self.assertEqual(payload, original_report(*args))
            (root / "remote.tar.gz").unlink()
            self.assertEqual(expected, load_report(*args))

    def test_legacy_reports_require_no_artifact_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "legacy.json"
            path.write_bytes(b'{ "plan": "old plan", "nanos": [1,2] }\n')
            self.assertEqual(path.read_bytes(), original_report(path, root, root, root / "missing"))

    def test_tampered_observations_references_and_versions_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, manifest, _, _, report = self.fixture(root)
            variants = []
            wrong = copy.deepcopy(report)
            wrong["observations"][0]["nanos"][0] = 99
            variants.append(wrong)
            for field, value in (("sha256", "0" * 64), ("pointer", "/elsewhere"),
                                 ("path", "docs/benchmarks/elsewhere.json")):
                wrong = copy.deepcopy(report)
                wrong["observations"][0]["nativePlan"]["$plan"][field] = value
                variants.append(wrong)
            wrong = copy.deepcopy(report)
            wrong[SCHEMA_KEY]["version"] = 3
            variants.append(wrong)
            wrong = copy.deepcopy(report)
            wrong["observations"][0]["nativePlan"] = "lost reference"
            variants.append(wrong)
            for index, variant in enumerate(variants):
                with self.subTest(variant=index):
                    path.write_text(json.dumps(variant))
                    with self.assertRaises(ValueError):
                        original_report(path, root / "evidence", root / "cache", manifest)


if __name__ == "__main__":
    unittest.main()
