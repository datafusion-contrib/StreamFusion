"""Keep canonical lane failures and complete selections visible to the merge gate."""

import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from dev.tools.ci import canonical_jobs as jobs


class CanonicalJobsTest(unittest.TestCase):
    def test_ci_cli_requires_an_inventory_before_starting_maven(self):
        result = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / "bin/ci_canonical_jobs.py"),
            '--includes', 'unused', '--profiles', 'canonical-kafka', '--line', '2.2'],
            capture_output=True, text=True)
        self.assertEqual(2, result.returncode)
        self.assertIn('--inventory', result.stderr)

    def test_successful_worker_exit_codes_cannot_hide_missing_test_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            includes, names, environment = self.fixture(root)
            inventory = root / 'inventory.json'
            inventory.write_text(json.dumps(dict(schema_version=1, line='2.2', jdk=17,
                full_cases=4, representative_cases=4,
                classes={name.replace('/', '.'): dict(abstract=False, full=1, representative=1,
                    methods={'checked': dict(kind='test', full=1, representative=1)}) for name in names})))
            selection = json.loads(includes.with_suffix('.json').read_text())
            selection.update(coverage='representative',
                             coverage_inventory_sha256=hashlib.sha256(inventory.read_bytes()).hexdigest())
            includes.with_suffix('.json').write_text(json.dumps(selection))
            with patch.dict(os.environ, environment), self.assertRaisesRegex(ValueError, 'Incomplete canonical outcomes'):
                jobs.run(root, includes, 'canonical-kafka', '2.2', 4, '-Xmx1g',
                         inventory=inventory, coverage='representative')
            self.assertEqual(4, len(list(root.glob('*.done'))))

    def fixture(self, root):
        names = [f"tech/streamfusion/FixtureWorker{i}JobTest" for i in range(4)]
        includes = root / "includes.txt"
        includes.write_text("".join(name + ".java\n" for name in names))
        includes.with_suffix(".json").write_text(json.dumps({
            "suite": "canonical", "flink_line": "2.2", "shard": 2,
            "inventory_sha256": "fixture", "classes": names,
        }))
        binary = root / "bin/mvn"
        binary.parent.mkdir()
        binary.write_text("#!" + sys.executable + "\n" + '''
import json, os, sys, time
from pathlib import Path
root = Path(__file__).parent.parent
args = sys.argv[1:]
if "install" in args:
    assert "-am" in args
    assert "-DskipTests=true" in args
    assert "-Dsf.runtime.tests.skip=true" in args
    if os.environ.get("PREP_FAIL"): sys.exit(17)
    (root / "prepared").touch()
else:
    assert (root / "prepared").exists()
    assert "-am" not in args
    assert "-Dsf.runtime.tests.skip=false" in args
    assert "surefire:test@default-test" in args and "surefire:test@orc-tests" in args
    assert "-Dsf.testForks=1" in args and "-Dsf.testReuseForks=false" in args
    includes = Path(next(a.split("=", 1)[1] for a in args if a.startswith("-Dsf.runtime.testIncludesFile=")))
    assert "-DtempDir=surefire-" + includes.stem in args
    time.sleep(0.1)
    for name in includes.read_text().splitlines():
        (root / (name.rsplit("/", 1)[-1] + ".done")).touch(exist_ok=False)
    if os.environ.get("WORKER_FAIL") and "Worker0" in includes.read_text(): sys.exit(23)
''')
        binary.chmod(0o755)
        return includes, names, {"PATH": str(binary.parent) + os.pathsep + os.environ["PATH"]}

    def test_first_and_later_shards_prepare_without_tests_and_run_every_selected_class(self):
        for shard in (1, 2):
            with self.subTest(shard=shard), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                includes, names, environment = self.fixture(root)
                manifest = json.loads(includes.with_suffix(".json").read_text())
                manifest["shard"] = shard
                includes.with_suffix(".json").write_text(json.dumps(manifest))
                with patch.dict(os.environ, environment):
                    code = jobs.run(root, includes, "canonical-kafka", "2.2", 4, "-Xmx1g")
                self.assertEqual(0, code)
                self.assertEqual({name.rsplit("/", 1)[-1] + ".java.done" for name in names},
                                 {path.name for path in root.glob("*.done")})

    def test_a_worker_failure_retains_all_other_completed_lanes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            includes, names, environment = self.fixture(root)
            environment["WORKER_FAIL"] = "1"
            with patch.dict(os.environ, environment):
                code = jobs.run(root, includes, "canonical-kafka", "2.2", 4, "-Xmx1g")
            self.assertEqual(1, code)
            self.assertEqual(4, len(list(root.glob("*.done"))))
            manifest = json.loads((includes.with_suffix(".workers") / "selection.json").read_text())
            self.assertCountEqual(names, [n for w in manifest["workers"] for n in w["classes"]])
            self.assertEqual(4, len(list(includes.with_suffix(".workers").glob("*.log"))))

    def test_failed_preparation_does_not_start_any_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            includes, _, environment = self.fixture(root)
            environment["PREP_FAIL"] = "1"
            with patch.dict(os.environ, environment), self.assertRaises(subprocess.CalledProcessError):
                jobs.run(root, includes, "canonical-kafka", "2.2", 4, "-Xmx1g")
            self.assertFalse(list(root.glob("*.done")))
            self.assertFalse(list(includes.with_suffix(".workers").glob("*.log")))

    def test_manifest_mismatch_fails_before_preparing_the_reactor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            includes, _, environment = self.fixture(root)
            includes.write_text("DifferentTest.java\n")
            with patch.dict(os.environ, environment), self.assertRaises(ValueError):
                jobs.run(root, includes, "canonical-kafka", "2.2", 4, "-Xmx1g")
            self.assertFalse((root / "prepared").exists())

    def test_new_canonical_classes_need_a_cost_before_hosting_long_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            includes, _, environment = self.fixture(root)
            name = "tech/streamfusion/CanonicalUnmeasuredFixtureJobTest"
            includes.write_text(name + ".java\n")
            manifest = json.loads(includes.with_suffix(".json").read_text())
            manifest["classes"] = [name]
            includes.with_suffix(".json").write_text(json.dumps(manifest))
            with patch.dict(os.environ, environment), self.assertRaisesRegex(ValueError, "scheduling costs"):
                jobs.run(root, includes, "canonical-kafka", "2.2", 4, "-Xmx1g")
            self.assertFalse((root / "prepared").exists())


if __name__ == "__main__":
    unittest.main()
