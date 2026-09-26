from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

import runtime_shards


class RuntimeShardsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.classes = self.root / "classes"
        self.classes.mkdir()
        self.baseline = self.root / "baseline.tsv"
        rows = ["class\tseconds\ttests"]
        for index, seconds in enumerate((100, 80, 60, 40, 20, 10, 5, 1)):
            name = f"org.apache.flink.C{index}ITCase"
            path = self.classes / (name.replace(".", "/") + ".class")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
            rows.append(f"{name}\t{seconds}\t3")
        self.baseline.write_text("\n".join(rows) + "\n")
        self.plan = runtime_shards.make_plan(self.classes, self.baseline)

    def audit(self, shard):
        return {"validation_problems": [], "testcases": [
            {"test": name + "#testRows", "outcome": outcome}
            for name in self.plan["shards"][shard - 1]
            for outcome in ("passed", "passed", "skipped")
        ]}

    def test_deterministic_complete_balance_and_new_classes(self):
        self.assertEqual(self.plan, runtime_shards.make_plan(self.classes, self.baseline))
        groups = self.plan["shards"]
        self.assertEqual(8, len({name for group in groups for name in group}))
        self.assertEqual(8, sum(map(len, groups)))
        self.assertLess(max(self.plan["estimated_seconds"]), 120)
        (self.classes / "NewITCase.class").touch()
        (self.classes / "Outer$NestedITCase.class").touch()
        (self.classes / "UnitTest.class").touch()
        updated = runtime_shards.make_plan(self.classes, self.baseline)
        self.assertEqual(9, sum(map(len, updated["shards"])))
        self.assertIn("NewITCase", {name for group in updated["shards"] for name in group})

    def test_missing_compiled_class_cannot_shrink_selection(self):
        next(self.classes.rglob("*ITCase.class")).unlink()
        with self.assertRaisesRegex(ValueError, "missing"):
            runtime_shards.make_plan(self.classes, self.baseline)

    def test_missing_contracted_class_cannot_escape_shard_checks(self):
        contracts = self.root / "contracts.tsv"
        contracts.write_text("org.apache.flink.MissingITCase#testRows\t*\tNativeCalcOperator\n")
        with self.assertRaisesRegex(ValueError, "Contracted runtime classes are missing"):
            runtime_shards.make_plan(self.classes, self.baseline, contracts)

    def test_parameter_invocations_and_skips_are_counted(self):
        audit = self.audit(1)
        self.assertEqual(3 * len(self.plan["shards"][0]),
                         sum(runtime_shards.verify(self.plan, 1, audit)["cases"].values()))
        audit["testcases"].pop()
        with self.assertRaisesRegex(ValueError, "expected 3"):
            runtime_shards.verify(self.plan, 1, audit)

    def test_other_shards_tests_and_invalid_evidence_fail(self):
        with self.assertRaisesRegex(ValueError, "outside this shard"):
            runtime_shards.verify(self.plan, 1, self.audit(2))
        audit = self.audit(1)
        audit["validation_problems"] = ["missing native witness"]
        with self.assertRaisesRegex(ValueError, "validation failures"):
            runtime_shards.verify(self.plan, 1, audit)

    def test_aggregate_requires_all_four_unique_matching_shards(self):
        for shard in range(1, 5):
            directory = self.root / str(shard)
            directory.mkdir()
            coverage = runtime_shards.verify(self.plan, shard, self.audit(shard))
            (directory / "shard-coverage.json").write_text(json.dumps(coverage))
        self.assertEqual(24, runtime_shards.aggregate(self.plan, self.root))
        path = self.root / "4/shard-coverage.json"
        saved = path.read_text()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "Missing runtime shards"):
            runtime_shards.aggregate(self.plan, self.root)
        wrong = json.loads(saved)
        wrong["shard"] = 3
        path.write_text(json.dumps(wrong))
        with self.assertRaises(ValueError):
            runtime_shards.aggregate(self.plan, self.root)
        wrong = json.loads(saved)
        wrong["plan"] = "old build"
        path.write_text(json.dumps(wrong))
        with self.assertRaisesRegex(ValueError, "different runtime plan"):
            runtime_shards.aggregate(self.plan, self.root)

    def test_plan_rejects_duplicate_classes(self):
        plan = deepcopy(self.plan)
        plan["shards"][1].append(plan["shards"][0][0])
        path = self.root / "plan.json"
        path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "multiple shards"):
            runtime_shards.read_plan(path)


if __name__ == "__main__":
    unittest.main()
