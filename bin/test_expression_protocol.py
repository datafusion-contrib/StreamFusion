import copy
import json
from pathlib import Path
import runpy
import unittest

ROOT = Path(__file__).resolve().parent.parent
GENERATOR = runpy.run_path(str(ROOT / "bin/generate-expression-protocol.py"))


class ExpressionProtocolTest(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((ROOT / "native/engine/expression-protocol.json").read_text())

    def test_checked_in_sources_match_the_manifest(self):
        for relative, source in GENERATOR["render"](self.manifest).items():
            self.assertEqual(source, (ROOT / relative).read_text(), relative)

    def test_duplicate_names_and_codes_are_rejected_in_each_namespace(self):
        for group in (*GENERATOR["ENUMS"], *GENERATOR["CODES"]):
            for field in ("name", "code"):
                broken = copy.deepcopy(self.manifest)
                broken[group][1][field] = broken[group][0][field]
                with self.assertRaisesRegex(ValueError, "Duplicate"):
                    GENERATOR["render"](broken)

    def test_retired_kinds_and_invalid_codes_cannot_be_assigned(self):
        broken = copy.deepcopy(self.manifest)
        broken["nodeKinds"].append({"name": "RETIRED", "code": 10})
        with self.assertRaisesRegex(ValueError, "retired"):
            GENERATOR["render"](broken)
        for invalid in (-1, "0", True):
            broken = copy.deepcopy(self.manifest)
            broken["castTargets"][0]["code"] = invalid
            with self.assertRaisesRegex(ValueError, "Invalid"):
                GENERATOR["render"](broken)

    def test_invalid_names_and_unknown_versions_are_rejected(self):
        for invalid in ("1TYPE", "MixedCase", "HAS-DASH"):
            broken = copy.deepcopy(self.manifest)
            broken["nodeKinds"][0]["name"] = invalid
            with self.assertRaisesRegex(ValueError, "Invalid"):
                GENERATOR["render"](broken)
        broken = copy.deepcopy(self.manifest)
        broken["version"] = 2
        with self.assertRaisesRegex(ValueError, "version"):
            GENERATOR["render"](broken)

    def test_existing_payload_codes_keep_their_meanings(self):
        kinds = {entry["name"]: entry["code"] for entry in self.manifest["nodeKinds"]}
        self.assertEqual(15, kinds["LIT_INTERVAL"])
        self.assertEqual(17, kinds["UDF"])
        self.assertEqual(20, kinds["DECIMAL_DIVIDE"])
        self.assertEqual(21, kinds["DECIMAL_MOD"])
        self.assertEqual(38, kinds["ROW_UDF"])
        self.assertEqual(47, kinds["DECIMAL_IF"])

    def test_all_frozen_wire_identities_survive_protocol_extensions(self):
        fixture = json.loads((ROOT / "streamfusion-runtime/src/test/resources/compact-protocol-v1.json").read_text())
        for group, frozen in fixture.items():
            actual = {entry["name"]: entry["code"] for entry in self.manifest[group]}
            for name, code in frozen.items():
                self.assertEqual(code, actual.get(name), (group, name))

    def test_aggregate_contexts_keep_their_overlapping_identities(self):
        group = {entry["name"]: entry["code"] for entry in self.manifest["groupAggregateKinds"]}
        window = {entry["name"]: entry["code"] for entry in self.manifest["windowAggregateKinds"]}
        self.assertEqual(9, group["SUM_DISTINCT"])
        self.assertEqual(9, window["RETRACT_SUM"])
        self.assertEqual(43, group["JSON_OBJECT_MERGE_NULL_ON_NULL"])
        self.assertEqual(43, window["COUNT_DISTINCT_WITH_COUNTS"])


if __name__ == "__main__":
    unittest.main()
