import unittest
from collections import Counter

from suite_matrix import matrix


class SuiteMatrixTest(unittest.TestCase):
    def cases(self, inventory=False, scope="all"):
        return Counter((entry["line"], suite, entry["shard"])
                       for entry in matrix(inventory, scope)["include"]
                       for suite in entry["suites"].split())

    def test_full_matrix_preserves_every_suite_and_shard_once(self):
        expected = Counter()
        for line in ("2.2", "1.18"):
            for shard in range(1, 5):
                expected[line, "runtime", str(shard)] += 1
            for suite in ("formats", "parquet", "orc", "kafka", "paimon", "state"):
                expected[line, suite, ""] += 1
        expected["2.2", "delta", ""] += 1
        self.assertEqual(expected, self.cases())
        self.assertEqual(14, len(matrix()["include"]))
        for entry in matrix()["include"]:
            if entry["label"] == "connectors":
                self.assertEqual("formats", entry["suites"].split()[0],
                                 "Formats cleanup must precede the other format reports")
        self.assertEqual(expected, self.cases(True, "all"))
        self.assertEqual(expected, self.cases(False, "connectors"))

    def test_inventory_scopes_preserve_selection(self):
        full = self.cases()
        self.assertEqual(Counter({k: v for k, v in full.items() if k[1] == "runtime"}),
                         self.cases(True, "runtime"))
        self.assertEqual(Counter({k: v for k, v in full.items() if k[1] not in ("runtime", "state")}),
                         self.cases(True, "connectors"))

    def test_unknown_scope_is_rejected(self):
        with self.assertRaises(ValueError):
            matrix(True, "typo")


if __name__ == "__main__":
    unittest.main()
