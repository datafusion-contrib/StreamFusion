"""Retain trial validation across legacy and bounded-domain benchmark output."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "bench_jvm_aggregates", Path(__file__).with_name("bench-jvm-aggregates.py")
)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


class SummariesTest(unittest.TestCase):
    def summary(self, domain="", stock="[0.4, 0.5, 0.6]"):
        return BENCH.summaries(
            "[jvm-aggregate] shape=collect key_type=string key_prefix_chars=16 "
            f"rows=200000 keys=64 {domain}stock=0.500000 sf=0.600000 "
            f"stock_trials={stock} sf_trials=[0.5, 0.6, 0.7]",
            3,
        )[0]

    def test_legacy_and_bounded_domains_retain_matching_trials(self):
        legacy = self.summary()
        for domain in (0, 8):
            current = self.summary(f"values={domain} ")
            self.assertEqual(domain, current.pop("values"))
            self.assertEqual({k: v for k, v in legacy.items() if k != "values"}, current)
        self.assertEqual(0, legacy["values"])

    def test_invalid_trials_still_fail(self):
        for trials in ("[0.5]", "[0, 0.5, 0.6]", "[0.4, 0.6, 0.7]"):
            with self.subTest(trials=trials), self.assertRaises(ValueError):
                self.summary("values=8 ", trials)

    def test_unrecognized_output_fails(self):
        with self.assertRaisesRegex(ValueError, "no typed benchmark result"):
            BENCH.summaries("BUILD SUCCESS", 3)


if __name__ == "__main__":
    unittest.main()
