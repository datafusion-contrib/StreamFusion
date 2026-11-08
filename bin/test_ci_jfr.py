"""Keep sampled stack attribution distinct from compiler activity and allocation estimates."""

from collections import Counter, defaultdict
import io
import json
import unittest

import ci_jfr


class JfrTest(unittest.TestCase):
    def test_recursive_stack_samples_are_not_counted_twice_inclusive(self):
        frames = ["example.Recursive.call"] * 2
        events = [["jdk.ExecutionSample", "main", frames], ["jdk.NativeMethodSample", "main", frames],
                  ["jdk.Compilation", "c2", 62.5], ["jdk.ObjectAllocationSample", "[B", 4096]]
        counters = defaultdict(Counter)
        ci_jfr.collect(io.StringIO("\n".join(json.dumps(event) for event in events)), counters)
        for prefix in ("java", "native"):
            self.assertEqual(1, counters[prefix + "_leaf_frames"]["example.Recursive.call"])
            self.assertEqual(1, counters[prefix + "_inclusive_frames"]["example.Recursive.call"])
            self.assertEqual(1, counters[prefix + "_sampled_threads"]["main"])
        self.assertEqual(1, counters["compilation_events"]["c2"])
        self.assertEqual(62.5, counters["compilation_activity_seconds"]["c2"])
        self.assertEqual(4096, counters["allocation_sample_weight_bytes"]["[B"])

    def test_invalid_activity_durations_fail_instead_of_becoming_zero(self):
        with self.assertRaises(ValueError):
            ci_jfr.collect(io.StringIO('["jdk.Compilation", "c2", "not a duration"]'), defaultdict(Counter))


if __name__ == "__main__":
    unittest.main()
