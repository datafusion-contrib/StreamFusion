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

    def test_waits_and_gc_activity_remain_distinct_and_heap_is_a_maximum(self):
        events = [["jdk.ThreadPark", "main", 2.5, ["example.await", "example.run"]],
                  ["jdk.ThreadSleep", "worker", 40, ["example.retry"]],
                  ["jdk.GarbageCollection", "G1 / allocation", 0.25],
                  ["jdk.GCHeapSummary", "Before GC", 1000],
                  ["jdk.GCHeapSummary", "Before GC", 600]]
        counters = defaultdict(Counter)
        ci_jfr.collect(io.StringIO("\n".join(json.dumps(x) for x in events)), counters)
        self.assertEqual({"example.await > example.run": 2.5}, counters["main_wait_seconds_by_stack"])
        self.assertEqual(40, counters["wait_seconds_by_thread"]["worker"])
        self.assertEqual(0.25, counters["gc_activity_seconds"]["G1 / allocation"])
        self.assertEqual(1000, counters["gc_heap_used_max_bytes"]["Before GC"])


if __name__ == "__main__":
    unittest.main()
