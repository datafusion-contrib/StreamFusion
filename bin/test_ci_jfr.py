"""Keep sampled stack attribution distinct from compiler activity and allocation estimates."""

from collections import Counter, defaultdict
import io
import unittest

import ci_jfr


class JfrTest(unittest.TestCase):
    def test_recursive_stack_samples_are_not_counted_twice_inclusive(self):
        frame = ('<struct><struct name="method"><struct name="type">'
                 '<value name="name">example/Recursive</value></struct>'
                 '<value name="name">call</value></struct></struct>')
        sample = ('<struct name="sampledThread"><value name="javaName">main</value></struct>'
                  '<struct name="stackTrace"><array name="frames">' + frame * 2 + '</array></struct>')
        xml = ('<recording><events><event type="jdk.ExecutionSample">' + sample + '</event>'
               '<event type="jdk.NativeMethodSample">' + sample + '</event>'
               '<event type="jdk.Compilation"><value name="compiler">c2</value>'
               '<value name="duration">PT1M2.5S</value></event>'
               '<event type="jdk.ObjectAllocationSample"><struct name="objectClass">'
               '<value name="name">[B</value></struct><value name="weight">4096</value></event>'
               '</events></recording>')
        counters = defaultdict(Counter)
        ci_jfr.collect(io.StringIO(xml), counters)
        self.assertEqual(2, counters["leaf_frames"]["example.Recursive.call"])
        self.assertEqual(2, counters["inclusive_frames"]["example.Recursive.call"])
        self.assertEqual(2, counters["sampled_threads"]["main"])
        self.assertEqual(1, counters["compilation_events"]["c2"])
        self.assertEqual(62.5, counters["compilation_activity_seconds"]["c2"])
        self.assertEqual(4096, counters["allocation_sample_weight_bytes"]["[B"])

    def test_invalid_activity_durations_fail_instead_of_becoming_zero(self):
        self.assertEqual(0.125, ci_jfr.duration("PT0.125S"))
        self.assertEqual(3602, ci_jfr.duration("PT1H2S"))
        with self.assertRaises(ValueError):
            ci_jfr.duration("not a duration")


if __name__ == "__main__":
    unittest.main()
