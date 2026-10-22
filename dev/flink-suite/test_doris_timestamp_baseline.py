import tempfile
import unittest
from pathlib import Path
import xml.etree.ElementTree as ET

import doris_timestamp_baseline as baseline


class DorisTimestampBaselineTest(unittest.TestCase):
    MESSAGE = ("arrays first differed at element [0]; expected:<+I[1, "
               "202[2-12-31T16:00, 2022-12-31T16]:00:00.000001]> but was:<+I[1, "
               "202[3-01-01T00:00, 2023-01-01T00]:00:00.000001]>")

    def write(self, directory, kind="failure", message=None):
        directory.mkdir()
        root = ET.Element("testsuite")
        case = ET.SubElement(root, "testcase", name=baseline.CASE)
        if kind:
            ET.SubElement(case, kind, message=self.MESSAGE if message is None else message)
        ET.SubElement(root, "testcase", name=baseline.METHOD +
                      "[useFlightSql: true, flightSqlPort: 9,611]")
        ET.ElementTree(root).write(directory / ("TEST-" + baseline.CLASS + ".xml"))

    def test_only_identical_stock_assertion_can_be_expected(self):
        with tempfile.TemporaryDirectory() as directory:
            stock, native = Path(directory) / "stock", Path(directory) / "native"
            self.write(stock)
            self.write(native)
            self.assertEqual(baseline.CLASS + "#" + baseline.CASE,
                             baseline.expected_failure(stock, native))

    def test_different_failures_and_errors_cannot_be_hidden(self):
        for kind, message in (("failure", "wrong result"), ("error", self.MESSAGE),
                              ("skipped", self.MESSAGE)):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                stock, native = Path(directory) / "stock", Path(directory) / "native"
                self.write(stock)
                self.write(native, kind, message)
                with self.assertRaises(ValueError):
                    baseline.expected_failure(stock, native)

    def test_passing_control_does_not_allow_a_native_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            stock, native = Path(directory) / "stock", Path(directory) / "native"
            self.write(stock, kind=None)
            self.write(native)
            self.assertIsNone(baseline.expected_failure(stock, native))
