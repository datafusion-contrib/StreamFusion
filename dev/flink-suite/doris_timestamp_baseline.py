#!/usr/bin/env python3
"""Admit only the unchanged timestamp assertion also failed by stock Doris SQL."""

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


CLASS = "org.apache.doris.flink.source.DorisSourceITCase"
METHOD = "testTableSourceTimestampFilterAndProjectionPushDown"
CASE = METHOD + "[useFlightSql: false, flightSqlPort: -1]"


def cases(directory):
    root = ET.parse(Path(directory) / ("TEST-" + CLASS + ".xml")).getroot()
    return {case.attrib["name"]: case for case in root.findall("testcase")
            if case.attrib["name"].startswith(METHOD + "[")}


def expected_failure(baseline, actual):
    controls = cases(baseline)
    observed = cases(actual)
    if len(controls) != 2 or controls.keys() != observed.keys():
        raise ValueError("Both unchanged timestamp fixture variants must execute")
    expected = None
    for name, control in controls.items():
        if control.find("error") is not None or control.find("skipped") is not None:
            raise ValueError("Stock timestamp control did not execute successfully")
        failure = control.find("failure")
        if failure is None:
            continue
        message = failure.attrib.get("message", "")
        if (name != CASE or not message.startswith("arrays first differed at element [0];")
                or "202[2-12-31T16:00, 2022-12-31T16]" not in message
                or "202[3-01-01T00:00, 2023-01-01T00]" not in message):
            raise ValueError("Unexpected stock timestamp failure")
        candidate = observed[name]
        if candidate.find("error") is not None or candidate.find("skipped") is not None:
            raise ValueError("StreamFusion timestamp case errored or was skipped")
        result = candidate.find("failure")
        if result is not None:
            if result.attrib.get("message") != message:
                raise ValueError("StreamFusion timestamp failure differs from stock")
            expected = CLASS + "#" + CASE
    return expected


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("actual", type=Path)
    args = parser.parse_args()
    expected = expected_failure(args.baseline, args.actual)
    if expected:
        print(expected)
