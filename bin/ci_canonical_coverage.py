"""Audit the selected canonical matrix independently of its Java selection algorithm."""

from collections import Counter
import argparse
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


QUARANTINE = "CI_QUARANTINE_LEGACY_KAFKA_TASK_FAILURE"
KAFKA_CLASS = "tech.streamfusion.CanonicalKafkaPipelineFailureJobTest"
KAFKA_METHOD = "preservesExactlyOnceSinkEffectsAcrossRepeatedCanonicalSwitches"


def interactions(rows, axes):
    result = set()
    for row in rows:
        values = [json.dumps(value, sort_keys=True) for value in row]
        for first in range(len(row)):
            for second in range(first, len(row)):
                result.add((first, values[first], second, values[second]))
        if axes:
            result.add(tuple(item for axis in axes for item in (axis, values[axis])))
    return result


def load_inventory(path):
    data = json.loads(path.read_text())
    if data.get("schema_version") != 1 or data.get("line") not in ("1.18", "2.2") or data.get("jdk") not in (11, 17):
        raise ValueError("Invalid canonical inventory identity")
    totals = Counter()
    for name, fixture in data["classes"].items():
        counts = Counter()
        for method, selection in fixture["methods"].items():
            full, selected = selection["full"], selection["representative"]
            if not 0 < selected <= full:
                raise ValueError(f"Empty or inflated canonical selection: {name}#{method}")
            rows = selection.get("full_arguments")
            if rows is not None:
                indices = selection["selected_indices"]
                axes = selection["recovery_axes"]
                if (len(rows) != full or len(indices) != selected or len(set(indices)) != selected
                        or any(not isinstance(i, int) or i < 0 or i >= full for i in indices)
                        or len({len(row) for row in rows}) != 1
                        or axes and (len(axes) != 3 or len(set(axes)) != 3
                                     or any(axis < 0 or axis >= len(rows[0]) for axis in axes))):
                    raise ValueError(f"Invalid canonical argument inventory: {name}#{method}")
                if interactions(rows, axes) != interactions([rows[i] for i in indices], axes):
                    raise ValueError(f"Canonical interactions were lost: {name}#{method}")
            elif full != selected:
                raise ValueError("Uninventoried providers cannot reduce their cases")
            counts.update(full=full, representative=selected)
        if fixture["abstract"] != (not fixture["methods"]):
            raise ValueError(f"Concrete canonical class has no test methods: {name}")
        if any(fixture[key] != counts[key] for key in ("full", "representative")):
            raise ValueError(f"Canonical fixture totals differ: {name}")
        totals.update(counts)
    if not totals["representative"] or any(data[key + "_cases"] != totals[key] for key in ("full", "representative")):
        raise ValueError("Canonical inventory totals differ")
    return data


def costs(durations, inventory, coverage):
    if coverage not in ("full", "representative"):
        raise ValueError("Unknown canonical coverage")
    result = dict(durations)
    for name, fixture in inventory["classes"].items():
        path = name.replace(".", "/")
        if fixture["full"]:
            result[path] = max(1.0, result.get(path, 1.0) * fixture[coverage] / fixture["full"])
    return result


def audit(selection, inventory_path, reports, coverage):
    inventory = load_inventory(inventory_path)
    digest = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
    if selection.get("coverage_inventory_sha256") != digest or selection.get("coverage") != coverage:
        raise ValueError("Canonical selection does not match its coverage inventory")
    expected, quarantines = set(), set()
    for path in selection["classes"]:
        name = path.replace("/", ".")
        fixture = inventory["classes"][name]
        for method, contract in fixture["methods"].items():
            if contract["kind"] == "test":
                expected.add((name, method, None))
                continue
            for index in range(1, contract[coverage] + 1):
                key = (name, method, index)
                expected.add(key)
                if (name == KAFKA_CLASS and method == KAFKA_METHOD
                        and inventory["line"] == "1.18" and inventory["jdk"] == 17):
                    raw = index - 1 if coverage == "full" else contract["selected_indices"][index - 1]
                    if contract["full_arguments"][raw] == ["task-failure", False, False, "incrementing"]:
                        quarantines.add(key)
    if not expected:
        raise ValueError("Canonical shard has no expected invocations")
    observed, skipped = set(), set()
    for report in reports.glob("TEST-*.xml"):
        suite = ET.parse(report).getroot()
        for case in suite.iter("testcase"):
            text = case.attrib["name"]
            invocation = re.search(r"\[(\d+)\]$", text)
            key = (case.attrib["classname"], text.split("(")[0].split("[")[0],
                   int(invocation[1]) if invocation else None)
            if key in observed or key not in expected:
                raise ValueError(f"Duplicate or unexpected canonical invocation: {key}")
            observed.add(key)
            if any(case.find(tag) is not None for tag in ("failure", "error")):
                raise ValueError(f"Failed canonical invocation: {key}")
            skip = case.find("skipped")
            if skip is not None:
                if key not in quarantines or QUARANTINE not in ET.tostring(skip, encoding="unicode"):
                    raise ValueError(f"Unexpected skipped canonical invocation: {key}")
                skipped.add(key)
    if expected != observed or skipped != quarantines:
        raise ValueError(f"Incomplete canonical outcomes: missing {len(expected - observed)}; quarantine mismatch {skipped ^ quarantines}")
    result = dict(coverage=coverage, expected_cases=len(expected), passed=len(observed - skipped),
                  quarantined=len(skipped), inventory_sha256=digest,
                  classes=len(selection["classes"]), all_selected_invocations_verified=True)
    print(json.dumps(result, sort_keys=True))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--line', choices=('1.18', '2.2'), required=True)
    parser.add_argument('--jdk', choices=(11, 17), type=int, required=True)
    args = parser.parse_args()
    data = load_inventory(args.inventory)
    if data['line'] != args.line or data['jdk'] != args.jdk:
        parser.error('Canonical inventory does not match the preparation cohort')
    print(json.dumps(dict(line=data['line'], jdk=data['jdk'],
                          full_cases=data['full_cases'], representative_cases=data['representative_cases'],
                          classes=len(data['classes']),
                          inventory_sha256=hashlib.sha256(args.inventory.read_bytes()).hexdigest())))
