#!/usr/bin/env python3
"""Run unchanged released Paimon compaction fixtures and verify per-invocation work."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
RELEASES = json.loads((ROOT / "dev/flink-suite/paimon-compaction-releases.json").read_text())


def case_key(class_name, case):
    invocation = re.search(r"\[(\d+)\]$", case)
    method = case.split("(")[0].split("[")[0]
    return class_name + "#" + method + (f"[{invocation[1]}]" if invocation else "")


def inventory_key(record):
    source = record["source"]
    class_name = re.search(r"className = '([^']+)'", source)[1]
    method = re.search(r"methodName = '([^']+)'", source)[1]
    invocation = re.search(r"test-template-invocation:#(\d+)", record["junit_id"])
    return class_name + "#" + method + (f"[{invocation[1]}]" if invocation else "")


def validate(expected, output, mode, version):
    outcomes = {}
    for path in (output / "junit").glob("TEST-*.xml"):
        suite = ET.parse(path).getroot()
        for case in suite.findall("testcase"):
            key = case_key(case.attrib["classname"], case.attrib["name"])
            assert key not in outcomes, f"Duplicate test result: {key}"
            assert not any(case.find(tag) is not None for tag in ("skipped", "error", "failure")), key
            outcomes[key] = case
    assert outcomes.keys() == expected.keys(), ("JUnit case mismatch", expected.keys() - outcomes.keys(), outcomes.keys() - expected.keys())
    inventory = {}
    for path in (output / "inventory").glob("*.json"):
        record = json.loads(path.read_text())
        key = inventory_key(record)
        assert key not in inventory, f"Duplicate inventory: {key}"
        assert record["junit_status"] == "SUCCESSFUL", key
        inventory[key] = record
    assert inventory.keys() == expected.keys(), ("Inventory case mismatch", expected.keys() - inventory.keys(), inventory.keys() - expected.keys())
    rows = []
    for key, contract in expected.items():
        record = inventory[key]
        work = record.get("compactions", [])
        native = [item for item in work if item["native"]]
        stock = [item for item in work if item.get("fallback")]
        failures = [item for item in work if item.get("failure")]
        if mode == "stock":
            assert not work, f"Stock baseline used the native decorator: {key}"
        else:
            expectation = contract["expectation"]
            if expectation in ("native", "mixed"):
                assert native, f"Missing native rewrite: {key}"
            if expectation in ("fallback", "mixed"):
                assert stock, f"Missing required Java delegation: {key}"
            if expectation == "no-rewrite":
                assert not work, f"Unexpected payload rewrite in selection/upgrade fixture: {key}"
            if expectation in ("native", "selected-native"):
                assert not stock, f"Unexpected fallback: {key}: {stock}"
            if expectation in ("fallback", "stock-policy", "mixed"):
                if expectation != "mixed":
                    assert not native, f"Unsupported policy ran natively: {key}"
                reasons = contract["fallback_reasons"].split("|")
                assert all(item["fallback"] in reasons for item in stock), (key, stock)
            for item in native:
                assert item.get("before_files", 0) > 0 and item.get("before_bytes", 0) > 0, (key, item)
            for item in failures:
                assert ("testExtractAndRecoverState" in key or "testStreamingCompactConflictWithOverwrite" in key or "testAsyncCompactionFileDeletedWhenShutdown" in key) and any(
                    reason in item["failure"] for reason in ("InterruptedIOException", "ClosedByInterruptException", "InterruptedException")), (key, item)
            assert all(item["native"] or item.get("fallback") or item.get("failure") for item in work), (key, work)
        rows.append({"mode": mode, "case": key, "display_name": record["display_name"],
                     "result": record["junit_status"], "expectation": contract["expectation"],
                     "native_groups": len(native), "stock_groups": len(stock), "interrupted_groups": len(failures),
                     "input_files": sum(item.get("before_files", 0) for item in native),
                     "input_bytes": sum(item.get("before_bytes", 0) for item in native),
                     "output_rows": sum(item.get("after_rows", 0) for item in native),
                     "input_formats": "|".join(sorted({fmt for item in native for fmt in item.get("before_formats", [])})),
                     "fallback_reasons": "|".join(sorted({item["fallback"] for item in stock})),
                     "junit_id": record["junit_id"]})
    with (output / "results.csv").open("w") as report:
        writer = csv.DictWriter(report, fieldnames=rows[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Paimon {version} {mode}: {len(rows)} cases, zero skips, {sum(row['native_groups'] for row in rows)} native groups", flush=True)


def run(command):
    print("+ " + " ".join(map(str, command)), flush=True)
    subprocess.run(list(map(str, command)), cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paimon-version", choices=RELEASES, default="2.1.0")
    parser.add_argument("--mode", choices=("stock", "native", "both"), default="both")
    parser.add_argument("--skip-native-build", action="store_true")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--validate-only", type=Path, help="Validate a retained mode directory without rerunning tests")
    args = parser.parse_args()
    spec = RELEASES[args.paimon_version]
    cases = ROOT / "dev/flink-suite" / spec["manifest"]
    with cases.open() as manifest:
        expected = {case_key(row["class"], row["case"]): row for row in csv.DictReader(manifest)}
    assert len(expected) == spec["case_count"], "The pinned upstream corpus must retain every invocation"
    assert expected.keys().isdisjoint(spec.get("excluded", {})), "An excluded fixture is in the acceptance corpus"
    if args.validate_only:
        assert args.mode != "both", "Select the retained directory's mode"
        validate(expected, args.validate_only, args.mode, args.paimon_version)
        return
    module = spec["module"]
    output = ROOT / module / "target/upstream-compaction" / args.paimon_version
    fixtures = output / "fixtures"
    for artifact, digest in spec["fixture_sha256"].items():
        run(["mvn", "-B", "-ntp", "-N", "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:copy",
             f"-Dartifact=org.apache.paimon:{artifact}:{args.paimon_version}:jar:tests", f"-DoutputDirectory={fixtures}"])
        jar = fixtures / f"{artifact}-{args.paimon_version}-tests.jar"
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest, f"Fixture provenance mismatch: {artifact}"
    run(["mvn", "-B", "-ntp", "-f", ROOT / "dev/flink-suite/agent/pom.xml", "package"])
    agent = ROOT / "dev/flink-suite/agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
    methods = {}
    for row in expected.values():
        methods.setdefault(row["class"], set()).add(row["case"].split("(")[0].split("[")[0])
    tests = ",".join(name + "#" + "+".join(sorted(selected)) for name, selected in sorted(methods.items()))
    runtime = output / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    for mode in (["stock", "native"] if args.mode == "both" else [args.mode]):
        selected = output / mode
        if selected.exists():
            shutil.rmtree(selected)
        (selected / "junit").mkdir(parents=True)
        extra = (f'"-javaagent:{agent}" -Dstreamfusion.paimon.compaction.mode={mode} '
                 f'"-Dstreamfusion.flink-suite.sql-inventory={selected / "inventory"}" '
                 f'"-Djava.io.tmpdir={runtime}" -Dstreamfusion.flink-suite.flink-line={spec["flink_line"]}')
        run(["mvn", "-B", "-ntp", "-P" + ",".join(spec["profiles"]) + (",bench" if args.release else ""),
             "-pl", module, "-am", "-Dsurefire.failIfNoSpecifiedTests=false",
             f"-Dtest={tests}", f"-Dnative.build.skip={str(args.skip_native_build).lower()}",
             f"-Dstreamfusion.flink-suite.sql-inventory={selected / 'inventory'}",
             "-Dmaven.javadoc.skip=true", f"-Dsf.extraJvmArgs={extra}", "test"])
        for name in {row["class"] for row in expected.values()}:
            source = ROOT / module / f"target/surefire-reports/TEST-{name}.xml"
            shutil.copy2(source, selected / "junit" / source.name)
        validate(expected, selected, mode, args.paimon_version)


if __name__ == "__main__":
    main()
