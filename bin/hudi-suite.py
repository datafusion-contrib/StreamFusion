#!/usr/bin/env python3
"""Run the released Hudi SQL fixtures unchanged against stock and StreamFusion."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "dev/flink-suite/hudi-fixtures"
UPSTREAM_CLASSES = {
    "org.apache.hudi.table." + name
    for name in (
        "ITTestHoodieDataSource",
        "TestHoodieTableSource",
        "TestHoodieFileGroupReaderOnFlink",
        "ITTestSchemaEvolutionBySQLWithDFSCatalog",
        "ITTestSchemaEvolutionBySQLWithHMSCatalog",
        "ITTestSchemaEvolution",
        "ITTestDynamicBucketStreamWrite",
        "ITTestBlobWrite",
        "ITTestVectorCrossEngineCompatibility",
    )
}


def results(reports):
    cases = {}
    for report in reports.glob("TEST-*.xml"):
        for case in ET.parse(report).getroot().findall("testcase"):
            key = case.attrib["classname"] + "#" + case.attrib["name"]
            assert key not in cases, f"Duplicate upstream invocation: {key}"
            status = next(
                (tag for tag in ("failure", "error", "skipped") if case.find(tag) is not None),
                "passed",
            )
            cases[key] = status
    assert cases, "No upstream Hudi invocations executed"
    return cases


def verify_provenance(repository):
    provenance = json.loads((FIXTURES / "provenance.json").read_text())
    for name, digest in provenance["artifacts"].items():
        artifact = name.removesuffix("-tests")
        classifier = "-tests" if name.endswith("-tests") else ""
        jar = (
            repository / "org/apache/hudi" / artifact / provenance["version"]
            / f"{artifact}-{provenance['version']}{classifier}.jar"
        )
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest, (
            f"Released fixture changed: {jar}"
        )
    for name, record in provenance["helpers"].items():
        helper = FIXTURES / "src/main/java" / name
        assert hashlib.sha256(helper.read_bytes()).hexdigest() == record["sha256"], (
            f"Upstream helper changed: {name}"
        )


def evidence(observations, cases):
    work = Counter()
    reasons = Counter()
    for observation in observations:
        work.update(observation.get("native_work", {}))
        for plan in observation.get("plans", []):
            reasons.update(plan.get("fallback_reasons", []))
    invocations = {}
    for observation in observations:
        key = observation["junit_id"]
        assert key not in invocations, f"Duplicate SQL inventory invocation: {key}"
        invocations[key] = {
            "status": observation["junit_status"],
            "display_name": observation["display_name"],
            "native_work": observation.get("native_work", {}),
            "fallback_reasons": sorted({
                reason
                for plan in observation.get("plans", [])
                for reason in plan.get("fallback_reasons", [])
            }),
        }
    assert len(invocations) == sum(status != "skipped" for status in cases.values()), (
        "Missing upstream invocation evidence"
    )
    return dict(work), dict(reasons), invocations


def native_contracts(invocations, contracts):
    for key, observation in invocations.items():
        assert key in contracts, f"Missing native execution contract: {key}"
        expected = contracts[key]
        work = observation["native_work"]
        assert set(work) == set(expected["native_work"]), (
            f"Native execution changed: {key}: {work}"
        )
        for operator, minimum in expected["native_work"].items():
            assert work[operator] >= minimum, f"Missing native work: {key}: {operator}"
        assert observation["fallback_reasons"] == expected["fallback_reasons"], (
            f"Unexpected fallback: {key}: {observation['fallback_reasons']}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("stock", "native", "both"), default="both")
    parser.add_argument("--tests", help="Focused Surefire selection; omit for the pinned SQL/source corpus")
    parser.add_argument(
        "--inventory", action="store_true",
        help="Discover execution contracts for a focused selection; does not produce a passing gate report",
    )
    parser.add_argument("--maven", default="mvn")
    parser.add_argument("--repository", type=Path, default=Path.home() / ".m2/repository")
    parser.add_argument("--native-library-dir", type=Path, help="Reuse prebuilt engine and Parquet libraries")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "streamfusion-hudi/target/hudi-upstream.json")
    args = parser.parse_args()
    if args.inventory and not args.tests:
        parser.error("--inventory requires an explicit --tests selection")
    summary = {
        "status": "running", "hudi": "1.2.1", "flink": "1.18.1",
        "selection": args.tests, "modes": {},
    }

    def write_summary():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(summary, indent=2) + "\n")

    def run(arguments, **kwargs):
        command = [
            args.maven, "-B", "-ntp", f"-Dmaven.repo.local={args.repository.resolve()}",
            *map(str, arguments),
        ]
        print("+ " + " ".join(command), flush=True)
        try:
            return subprocess.run(command, cwd=ROOT, check=True, **kwargs)
        except subprocess.CalledProcessError:
            summary["status"] = "failed"
            write_summary()
            raise

    write_summary()
    try:
        with tempfile.TemporaryDirectory(prefix="streamfusion-hudi-suite-") as temporary:
            scratch = Path(temporary)
            fixture_build = scratch / "fixtures"
            run(["-f", FIXTURES / "pom.xml", f"-Dfixtures.output={fixture_build}", "package"])
            verify_provenance(args.repository)
            classes = scratch / "classes"
            with zipfile.ZipFile(fixture_build / "hudi-upstream-fixtures-1.0.jar") as archive:
                archive.extractall(classes)
            agent_build = scratch / "agent"
            run([
                "-f", ROOT / "dev/flink-suite/agent/pom.xml",
                f"-Dsuite.agent.output={agent_build}", "-DskipTests", "package",
            ])
            agent = agent_build / "streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar"
            runtime = scratch / "runtime"
            runtime.mkdir()
            for mode in (["stock", "native"] if args.mode == "both" else [args.mode]):
                reports = scratch / mode / "reports"
                inventory = scratch / mode / "inventory"
                extra = (
                    f'"-javaagent:{agent}" "-Djava.io.tmpdir={runtime}" '
                    '-Dstreamfusion.flink-suite.flink-line=1.18 -Duser.timezone=UTC '
                    '-Dlog4j.configurationFile=log4j2-streamfusion-hudi.xml '
                )
                extra += f'"-Dderby.stream.error.file={runtime / "derby.log"}" '
                extra += (
                    f'"-Dstreamfusion.flink-suite.sql-inventory={inventory}" '
                    f'-Dstreamfusion.native.enabled={str(mode == "native").lower()}'
                )
                if args.native_library_dir:
                    extra += f' "-Djava.library.path={args.native_library_dir.resolve()}"'
                options = [
                    "-Phudi,flink-1.18,hudi-upstream" + (",bench" if args.release else ""),
                    "-pl", "streamfusion-hudi", "-am",
                    "-Dmaven.javadoc.skip=true", "-Dsf.testForks=1",
                    "-Dsurefire.failIfNoSpecifiedTests=false",
                    f"-Dhudi.fixtures.classes={classes}",
                    f"-Dhudi.upstream.reports={reports}", f"-Dsf.extraJvmArgs={extra}",
                ]
                if args.native_library_dir:
                    options += ["-Dnative.build.skip=true", "-Dnative.test.resource.skip=true"]
                selection = args.tests or ",".join(
                    sorted(name.rsplit(".", 1)[1] for name in UPSTREAM_CLASSES)
                )
                options.append(f"-Dtest={selection}")
                failure = None
                try:
                    run([*options, "test"])
                except subprocess.CalledProcessError as error:
                    failure = error
                if failure and not list(reports.glob("TEST-*.xml")):
                    raise failure
                cases = results(reports)
                if not args.tests:
                    assert {case.split("#")[0] for case in cases} == UPSTREAM_CLASSES, (
                        "Incomplete upstream Hudi class inventory"
                    )
                observations = [json.loads(path.read_text()) for path in inventory.glob("*.json")]
                work, reasons, invocations = evidence(observations, cases)
                summary["modes"][mode] = {
                    "cases": cases, "native_work": work, "fallback_reasons": reasons,
                    "invocations": invocations,
                }
                write_summary()
                if failure:
                    raise failure
                assert not any(status in ("failure", "error") for status in cases.values())
                if not args.tests:
                    expected = json.loads((FIXTURES / "invocations.json").read_text())
                    assert cases == expected["cases"], "Upstream Hudi invocation/status inventory changed"
                    assert set(invocations) == set(expected["junit_ids"]), (
                        "Upstream Hudi execution-evidence inventory changed"
                    )
                    assert set(expected["native_contracts"]) == set(expected["junit_ids"]), (
                        "Incomplete upstream Hudi execution contracts"
                    )
                if mode == "stock":
                    assert not work, f"Stock run performed native work: {work}"
                elif not args.inventory:
                    native_contracts(
                        invocations,
                        json.loads((FIXTURES / "invocations.json").read_text())["native_contracts"],
                    )
            if args.mode == "both":
                assert summary["modes"]["stock"]["cases"] == summary["modes"]["native"]["cases"], (
                    "Stock/native upstream invocation inventories differ"
                )
        summary["status"] = "inventory" if args.inventory else "passed"
        write_summary()
    except BaseException:
        summary["status"] = "failed"
        write_summary()
        raise
    print(f"Hudi upstream results: {args.output}")


if __name__ == "__main__":
    main()
