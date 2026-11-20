"""Prime one dependency-only repository per JDK before CI consumers fan out."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import platform
import shutil
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[3]
MARKER = ".streamfusion-ci-dependencies.json"


def recipes(jdk):
    os_name = {"Linux": "linux", "Darwin": "osx"}[platform.system()]
    architecture = {"x86_64": "x86_64", "arm64": "aarch_64", "aarch64": "aarch_64"}[platform.machine()]
    classifier = f"{os_name}-{architecture}"
    versions = {entry.text.strip() for entry in ET.parse(ROOT / "pom.xml").iter(
        "{http://maven.apache.org/POM/4.0.0}protobuf.version")}
    result = [["--artifact", f"com.google.protobuf:protoc:{version}:exe:{classifier}"]
              for version in sorted(versions)]
    for line in ("", ",flink-1.18"):
        result.append(["--profile", line.lstrip(",")])
        result.append(["--profile", "canonical-kafka" + line])
        result.append(["--profile", "paimon" + line,
                       "--projects", "streamfusion-paimon", "--also-make"])
        if jdk == "17":
            result.append(["--profile", "paimon,tpcds" + line,
                           "--projects", "streamfusion-paimon", "--also-make"])
    result.extend([
        ["--profile", "hudi,flink-1.18", "--projects", "streamfusion-hudi", "--also-make"],
        ["--profile", "delta", "--projects", "streamfusion-delta", "--also-make"],
        ["--pom", "dev/flink-suite/agent/pom.xml"],
        ["--artifact", "org.apache.maven.surefire:surefire-junit-platform:3.2.5",
         "--artifact", "org.apache.maven.surefire:surefire-junit-platform:3.5.3",
         "--artifact", "com.google.protobuf:protobuf-java:2.5.0"],
    ])
    if jdk == "17":
        result.extend([
            ["--model-only", "--profile", "hudi,flink-1.18,hudi-upstream",
             "--projects", "streamfusion-hudi", "--also-make"],
            ["--model-only", "--pom", "dev/flink-suite/hudi-fixtures/pom.xml"],
            ["--profile", "iceberg,iceberg-upstream", "--projects", "streamfusion-iceberg", "--also-make"],
            *[["--profile", profile, "--projects", "streamfusion-doris", "--also-make"]
              for profile in ("doris,doris-it", "doris,doris-upstream", "doris,doris-it,doris-upstream")],
            ["--profile", "image-it"],
            ["--profile", "image-it,flink-1.18"],
            ["--profile", "paimon,upstream-paimon-compaction", "--projects", "streamfusion-paimon", "--also-make"],
        ])
        release = json.loads((ROOT / "dev/flink-suite/paimon-compaction-releases.json").read_text())["2.1.0"]
        result.append([item for artifact in release["fixture_sha256"]
                       for item in ("--artifact", f"org.apache.paimon:{artifact}:2.1.0:jar:tests")])
    return result


def metadata(key, jdk):
    return {"key": key, "jdk": jdk, "recipes": recipes(jdk)}


def graph_artifacts(node):
    result = set()
    if node["groupId"] != "tech.streamfusion":
        kind = node["type"]
        classifier = node.get("classifier", "")
        if kind == "test-jar":
            kind, classifier = "jar", classifier or "tests"
        elif kind in ("bundle", "maven-plugin"):
            kind = "jar"
        coordinate = (node["groupId"], node["artifactId"], node["version"], kind, classifier)
        result.add(coordinate)
    for child in node.get("children", []):
        result.update(graph_artifacts(child))
    return result


def complete_graph(repository, recipe):
    # go-offline traverses original POMs; compilation uses Maven's mediated graph.
    # Resolve that graph too, without building/installing our reactor artifacts.
    paths = [ROOT / "target/ci-maven-graph.json", ROOT / "dev/flink-suite/agent/target/ci-maven-graph.json"]
    paths.extend(ROOT.glob("streamfusion-*/target/ci-maven-graph.json"))
    if "--pom" in recipe:
        paths.append((ROOT / recipe[recipe.index("--pom") + 1]).parent / "target/ci-maven-graph.json")
    for path in paths:
        path.unlink(missing_ok=True)
    subprocess.run([sys.executable, str(ROOT / "bin/ci-maven-dependencies.py"),
                    "--repository", str(repository),
                    *(value for value in recipe if value != "--model-only"), "--graph"],
                   cwd=ROOT, check=True)
    paths = [ROOT / "target/ci-maven-graph.json", ROOT / "dev/flink-suite/agent/target/ci-maven-graph.json"]
    paths.extend(ROOT.glob("streamfusion-*/target/ci-maven-graph.json"))
    if "--pom" in recipe:
        paths.append((ROOT / recipe[recipe.index("--pom") + 1]).parent / "target/ci-maven-graph.json")
    artifacts = set()
    for path in paths:
        if path.exists():
            artifacts.update(graph_artifacts(json.loads(path.read_text())))
            path.unlink()
    if not artifacts:
        raise ValueError("Maven produced no external dependency graph")
    missing, missing_paths = [], []
    for group, artifact, version, kind, classifier in sorted(artifacts):
        filename = artifact + "-" + version + ("-" + classifier if classifier else "") + "." + kind
        path = repository / group.replace(".", "/") / artifact / version / filename
        if not path.is_file():
            missing_paths.append(path)
            missing.extend(["--artifact", ":".join([group, artifact, version, kind, *([classifier] if classifier else [])])])
    if missing:
        print(f"Completing {len(missing) // 2} mediated dependency artifacts", flush=True)
        subprocess.run([sys.executable, str(ROOT / "bin/ci-maven-dependencies.py"),
                        "--repository", str(repository), "--no-transitive", *missing], cwd=ROOT, check=True)
        if any(not path.is_file() for path in missing_paths):
            raise ValueError("Maven did not populate every selected dependency artifact")


def prepare(repository, key, jdk):
    repository.mkdir(parents=True, exist_ok=True)
    marker = repository / MARKER
    marker.unlink(missing_ok=True)
    for recipe in recipes(jdk):
        subprocess.run([sys.executable, str(ROOT / "bin/ci-maven-dependencies.py"),
                        "--repository", str(repository), *recipe], cwd=ROOT, check=True)
        if "--artifact" not in recipe:
            complete_graph(repository, recipe)
    marker.write_text(json.dumps(metadata(key, jdk), indent=2) + "\n")


def verify(repository, key, jdk):
    if json.loads((repository / MARKER).read_text()) != metadata(key, jdk):
        raise ValueError("Maven cache does not match the complete dependency preparation")


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def pack(repository, archive, key, jdk):
    if archive.is_relative_to(repository):
        raise ValueError('The transfer archive must be outside the Maven repository')
    verify(repository, key, jdk)
    files = []
    with tarfile.open(archive, 'w') as output:
        for path in sorted(repository.rglob('*')):
            relative = path.relative_to(repository)
            if relative.parts[:2] == ('tech', 'streamfusion'):
                continue
            if path.is_symlink():
                raise ValueError(f'Maven dependency cache contains a symbolic link: {relative}')
            if path.is_file() and not path.name.endswith(('.lastUpdated', '.part', '.lock')):
                output.add(path, arcname=relative.as_posix(), recursive=False)
                files.append(path)
    archive.with_name(archive.name + '.sha256').write_text(digest(archive) + '\n')
    print(f'Shared Maven dependencies: {len(files)} files, {sum(p.stat().st_size for p in files)} bytes')


def restore(repository, archive, key, jdk):
    if repository.exists() and any(repository.iterdir()):
        raise ValueError('Restore requires a fresh Maven repository; existing local artifacts are preserved')
    expected = archive.with_name(archive.name + '.sha256').read_text().strip()
    if len(expected) != 64 or digest(archive) != expected:
        raise ValueError('Shared Maven dependency checksum mismatch')
    with tarfile.open(archive, 'r') as source:
        marker = json.load(source.extractfile(MARKER))
        if marker != metadata(key, jdk):
            raise ValueError('Shared Maven dependency inputs do not match this consumer')
        seen = set()
        for member in source.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or '..' in path.parts or path in seen or not member.isfile():
                raise ValueError(f'Invalid Maven dependency archive member: {member.name}')
            seen.add(path)
        repository.mkdir(parents=True, exist_ok=True)
        for member in source.getmembers():
            target = repository / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as content, target.open('wb') as destination:
                shutil.copyfileobj(content, destination)
    verify(repository, key, jdk)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "verify", "pack", "restore", "complete-graph"))
    parser.add_argument("--repository", type=Path, default=Path.home() / ".m2/repository")
    parser.add_argument("--key")
    parser.add_argument("--jdk", choices=("11", "17"))
    parser.add_argument("--archive", type=Path)
    args, recipe = parser.parse_known_args()
    if args.command == "complete-graph":
        if recipe[:1] != ["--"]:
            parser.error("complete-graph requires Maven recipe options after --")
        complete_graph(args.repository.resolve(), recipe[1:])
        return
    if recipe:
        parser.error("unrecognized arguments: " + " ".join(recipe))
    if args.key is None or args.jdk is None:
        parser.error(f"{args.command} requires --key and --jdk")
    if args.command in ('pack', 'restore'):
        if not args.archive:
            parser.error('pack and restore require --archive')
        operation = pack if args.command == 'pack' else restore
        operation(args.repository.resolve(), args.archive.resolve(), args.key, args.jdk)
    else:
        operation = prepare if args.command == "prepare" else verify
        operation(args.repository.resolve(), args.key, args.jdk)


if __name__ == "__main__":
    main()
