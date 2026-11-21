"""Prepare and check publication inputs for the signed release workflow."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

POM = "http://maven.apache.org/POM/4.0.0"
ET.register_namespace("", POM)
ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

NATIVE_JOBS = {
    "linux-x86_64": "Build native payload (Linux x86_64)",
    "darwin-aarch64": "Build native payload (macOS Apple Silicon)",
}


def gh_api(repo, endpoint):
    return subprocess.check_output(
        ["gh", "api", "--allow-escape-sequences", f"repos/{repo}/{endpoint}"],
        text=True,
    )


def validate_native_run(repo, run_id, commit, version):
    if not re.fullmatch(r"[1-9][0-9]*", run_id):
        raise ValueError("The previous release run must be a numeric GitHub Actions run ID")
    path = f"actions/runs/{run_id}"
    run = json.loads(gh_api(repo, path))
    identity = (run["path"], run["head_branch"], run["head_repository"]["full_name"], run["status"])
    if identity != (".github/workflows/release.yml", "main", repo, "completed"):
        raise ValueError(f"Run {run_id} is not a completed canonical main release run")
    jobs = json.loads(gh_api(repo, f"{path}/jobs?filter=latest&per_page=100"))["jobs"]
    bundle_artifacts = json.loads(gh_api(repo, f"{path}/artifacts?per_page=100"))["artifacts"]
    result = []
    for platform, task in NATIVE_JOBS.items():
        name = f"native-{platform}-{version}"
        artifacts = [item for item in bundle_artifacts if item["name"] == name and not item["expired"]]
        producers = [item for item in jobs if item["name"] == task and item["conclusion"] == "success"]
        if len(artifacts) != 1 or len(producers) != 1:
            raise ValueError(f"Run {run_id} needs one successful build and unexpired artifact for {platform}")
        artifact, job = artifacts[0], producers[0]
        log = gh_api(repo, f"actions/jobs/{job['id']}/logs")
        checkout = re.search(r"git log -1 --format=%H\r?\n\S+ ([0-9a-f]{40})\r?$", log, re.MULTILINE)
        uploaded = f"Artifact {name}.zip successfully finalized. Artifact ID {artifact['id']}"
        digest = artifact.get("digest") or ""
        if not checkout or checkout[1] != commit or uploaded not in log:
            raise ValueError(f"{name} was not uploaded by a successful job using {commit}")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError(f"{name} has no valid SHA-256 digest")
        result.append({"id": artifact["id"], "sha256": digest.removeprefix("sha256:")})
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        print("native_artifacts=" + json.dumps(result, separators=(",", ":")), file=output)


def download_native_artifacts(repo, artifacts, destination):
    destination.mkdir(parents=True, exist_ok=True)
    for artifact in json.loads(artifacts):
        with tempfile.TemporaryFile() as downloaded:
            subprocess.run(
                ["gh", "api", "--allow-escape-sequences",
                 f"repos/{repo}/actions/artifacts/{artifact['id']}/zip"],
                stdout=downloaded, check=True,
            )
            downloaded.seek(0)
            if hashlib.file_digest(downloaded, "sha256").hexdigest() != artifact["sha256"]:
                raise ValueError(f"Native artifact {artifact['id']} failed its SHA-256 check")
            with zipfile.ZipFile(downloaded) as archive:
                for name in archive.namelist():
                    path = PurePosixPath(name)
                    if path.is_absolute() or ".." in path.parts:
                        raise ValueError(f"Invalid native artifact path {name}")
                archive.extractall(destination)


def element(parent, name, value=None):
    child = ET.SubElement(parent, f"{{{POM}}}{name}")
    child.text = value
    return child


def attach_runtime_documentation(repo, tag, directory):
    module = Path("streamfusion-runtime")
    if any((module / "src/main").rglob("*.java")):
        return
    # Central requires both classifiers for a published JAR, including a resource-only assembly.
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "README.md").write_text(
        "# StreamFusion integration-test assembly\n\n"
        "This module contains release resources and integration fixtures. "
        "Deployable StreamFusion APIs belong to the core and connector modules.\n\n"
        f"Release sources and tests: https://github.com/{repo}/tree/{tag}/streamfusion-runtime\n"
        f"Project documentation: https://github.com/{repo}/blob/{tag}/docs/development.md\n",
        encoding="utf-8",
    )
    pom = module / "pom.xml"
    tree = ET.parse(pom, parser=ET.XMLParser(target=ET.TreeBuilder(insert_comments=True)))
    [jar] = [plugin for plugin in tree.findall(f"./{{{POM}}}build/{{{POM}}}plugins/{{{POM}}}plugin")
             if plugin.findtext(f"{{{POM}}}artifactId") == "maven-jar-plugin"]
    executions = jar.find(f"{{{POM}}}executions")
    for classifier in ("sources", "javadoc"):
        identifier = f"attach-runtime-{classifier}"
        existing = executions.find(f"{{{POM}}}execution[{{{POM}}}id='{identifier}']")
        if existing is not None:
            executions.remove(existing)
        execution = element(executions, "execution")
        element(execution, "id", identifier)
        element(execution, "phase", "package")
        element(element(execution, "goals"), "goal", "jar")
        configuration = element(execution, "configuration")
        element(configuration, "classifier", classifier)
        element(configuration, "classesDirectory", str(directory.resolve()))
    tree.write(pom, encoding="utf-8", xml_declaration=True)


def check_attachments(version):
    modules = sorted(Path.cwd().glob("streamfusion-*/.flattened-pom.xml"))
    if not modules:
        raise ValueError("No flattened Maven modules found")
    for pom in modules:
        project = ET.parse(pom)
        if project.findtext(f"{{{POM}}}packaging") == "pom":
            continue
        artifact = project.findtext(f"{{{POM}}}artifactId")
        for classifier in ("sources", "javadoc"):
            attachment = pom.parent / "target" / f"{artifact}-{version}-{classifier}.jar"
            if not zipfile.is_zipfile(attachment):
                raise ValueError(f"Missing or invalid Central attachment: {attachment}")
            with zipfile.ZipFile(attachment) as archive:
                if any(name.endswith((".so", ".dylib")) for name in archive.namelist()):
                    raise ValueError(f"A native library was bundled in {attachment}")
    print(f"Verified Central sources and Javadoc attachments for {len(modules)} Maven modules.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    native = commands.add_parser("validate-native-run")
    for field in ("repo", "run_id", "commit", "version"):
        native.add_argument(field)
    download = commands.add_parser("download-native")
    download.add_argument("repo")
    download.add_argument("artifacts")
    download.add_argument("destination", type=Path)
    attach = commands.add_parser("attach-runtime-documentation")
    attach.add_argument("repo")
    attach.add_argument("tag")
    attach.add_argument("directory", type=Path)
    verify = commands.add_parser("check-attachments")
    verify.add_argument("version")
    arguments = vars(parser.parse_args())
    handlers = {
        "validate-native-run": validate_native_run,
        "download-native": download_native_artifacts,
        "attach-runtime-documentation": attach_runtime_documentation,
        "check-attachments": check_attachments,
    }
    handlers[arguments.pop("command")](**arguments)


if __name__ == "__main__":
    main()
