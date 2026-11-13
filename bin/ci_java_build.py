"""Share exact-commit canonical Java compilation without sharing test results."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import shutil
import subprocess
import tarfile
import tempfile


MODULES = ("streamfusion-core", "streamfusion-json", "streamfusion-runtime")


def identity(root, line):
    subprocess.run(["git", "-C", str(root), "diff", "--exit-code", "HEAD", "--"], check=True)
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else "java"
    environment = {key: value for key, value in os.environ.items()
                   if key not in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")}
    return {
        "commit": subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip(),
        "flink_line": line,
        "java": subprocess.check_output([java, "-fullversion"], stderr=subprocess.STDOUT,
                                         env=environment, text=True).strip(),
        "platform": platform.system(),
        "architecture": platform.machine(),
    }


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def require_classes(root):
    for module in MODULES:
        if not any((root / module / "target/classes").rglob("*.class")):
            raise ValueError(f"Missing compiled module: {module}")
    if not any((root / "streamfusion-runtime/target/test-classes").rglob("Canonical*JobTest.class")):
        raise ValueError("Missing compiled canonical test inventory")


def pack(root, line, archive):
    metadata = identity(root, line)
    require_classes(root)
    entries = []
    for module in MODULES:
        target = root / module / "target"
        entries.extend(path for path in (target / "classes", target / "test-classes") if path.is_dir())
    metadata["entries"] = [path.relative_to(root).as_posix() for path in entries]
    with tarfile.open(archive, "w:gz", compresslevel=1) as output:
        content = json.dumps(metadata).encode()
        header = tarfile.TarInfo("shared-java-build.json")
        header.size = len(content)
        output.addfile(header, io.BytesIO(content))
        for entry in entries:
            paths = [entry, *entry.rglob("*")] if entry.is_dir() else [entry]
            if any(path.is_symlink() for path in paths):
                raise ValueError(f"Shared Java build cannot contain symbolic links: {entry}")
            def shared_resource(member):
                path = PurePosixPath(member.name)
                native_resource = path.parts[3:6] == ("tech", "streamfusion", "native")
                return None if native_resource and path.suffix in (".so", ".dylib", ".dll") else member
            output.add(entry, arcname=entry.relative_to(root).as_posix(), filter=shared_resource)
    archive.with_name(archive.name + ".sha256").write_text(digest(archive) + "\n")


def restore(root, line, archive):
    expected = archive.with_name(archive.name + ".sha256").read_text().strip()
    if len(expected) != 64 or digest(archive) != expected:
        raise ValueError("Shared Java build checksum mismatch")
    current = identity(root, line)
    for module in MODULES:
        if (root / module / "target").exists():
            raise ValueError(f"Restore requires fresh module outputs: {module}")
    with tarfile.open(archive, "r:gz") as source:
        metadata = json.load(source.extractfile("shared-java-build.json"))
        for key, value in current.items():
            if metadata.get(key) != value:
                raise ValueError(f"Shared Java build {key} mismatch: built with {metadata.get(key)!r}, "
                                 f"runner has {value!r}")
        allowed = [PurePosixPath(entry) for entry in metadata["entries"]]
        for entry in allowed:
            if (entry.parts[:1] not in [(module,) for module in MODULES]
                    or len(entry.parts) != 3 or entry.parts[1] != "target"
                    or entry.name not in ("classes", "test-classes")):
                raise ValueError(f"Invalid build output: {entry}")
        with tempfile.TemporaryDirectory(prefix="shared-java-", dir=archive.parent) as directory:
            staged = Path(directory)
            seen = set()
            for member in source.getmembers():
                path = PurePosixPath(member.name)
                if member.name == "shared-java-build.json":
                    continue
                if (path.is_absolute() or ".." in path.parts or path in seen
                        or not any(path == entry or entry in path.parents for entry in allowed)
                        or not (member.isfile() or member.isdir())):
                    raise ValueError(f"Invalid shared build member: {member.name}")
                seen.add(path)
                target = staged / path
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.extractfile(member) as content, target.open("wb") as destination:
                        shutil.copyfileobj(content, destination)
            require_classes(staged)
            for module in MODULES:
                (root / module).mkdir(parents=True, exist_ok=True)
                shutil.move(staged / module / "target", root / module / "target")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("pack", "restore"))
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--line", choices=("1.18", "2.2"), required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    operation = pack if args.command == "pack" else restore
    operation(args.root.resolve(), args.line, args.archive.resolve())


if __name__ == "__main__":
    main()
