"""Transfer a fresh common suite build between runners without transferring test evidence."""

import argparse
import json
import platform
from pathlib import Path
import subprocess


def identity(repository, version):
    return {
        "revision": subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
        ).strip(),
        "flink_version": version,
        "platform": platform.system(),
        "architecture": platform.machine(),
    }


def prepare(root, repository, version):
    required = [
        root / f"flink-{version}/.git",
        root / "m2",
        root / "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
        root / "streamfusion-classpath.txt",
        root / "runtime-shards.json",
    ]
    for path in required:
        if not path.exists():
            raise ValueError(f"Missing shared build input: {path}")
    libraries = sorted((root / "streamfusion-source/native/target/debug").glob("libstreamfusion*"))
    libraries = [path for path in libraries if path.suffix in (".so", ".dylib")]
    if not libraries:
        raise ValueError("Missing freshly built StreamFusion native libraries")
    classpath = root / "streamfusion-classpath.txt"
    paths = [Path(entry).resolve() for entry in classpath.read_text().strip().split(":")]
    for path in paths:
        path.relative_to(root.resolve())
        if not path.is_file():
            raise ValueError(f"Missing classpath artifact: {path}")
    classpath.write_text(":".join(map(str, paths)))
    metadata = {**identity(repository, version), "suite_root": str(root.resolve())}
    (root / "shared-build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # Keep executable bits and the clean pinned checkout. Cargo intermediates and copied
    # StreamFusion sources are unnecessary for test consumers and dominate archive size.
    entries = [f"flink-{version}", "m2", "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
               "streamfusion-classpath.txt", "runtime-shards.json", "shared-build.json"]
    entries += [str(path.relative_to(root)) for path in libraries]
    entries += [path.name for path in sorted(root.glob("*-unshaded.jar"))]
    return entries


def restore(root, repository, version):
    metadata = json.loads((root / "shared-build.json").read_text())
    for key, value in identity(repository, version).items():
        if metadata[key] != value:
            raise ValueError(f"Shared build {key} mismatch: {metadata[key]} != {value}")
    old = Path(metadata["suite_root"])
    classpath = root / "streamfusion-classpath.txt"
    relocated = []
    for entry in classpath.read_text().strip().split(":"):
        try:
            path = root.resolve() / Path(entry).relative_to(old)
        except ValueError as error:
            raise ValueError(f"Classpath entry outside the shared build: {entry}") from error
        if not path.is_file():
            raise ValueError(f"Missing relocated classpath artifact: {path}")
        relocated.append(str(path))
    classpath.write_text(":".join(relocated))
    metadata["suite_root"] = str(root.resolve())
    (root / "shared-build.json").write_text(json.dumps(metadata, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("pack", "restore"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--version", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "pack":
            entries = prepare(args.root, args.repository, args.version)
            subprocess.run(
                ["tar", "--zstd", "-cf", str(args.archive.resolve()),
                 "--exclude=surefire-reports", "--exclude=failsafe-reports",
                 "-C", str(args.root), *entries], check=True
            )
        else:
            if args.root.exists() and any(args.root.iterdir()):
                raise ValueError("Restore needs an empty suite directory; it must not inherit old evidence")
            args.root.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["tar", "--zstd", "-xf", str(args.archive.resolve()), "-C", str(args.root)], check=True
            )
            restore(args.root, args.repository, args.version)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Shared suite build failed: {error}\n")


if __name__ == "__main__":
    main()
