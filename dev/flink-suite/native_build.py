"""Build current suite libraries with a reusable dependency target directory."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess


def build(source, target):
    source, target = source.resolve(), target.resolve()
    result = subprocess.run(
        ["cargo", "build", "--workspace", "--lib", "--locked",
         "--message-format=json-render-diagnostics", "--target-dir", str(target)],
        cwd=source / "native", stdout=subprocess.PIPE, text=True, check=True)
    libraries = set()
    for line in result.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") != "compiler-artifact" or "cdylib" not in message["target"]["crate_types"]:
            continue
        for filename in message["filenames"]:
            path = Path(filename)
            if path.name.startswith("libstreamfusion") and path.suffix in (".so", ".dylib"):
                path.resolve().relative_to(target / "debug")
                if not path.is_file():
                    raise ValueError(f"Missing current native artifact: {path}")
                libraries.add(path)
    if not libraries:
        raise ValueError("Cargo produced no StreamFusion native libraries")
    output = source / "native/target/debug"
    if output != target / "debug":
        output.mkdir(parents=True, exist_ok=True)
        for library in sorted(libraries):
            shutil.copy2(library, output / library.name)
    return libraries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    build(args.source, args.target)


if __name__ == "__main__":
    main()
