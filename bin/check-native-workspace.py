#!/usr/bin/env python3
"""Check package dependencies and, optionally, the built JNI export boundaries."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--libraries", type=Path, help="Cargo output directory containing the DSOs")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--format-version", "1",
        "--manifest-path", str(root / "native/Cargo.toml"),
    ], text=True))
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    owners = {
        "streamfusion": "Java_tech_streamfusion_Native_",
        "streamfusion-kafka": "Java_tech_streamfusion_kafka_NativeKafka_",
        "streamfusion-parquet": "Java_tech_streamfusion_parquet_NativeParquet_",
    }
    for name in ("json", "csv", "raw", "avro", "protobuf"):
        owners[f"streamfusion-{name}"] = (
            f"Java_tech_streamfusion_format_{name}_Native{name.title()}Format_"
        )

    for package_id in metadata["workspace_members"]:
        package = packages[package_id]
        name = package["name"]
        cdylibs = [target for target in package["targets"] if "cdylib" in target["crate_types"]]
        assert bool(cdylibs) == (name in owners), f"unexpected library target for {name}"
        if name in owners and name != "streamfusion":
            pending = [package_id]
            seen = set()
            while pending:
                dependency = pending.pop()
                if dependency in seen:
                    continue
                seen.add(dependency)
                dep_name = packages[dependency]["name"]
                assert dep_name != "streamfusion" and not dep_name.startswith("datafusion"), (
                    f"{name} reaches engine dependency {dep_name}"
                )
                if dependency != package_id and dep_name in owners:
                    raise AssertionError(f"{name} links another deployable library: {dep_name}")
                pending.extend(
                    dep["pkg"] for dep in nodes[dependency]["deps"]
                    if any(kind["kind"] is None for kind in dep["dep_kinds"])
                )

        if not cdylibs or args.libraries is None:
            continue
        suffix = "dylib" if sys.platform == "darwin" else "so"
        library = args.libraries / f"lib{cdylibs[0]['name']}.{suffix}"
        command = ["nm", "-gU"] if sys.platform == "darwin" else ["nm", "-D", "--defined-only"]
        symbols = subprocess.check_output(command + [str(library)], text=True)
        exports = set(re.findall(r"(?:^|\s)_?(Java_\w+)", symbols, re.MULTILINE))
        assert exports, f"{library} exports no JNI entry points"
        foreign = sorted(symbol for symbol in exports if not symbol.startswith(owners[name]))
        assert not foreign, f"{library} exports another class's entry points: {foreign}"
        if name != "streamfusion":
            assert "JNI_OnLoad" not in symbols, f"{library} exports the engine's load hook"
    print("Native package and JNI export boundaries verified.")


if __name__ == "__main__":
    main()
