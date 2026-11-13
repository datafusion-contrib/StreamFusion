"""Stage smaller test DSOs while retaining the original build and runtime ELF sections."""

import argparse
import json
from pathlib import Path
import shutil
import struct
import subprocess


def sections(path):
    with path.open("rb") as source:
        header = source.read(64)
        if len(header) != 64 or header[:6] != b"\x7fELF\x02\x01":
            raise ValueError(f"Expected a little-endian ELF64 library: {path}")
        offset = struct.unpack_from("<Q", header, 40)[0]
        width, count, strings = struct.unpack_from("<HHH", header, 58)
        if width != 64 or not 0 < strings < count:
            raise ValueError(f"Unsupported ELF section table: {path}")
        source.seek(offset)
        entries = [struct.unpack("<IIQQQQIIQQ", source.read(width)) for _ in range(count)]
        source.seek(entries[strings][4])
        names = source.read(entries[strings][5])
        return header[:24], {names[entry[0]:].split(b"\0", 1)[0]: entry for entry in entries}


def verify_runtime(original, stripped):
    header, before = sections(original)
    current, after = sections(stripped)
    if header != current:
        raise ValueError(f"Stripping changed the ELF identity: {original.name}")
    with original.open("rb") as source, stripped.open("rb") as target:
        for name, entry in before.items():
            if not entry[2] & 2:  # SHF_ALLOC: data mapped into the running process.
                continue
            other = after.get(name)
            if other is None or (entry[1:4], entry[5]) != (other[1:4], other[5]):
                raise ValueError(f"Stripping changed runtime section {name!r}: {original.name}")
            if entry[1] == 8:  # SHT_NOBITS reserves memory without file contents.
                continue
            source.seek(entry[4])
            target.seek(other[4])
            remaining = entry[5]
            while remaining:
                size = min(1024 * 1024, remaining)
                if source.read(size) != target.read(size):
                    raise ValueError(f"Stripping changed runtime bytes {name!r}: {original.name}")
                remaining -= size


def stage(source, output, strip="strip"):
    libraries = sorted(source.glob("libstreamfusion*.so"))
    if not any(path.name == "libstreamfusion.so" for path in libraries):
        raise ValueError("Missing the current core native library")
    output.mkdir(parents=True, exist_ok=False)
    try:
        manifest = {}
        for library in libraries:
            target = output / library.name
            subprocess.run([strip, "--strip-debug", "-o", str(target), str(library)], check=True)
            verify_runtime(library, target)
            manifest[library.name] = {"original_bytes": library.stat().st_size,
                                      "test_bytes": target.stat().st_size}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return manifest
    except BaseException:
        shutil.rmtree(output)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = stage(args.input.resolve(), args.output.resolve())
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
