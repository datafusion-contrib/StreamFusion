"""Read historical benchmark reports with versioned, hash-pinned plan references."""

import argparse
import json
from pathlib import Path

from benchmark_artifacts import MANIFEST, ROOT, fetch, load_manifest, verify

SCHEMA_KEY = "_evidence_schema"


def pointer_value(document, pointer):
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise ValueError(f"Invalid evidence JSON pointer: {pointer!r}")
    for part in pointer[1:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        document = document[int(part)] if isinstance(document, list) else document[part]
    return document


def original_report(path, destination, cache, manifest_path=MANIFEST):
    """Return verified original bytes, checking the maintained observations against them."""
    document = json.loads(path.read_text())
    schema = document.pop(SCHEMA_KEY, None)
    if schema is None:
        return path.read_bytes()
    if (not isinstance(schema, dict) or set(schema) != {"version", "original"}
            or schema["version"] != 2):
        raise ValueError("Unsupported benchmark report evidence schema")
    manifest = load_manifest(manifest_path)
    name = schema["original"]
    if not isinstance(name, str) or name not in manifest["files"]:
        raise ValueError(f"Unknown original benchmark report: {name!r}")
    record = manifest["files"][name]
    [archived] = fetch([name], destination, cache, manifest_path)
    if not verify(archived, record):
        raise ValueError(f"Unverified benchmark report: {name}")
    payload = archived.read_bytes()
    original = json.loads(payload)
    used = set()

    def restore(value, pointer=""):
        if isinstance(value, dict):
            if "$plan" in value:
                ref = value["$plan"]
                if (set(value) != {"$plan"} or not isinstance(ref, dict)
                        or set(ref) != {"path", "pointer", "sha256"}
                        or ref["path"] != name or ref["pointer"] != pointer
                        or ref["sha256"] != record["sha256"]):
                    raise ValueError(f"Invalid plan evidence reference: {pointer}")
                text = pointer_value(original, pointer)
                if not isinstance(text, str):
                    raise ValueError(f"Plan evidence is not text: {pointer}")
                used.add(pointer)
                return text
            return {key: restore(item, pointer + "/" + key.replace("~", "~0").replace("/", "~1"))
                    for key, item in value.items()}
        if isinstance(value, list):
            return [restore(item, pointer + "/" + str(index)) for index, item in enumerate(value)]
        return value

    restored = restore(document)
    if used != set(record["source"]["json_pointers"]):
        raise ValueError("Incomplete benchmark plan evidence references")
    if restored != original:
        raise ValueError("Maintained benchmark observations differ from the original report")
    return payload


def load_report(path, destination, cache, manifest_path=MANIFEST):
    return json.loads(original_report(path, destination, cache, manifest_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, help="Write the byte-identical original JSON report")
    parser.add_argument("--destination", type=Path, default=ROOT / "target/report-evidence")
    parser.add_argument("--cache", type=Path, default=ROOT / "target/benchmark-artifacts")
    args = parser.parse_args()
    payload = original_report(args.report, args.destination, args.cache)
    if args.output:
        args.output.write_bytes(payload)
    else:
        print(payload.decode("utf-8"), end="")


if __name__ == "__main__":
    main()
