"""Group short connector suites without changing the selected upstream test corpus."""

import json
import os


def matrix(inventory=False, scope="all"):
    if scope not in ("all", "runtime", "connectors"):
        raise ValueError(f"Unknown inventory scope: {scope}")
    entries = []
    for line, version in (("2.2", "2.2.1"), ("1.18", "1.18.1")):
        groups = []
        if not inventory or scope in ("all", "runtime"):
            groups.extend((f"runtime-{shard}", "runtime", str(shard)) for shard in range(1, 5))
        if not inventory or scope in ("all", "connectors"):
            connectors = "formats parquet orc kafka" + (" delta" if line == "2.2" else "")
            groups.extend((("connectors", connectors, ""), ("paimon", "paimon", "")))
        if not inventory or scope == "all":
            groups.append(("state", "state", ""))
        for label, suites, shard in groups:
            entries.append(dict(line=line, version=version, label=label, suites=suites, shard=shard))
    return {"include": entries}


if __name__ == "__main__":
    print("suites=" + json.dumps(matrix(os.environ.get("INVENTORY") == "true", os.environ.get("SCOPE", "all"))))
