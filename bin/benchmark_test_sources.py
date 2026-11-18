"""Copy current benchmark fixtures into the test layout of a historical checkout."""

import hashlib
from pathlib import Path
import shutil


def test_root(checkout):
    roots = [checkout / name for name in ("streamfusion-runtime/src/test", "src/test")
             if (checkout / name / "java").is_dir()]
    if len(roots) != 1:
        raise ValueError(f"Expected one shared Java test source tree in {checkout}: {roots}")
    return roots[0]


def copy_test_sources(source, checkout, fixtures):
    source_root, target_root = test_root(source), test_root(checkout)
    hashes = {}
    for fixture in fixtures:
        relative = Path(fixture)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Invalid test fixture path: {fixture}")
        original, target = source_root / relative, target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
        hashes[target.relative_to(checkout).as_posix()] = hashlib.sha256(target.read_bytes()).hexdigest()
    return hashes
