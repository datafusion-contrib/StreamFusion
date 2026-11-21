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


def fixture_source(checkout, fixture):
    relative = Path(fixture)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Invalid test fixture path: {fixture}")
    roots = [test_root(checkout), checkout / "streamfusion-test-utils/src/main",
             checkout / "streamfusion-runtime/src/benchmark"]
    matches = [root / relative for root in roots if (root / relative).is_file()]
    if not matches:
        raise FileNotFoundError(f"Missing test fixture: {fixture} in {checkout}")
    if len(matches) != 1:
        raise ValueError(f"Ambiguous test fixture: {fixture}: {matches}")
    return matches[0]


def copy_test_sources(source, checkout, fixtures):
    target_root = test_root(checkout)
    hashes = {}
    for fixture in fixtures:
        relative = Path(fixture)
        original = fixture_source(source, relative)
        target = target_root / relative
        if (checkout / "streamfusion-test-utils/pom.xml").is_file():
            try:
                target = fixture_source(checkout, relative)
            except FileNotFoundError:
                # Preserve extracted owners when a new fixture is absent in the target revision.
                for owner in ("streamfusion-test-utils/src/main", "streamfusion-runtime/src/benchmark"):
                    if original.is_relative_to(source / owner):
                        target = checkout / owner / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
        hashes[target.relative_to(checkout).as_posix()] = hashlib.sha256(target.read_bytes()).hexdigest()
    return hashes
