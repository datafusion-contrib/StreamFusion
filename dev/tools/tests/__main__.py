"""Discover tool tests and reject empty selections or silently omitted test modules."""

import argparse
from pathlib import Path
import sys
import unittest


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def selection(directory, excludes=()):
    paths = sorted(directory.glob("test_*.py"))
    names = {path.name for path in paths}
    unknown = set(excludes) - names
    if unknown:
        raise ValueError(f"Unknown tool test exclusions: {sorted(unknown)}")
    loader = unittest.TestLoader()
    selected = []
    for path in paths:
        if path.name in excludes:
            continue
        module = loader.loadTestsFromName("dev.tools.tests." + path.stem)
        tests = list(cases(module))
        if loader.errors:
            raise ValueError("\n".join(loader.errors))
        if not tests:
            raise ValueError(f"Tool test module has no test cases: {path.name}")
        selected.extend(tests)
    if not selected:
        raise ValueError("Tool test selection is empty")
    return unittest.TestSuite(selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exclude", action="append", default=[], metavar="TEST_FILE")
    parser.add_argument("--list", action="store_true", help="list selected invocations without running them")
    args = parser.parse_args()
    try:
        suite = selection(Path(__file__).parent, args.exclude)
    except ValueError as error:
        parser.error(str(error))
    if args.list:
        for case in cases(suite):
            print(case.id())
        return 0
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
