"""Measure child processes without changing each experiment's validation or report schema."""

import resource
import subprocess
import time


def alternating(modes, pair):
    return tuple(modes) if pair % 2 == 0 else tuple(reversed(modes))


def run_logged(command, *, root, log, environment):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    with log.open("w") as stream:
        result = subprocess.run(command, cwd=root, stdout=stream,
                                stderr=subprocess.STDOUT, env=environment)
    elapsed = time.monotonic() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    return result, {
        "elapsed_seconds": elapsed,
        "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
    }
