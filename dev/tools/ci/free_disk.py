#!/usr/bin/env python3
"""Reclaim only the space needed by a job on an ephemeral GitHub Linux runner."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess


GIB = 1024 ** 3
UNUSED_TOOLCHAINS = (
    Path('/usr/local/lib/android'),
    Path('/usr/share/dotnet'),
    Path('/opt/ghc'),
    Path('/usr/local/.ghcup'),
)


def reclaim(minimum_gib, workspace):
    required = minimum_gib * GIB
    for path in UNUSED_TOOLCHAINS:
        available = shutil.disk_usage(workspace).free
        if available >= required:
            break
        if path.exists():
            print(f'{available / GIB:.1f} GiB free; removing unused {path}', flush=True)
            subprocess.run(['sudo', 'rm', '-rf', '--', str(path)], check=True)
    available = shutil.disk_usage(workspace).free
    print(f'{available / GIB:.1f} GiB free (target: {minimum_gib} GiB)', flush=True)
    if available < required:
        raise RuntimeError('Insufficient runner disk space after removing unused toolchains')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--minimum-gib', type=int, default=30)
    args = parser.parse_args()
    if args.minimum_gib <= 0:
        parser.error('--minimum-gib must be positive')
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted'
            or os.environ.get('RUNNER_OS') != 'Linux'):
        parser.error('Cleanup is restricted to GitHub-hosted Linux runners')
    reclaim(args.minimum_gib, Path(os.environ['GITHUB_WORKSPACE']))


if __name__ == '__main__':
    main()
