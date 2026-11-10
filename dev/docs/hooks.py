"""Materialize published documentation downloads outside the source tree."""
from pathlib import Path
import sys

from mkdocs.structure.files import File

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'dev'))
sys.path.insert(0, str(ROOT / 'dev/flink-suite'))
from benchmark_artifacts import fetch
from render_sql_inventory import load_inventories, render_exports


def on_files(files, config):
    output = ROOT / 'target/docs-generated/sql-inventory'
    snapshots = [ROOT / f'docs/sql-inventory/flink-{line}.json.gz' for line in ('1.18', '2.2')]
    inventories = load_inventories(snapshots)
    render_exports(inventories, output)
    names = ['report.html'] + [f'flink-{line}{suffix}' for line in sorted(inventories)
                              for suffix in ('.csv', '-queries.csv')]
    for name in names:
        path = output / name
        files.append(File.generated(config, 'sql-inventory/' + path.name, abs_src_path=path))
    evidence = ROOT / 'target/docs-generated/evidence'
    for path in fetch(['docs/benchmarks'], evidence, ROOT / 'target/benchmark-artifacts'):
        src_uri = path.relative_to(evidence / 'docs').as_posix()
        existing = files.get_file_from_path(src_uri)
        if existing is not None:
            files.remove(existing)
        files.append(File.generated(config, src_uri, abs_src_path=path))
    return files
