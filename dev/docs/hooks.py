"""Materialize published documentation downloads outside the source tree."""
from pathlib import Path
import sys

from mkdocs.structure.files import File

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'dev'))
sys.path.insert(0, str(ROOT / 'dev/flink-suite'))
from benchmark_artifacts import fetch
from benchmark_reports import SCHEMA_KEY, original_report
from render_sql_inventory import load_inventories, render_exports
import json


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
    # Keep published JSON downloads byte-compatible with their original string-valued plans.
    reports = ROOT / 'target/docs-generated/reports'
    for path in (ROOT / 'docs/benchmarks/results').glob('*.json'):
        if SCHEMA_KEY not in json.loads(path.read_text()):
            continue
        target = reports / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(original_report(path, evidence, ROOT / 'target/benchmark-artifacts'))
        src_uri = path.relative_to(ROOT / 'docs').as_posix()
        existing = files.get_file_from_path(src_uri)
        if existing is not None:
            files.remove(existing)
        files.append(File.generated(config, src_uri, abs_src_path=target))
    return files
