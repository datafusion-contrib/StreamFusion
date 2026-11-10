"""Materialize published documentation downloads outside the source tree."""
from pathlib import Path
import sys

from mkdocs.structure.files import File

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'dev/flink-suite'))
from render_sql_inventory import load_inventories, render_exports


def on_files(files, config):
    output = ROOT / 'target/docs-generated/sql-inventory'
    snapshots = sorted((ROOT / 'docs/sql-inventory').glob('flink-*.json.gz'))
    render_exports(load_inventories(snapshots), output)
    for path in sorted(output.iterdir()):
        files.append(File.generated(config, 'sql-inventory/' + path.name, abs_src_path=path))
    return files
