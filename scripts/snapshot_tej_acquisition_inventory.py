"""Freeze public-safe TEJ metadata; no provider queries or licensed value reads."""
from __future__ import annotations

import argparse
from contextlib import closing
import csv
from datetime import UTC, datetime
import io
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.live.tej_dashboard import FEATURE_COLUMNS, _read, build_tej_public_status


def snapshot(repo: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("Refusing to overwrite acquisition inventory evidence")
    started = datetime.now(UTC).isoformat()
    status = build_tej_public_status(repo)
    if status['state'] in {'not_registered','metadata_unavailable'}:
        raise RuntimeError("No readable TEJ registry to snapshot")
    # Read all registered fields once, then close the read transaction before
    # formatting CSV. A long reporting transaction must not stall the writer.
    with closing(_read(repo/'data_tej')) as con:
        con.execute('BEGIN')
        fields = [dict(row) for row in con.execute(
            'SELECT '+','.join('f.'+key for key in FEATURE_COLUMNS)+
            ',t.name AS table_name,t.smart_id,t.category,t.state AS table_state '
            'FROM features f JOIN tables t ON t.table_id=f.table_id '
            'ORDER BY f.phase,t.category,t.name,f.field_index')]
    if len(fields) != status['catalog']['fields']:
        raise RuntimeError("Catalog changed during snapshot; do not publish mixed identities")
    output.mkdir(parents=True)
    for filename, rows in [('table_history_status.csv',status['tables']),
                           ('feature_download_status.csv',fields)]:
        keys = list(dict.fromkeys(k for row in rows for k in row))
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream,keys)
        writer.writeheader()
        writer.writerows({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v
                         for k,v in row.items()} for row in rows)
        atomic_write_text(output/filename,'\ufeff'+stream.getvalue())
    atomic_write_json(output/'public_status.json',status)
    forecast=status.get('eta',{}).get('forecast',{})
    manifest = {'contract_version':2,'provider':'tej_smart_wizard',
        'public_status_schema_version':status['schema_version'],
        'eta_contract':forecast.get('contract'),'eta_input_sha256':forecast.get('input_sha256'),
        'eta_scenarios_not_scheduled_completion':True,
        'started_at_utc':started,'completed_at_utc':datetime.now(UTC).isoformat(),
        'tables':len(status['tables']),'fields':len(fields),
        'provider_queries_sent':0,'raw_values_exposed':False,
        'consistency':'catalog_identity_verified_separate_bounded_metadata_read_transactions',
        'snapshot_not_live':True,
        'csv_empty_cell_means_unknown_not_zero':True,
        'menu_bounds_are_native_history_or_release_times':False,
        'all_history_downloaded':False,'all_source_units_verified':False,
        'source':'canonical_read_only_TEJ_projection_and_feature_registry'}
    atomic_write_json(output/'manifest.json',manifest)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args(argv)
    print(json.dumps(snapshot(args.root.resolve(),args.output),ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
