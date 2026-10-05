#!/usr/bin/env python3
"""Independent exact DuckLake/PG recovery in owned SSD scratch."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.control.lakehouse import configuration, restore_lake_delivery
from stockagent.data_sync.immutable_replication import digest, replicate
from downloader.artifact_io import atomic_write_json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--delivery-id", required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
if args.output.exists():
    raise ValueError("retain a fresh acceptance receipt")
c = configuration()
source = Path(c['lake_root']) / 'releases' / ('lake-' + args.delivery_id)
scratch = Path(tempfile.mkdtemp(prefix='sa-lake-acceptance-'))
copied = scratch / 'independent-data'
proof = replicate(source, copied, Path(c['binaries']) / 'bin/rclone')
pid = subprocess.check_output(['systemctl','show','postgresql@18-main','-p','MainPID','--value'],text=True).strip()
pg_bin = Path('/proc/' + pid + '/exe').resolve().parent
cluster = Path(tempfile.mkdtemp(prefix='sa-lake-private-pg-'))
cluster.rmdir()
restore = restore_lake_delivery(copied, cluster, extensions=Path(c['extensions']), pg_bin=pg_bin)
result = {'state':'accepted','delivery_identity_sha256':args.delivery_id,'fixed_independent_copy':proof,
          'catalog_restore':restore,'scratch':str(scratch),'private_pg_scratch':str(cluster),'nas_recovery_verified':False,
          'postgresql_main_pid_unchanged':subprocess.check_output(['systemctl','show','postgresql@18-main','-p','MainPID','--value'],text=True).strip()==pid}
atomic_write_json(args.output,result)
print(json.dumps(result))
