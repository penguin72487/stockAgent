#!/usr/bin/env python3
"""Enroll an existing frozen formal run without changing or starting training.

Automatic recipe inference is deliberately limited to the existing Taiwan
factorized value-only contract. Other products use an explicit reviewed spec
with their own canonical builders; never guess their source dependencies.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.training_rescue import Rescue, SSH, load_policy  # noqa: E402
from stockagent.training_rescue_worker import CONTRACT, absolute, atomic_json, digest  # noqa: E402
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id  # noqa: E402

DESCRIBE = r'''
import hashlib,json,pathlib,shlex,sys
import torch,yaml
node=pathlib.Path(sys.argv[1]); output=pathlib.Path(sys.argv[2]); config=pathlib.Path(sys.argv[3]); receipt=pathlib.Path(sys.argv[4])
release=json.loads(receipt.read_bytes()); source=receipt.parent/'build-source'
sys.path.insert(0,str(source))
from stockagent.runtime_identity import runtime_identity,validate_runtime_lock,verify_source_release,verify_release_bundles
manifest=json.loads((output/'run_manifest.json').read_bytes())
saved_source=manifest['runtime_provenance']['source_release']
proof=verify_source_release(receipt,source); verify_release_bundles(receipt)
assert proof['source_sha256']==saved_source['source_sha256'] and proof['receipt_sha256']==saved_source['receipt_sha256']
raw=yaml.safe_load(config.read_bytes()); assert not any(raw.get(k) for k in ('base_config','base_configs','extends','inherits')), 'provide complete standalone generated config'
data=manifest['configuration']['data']; prepared=pathlib.Path(data['factorized_feature_manifest'])
feature=json.loads(prepared.read_bytes()); assert feature.get('model_channel_policy')=='value_only'
parent=node/'artifacts/data_preparation'/prepared.parent.parent.name.replace('factorized_values','factorized_panel')/'prepared/factorized_manifest.json'
assert hashlib.sha256(parent.read_bytes()).hexdigest()==feature['parent_feature_manifest_sha256']
parent_meta=json.loads(parent.read_bytes())
prep_receipt=node/'artifacts/code_releases'/parent.parent.parent.name/'code-release/release.json'
prep_release=json.loads(prep_receipt.read_bytes()); prep_source=prep_receipt.parent/'build-source'
assert prep_release['code']['files']['scripts/prepare_tw_day_trade_factorized_panel.py']==parent_meta['builder_sha256'], 'original preparation code not preserved'
verify_release_bundles(prep_receipt)
if (prep_source/'train.py').is_file():verify_source_release(prep_receipt,prep_source)
else:
    import zipfile
    with zipfile.ZipFile(prep_receipt.parent/prep_release['source_bundle']['file']) as z:
        assert set(z.namelist())==set(prep_release['source_bundle']['files'])
        assert all(hashlib.sha256(z.read(n)).hexdigest()==h for n,h in prep_release['source_bundle']['files'].items())
assert release['code']['files']['stockagent/data/factorized_panel_projection.py']==feature['builder_sha256']
checkpoints=[torch.load(p,map_location='cpu',weights_only=True) for p in output.glob('train_*/checkpoint_last.pt')]
assert checkpoints, 'no committed optimizer checkpoint'
worlds={p['rng_state'].get('world_size',1) for p in checkpoints}; batches={p['effective_train_batch_size'] for p in checkpoints}
assert len(worlds)==len(batches)==1
argv=None
for log in sorted(output.glob('train_*.log'),reverse=True):
    with log.open(errors='replace') as stream:
        for i,line in enumerate(stream):
            if i>100:break
            if '[ddp] relaunching' in line and ' cmd=' in line:
                args=shlex.split(line.split(' cmd=',1)[1]); train=args[args.index('train.py'):]
                if train[train.index('--config')+1]==str(config) and train[train.index('--output-dir')+1]==str(output):argv=train;break
    if argv:break
assert argv is not None, 'cannot infer canonical launcher; supply explicit generic spec'
argv=[a for a in argv if a not in ('--no-resume','--resume')]+['--resume']
def row(path,role,target=None):
    return {'source_path':str(path),'target_path':str(target or path),'role':role,'bytes':path.stat().st_size,'sha256':hashlib.file_digest(path.open('rb'),'sha256').hexdigest()}
files=[row(receipt,'source_receipt'),row(receipt.parent/release['source_bundle']['file'],'source_zip'),row(receipt.parent/release['wheel']['file'],'source_wheel'),row(config,'training_config')]
files += [row(prep_receipt,'preparation_receipt'),row(prep_receipt.parent/prep_release['source_bundle']['file'],'preparation_zip'),row(prep_receipt.parent/prep_release['wheel']['file'],'preparation_wheel')]
# The preparation release records the original selected base. A later moved
# training.yaml may reference a retired code-training tree; never infer its
# historical configuration from today's mutable working checkout.
base_name=prep_release['selected_config']
assert base_name in prep_release['source_bundle']['files'], 'original selected preparation config not preserved'
base_config=prep_source/base_name
runtime=runtime_identity(); differences=validate_runtime_lock(manifest['runtime_provenance']['runtime'],runtime)
critical=('torch','numpy','pyarrow','polars','triton','flash-attn','xformers')
old=manifest['runtime_provenance']['runtime']
assert old['python']==runtime['python'] and all(old['packages'].get(k)==runtime['packages'].get(k) for k in critical), 'numerical runtime changed; reconstruct the selected runtime first'
print(json.dumps({'source_root':str(source),'source_receipt':str(receipt),'output_root':str(output),'config_file':str(config),
    'runtime_prefix':sys.prefix,'runtime_lock':runtime,'historical_runtime_differences':differences,'train_argv':argv,'bootstrap_files':files,
    'world_size':next(iter(worlds)),'global_batch':next(iter(batches)),
    'checkpoint_manifests':{p.relative_to(output).as_posix():__import__('stockagent.runtime_identity',fromlist=['identity_sha256']).identity_sha256(torch.load(p,map_location='cpu',weights_only=True)['experiment_manifest']) for p in output.glob('train_*/checkpoint_last.pt')},
    'data':{k:data[k] for k in ['parquet_root','day_trade_minute_execution_root','day_trade_physical_public_feature_path','panel_cache_root','day_trade_minute_execution_cache_dir']},
    'parent_prepared':str(parent),'value_prepared':str(prepared),'source_release':proof,
    'preparation_source_root':str(prep_source),'preparation_receipt':str(prep_receipt),'preparation_base_config':str(base_config),
    'source_manifest':str(pathlib.Path(data['parquet_root']).parent/'source_manifest.json'),
    'prepared_references':[row(parent,'parent_prepared_reference'),row(prepared,'value_prepared_reference')]}))
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--policy", type=Path, default=ROOT / "configs/training_rescue.json")
    p.add_argument("--run", required=True)
    p.add_argument("--ssh-config", type=Path, default=Path("/etc/stockagent/agent-ssh.conf"))
    p.add_argument("--ssh-alias", default="stockagent-vast")
    p.add_argument("--node-root", default="/root/stockAgent")
    p.add_argument("--output-root", required=True)
    p.add_argument("--config-file", required=True)
    p.add_argument("--source-receipt", required=True)
    p.add_argument("--runtime-capsule", required=True, help="Closed fixed-prefix archive path on source")
    args = p.parse_args()
    rescue = Rescue(load_policy(args.policy))
    rescue.guard()
    ssh = SSH.profile(args.ssh_config, args.ssh_alias)
    for value in (args.node_root, args.output_root, args.config_file, args.source_receipt, args.runtime_capsule):
        absolute(value)
    # The selected interpreter is discovered via the original runtime entrypoint.
    prefix = ssh.run(["bash", "-c", 'cd "$1"; source scripts/runtime_env.sh; run_fintech_python -c "import sys; print(sys.prefix)"',
                      "rescue-runtime-discovery", args.node_root]).decode().strip()
    absolute(prefix)
    observed = ssh.json([prefix + "/bin/python", "-c", DESCRIBE, args.node_root,
                        args.output_root, args.config_file, args.source_receipt], timeout=180)
    spec = {k: v for k, v in observed.items() if k not in {"data", "parent_prepared", "value_prepared", "prepared_references", "source_manifest"}}
    spec.update(contract=CONTRACT, schema_version=1, run_id=args.run,
                source_ssh={"config": str(args.ssh_config.absolute()), "alias": args.ssh_alias},
                runtime_capsule_path=args.runtime_capsule, source_supervised=False,
                environment={"STOCKAGENT_CODE_RELEASE_RECEIPT": args.source_receipt,
                    "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                    "POLARS_MAX_THREADS": "1", "RAYON_NUM_THREADS": "1", "PYTHONUNBUFFERED": "1"})
    # The prior formal launch explicitly recorded these thread limits. New specs
    # must capture their own executor settings rather than copy these defaults.
    data = observed["data"]
    roots = [data["parquet_root"], data["day_trade_minute_execution_root"], data["day_trade_physical_public_feature_path"]]
    bindings = {}
    base = Path("/srv/stockagent-packed-materialized")
    for value in roots:
        parts = Path(value).relative_to(base).parts
        if len(parts) < 2:
            raise ValueError("training source is not a pinned materialization")
        dataset, snapshot = parts[:2]
        resolved = resolve_packed_snapshot_id(Path("/srv/stockagent-packed"), dataset, snapshot)
        bindings[(dataset, snapshot)] = {"dataset": dataset, "snapshot_id": snapshot,
            "manifest_sha256": resolved.manifest_sha256, "materialized_root": str(base)}
    spec["data_sources"] = list(bindings.values())
    source = str(Path(data["parquet_root"]).parent)
    parent, value = Path(observed["parent_prepared"]), Path(observed["value_prepared"])
    source_id = Path(source).name
    spec["rebuild_steps"] = [
        ["scripts/prepare_tw_day_trade_factorized_panel.py", "--source-root", source,
         "--snapshot-id", source_id, "--minute-root", data["day_trade_minute_execution_root"],
         "--base-config", observed["preparation_base_config"], "--output-root", str(parent.parent),
         "--base-panel-cache-root", data["panel_cache_root"]],
        ["scripts/prepare_tw_day_trade_factorized_panel.py", "--project-prepared-manifest", str(parent),
         "--model-channel-policy", "value_only", "--output-root", str(value.parent)],
    ]
    spec["rebuild_source_roots"] = [observed["preparation_source_root"], observed["source_root"]]
    spec["source_releases"] = [
        {"zip_role": "source_zip", "receipt": args.source_receipt, "root": observed["source_root"]},
        {"zip_role": "preparation_zip", "receipt": observed["preparation_receipt"], "root": observed["preparation_source_root"]},
    ]
    references = []
    for index, row in enumerate(observed["prepared_references"]):
        row["target_path"] = f"/var/lib/stockagent-training-rescue/{args.run}/references/{index}-factorized_manifest.json"
        references.append({"step_index": index, "reference_path": row["target_path"],
                           "rebuilt_manifest": str(parent if index == 0 else value)})
        spec["bootstrap_files"].append(row)
    spec["prepared_reconciliation"] = references
    with rescue.lock():
        result = rescue.register(spec)
        atomic_json(rescue.path(args.run) / "enrollment-receipt.json", {
            "contract": CONTRACT, "spec_sha256": digest(spec), "source_release": spec["source_release"],
            "source_supervised": False, "historical_runtime_differences": spec["historical_runtime_differences"],
            "source_inputs_copied": False, "prepared_views_stored_on_penguin": False, "training_started": False})
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
