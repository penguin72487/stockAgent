"""Exercise the real shell entrypoint with cache/training side effects recorded."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml


@pytest.mark.parametrize("case", ["normal", "data_only", "cache_failure", "cache_wrong_target", "dual_alias"])
def test_launcher_uses_canonical_config_cache_and_training(tmp_path, case):
    repo = Path(__file__).resolve().parents[1]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("run_tw_stock_futures_day_trade_0845_minute.sh",
                 "run_tw_stock_futures_day_trade_multi_basis_projection_l1_dual_5090.sh"):
        shutil.copyfile(repo / "scripts" / name, scripts / name)
    public = tmp_path / "materialized" / "tw-public" / "tw-public-fixture"
    futures = tmp_path / "materialized" / "tw-futures" / "tw-futures-fixture"
    public.mkdir(parents=True)
    futures.mkdir(parents=True)
    config = tmp_path / "selected config.yaml"
    config.write_text(yaml.safe_dump({
        "base_config": str(repo / "configs/markets/tw_stock_futures_day_trade_0845_minute.yaml"),
        "data": {"parquet_root": str(public / "stocks"),
                 "tw_public_feature_path": str(public / "features/daily.parquet")},
        "trading": {"tw_stock_futures_day_trade_data_path": str(futures / "daily/data.parquet"),
                    "tw_stock_futures_day_trade_minute_data_path": str(futures / "minutes/minutes.parquet")},
    }))
    recorder = tmp_path / "record.py"
    recorder.write_text('''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if args[0] == "cache":
    from scripts.data_cache import build_parser
    build_parser().parse_args(args[1:])
refs = []
for fd in Path(f"/proc/{os.getppid()}/fd").iterdir():
    try:
        refs.append(os.readlink(fd))
    except OSError:
        pass
with open(os.environ["LAUNCH_TEST_LOG"], "a") as handle:
    handle.write(json.dumps({"args": args, "refs": refs}) + "\\n")
if args[:3] == ["cache", "use", os.environ.get("LAUNCH_TEST_FAIL_DATASET")]:
    raise SystemExit(2)
if args[0] == "cache":
    target = json.loads(os.environ["LAUNCH_TEST_TARGETS"])[args[2]]
    if os.environ.get("LAUNCH_TEST_WRONG_TARGET"):
        target = "/different/materialization"
    print(json.dumps({"lease": {"target": target, "snapshot_id": args[-1]}}))
''')
    (scripts / "runtime_env.sh").write_text('''run_fintech_python() {
  if [[ "$1" == "-" ]]; then
    "$LAUNCH_TEST_PYTHON" "$@"
  else
    "$LAUNCH_TEST_PYTHON" "$LAUNCH_TEST_RECORDER" "$@"
  fi
}
''')
    (scripts / "run_data_cache.sh").write_text(
        '"$LAUNCH_TEST_PYTHON" "$LAUNCH_TEST_RECORDER" cache "$@"\n'
    )
    log = tmp_path / "calls.jsonl"
    env = dict(os.environ, PYTHONPATH=str(repo), LAUNCH_TEST_PYTHON=sys.executable,
               LAUNCH_TEST_RECORDER=str(recorder), LAUNCH_TEST_LOG=str(log),
               LAUNCH_TEST_TARGETS=json.dumps({"tw-public": str(public), "tw-futures": str(futures)}))
    if case == "cache_failure":
        env["LAUNCH_TEST_FAIL_DATASET"] = "tw-futures"
    if case == "cache_wrong_target":
        env["LAUNCH_TEST_WRONG_TARGET"] = "1"
    launcher = ("run_tw_stock_futures_day_trade_multi_basis_projection_l1_dual_5090.sh"
                if case == "dual_alias" else "run_tw_stock_futures_day_trade_0845_minute.sh")
    forwarded = ["--config", str(config), "--max-folds", "1",
                 "--output-dir", str(tmp_path / "output with spaces")]
    if case == "data_only":
        forwarded.append("--check-data-only")
    result = subprocess.run(["bash", str(scripts / launcher), *forwarded], env=env,
                            cwd=tmp_path.parent, capture_output=True, text=True, timeout=30)
    assert result.returncode == ({"cache_failure": 2, "cache_wrong_target": 1}.get(case, 0)), result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    if case == "cache_wrong_target":
        assert len(calls) == 2
        assert "cache use result differs" in result.stderr
        return
    if case == "data_only":
        assert len(calls) == 1 and calls[0]["args"][0] == "train.py"
    else:
        assert calls[0]["args"][0] == "scripts/check_environment.py"
        assert calls[1]["args"] == ["cache", "use", "tw-public", "--snapshot-id", public.name]
        assert calls[2]["args"] == ["cache", "use", "tw-futures", "--snapshot-id", futures.name]
        if case == "cache_failure":
            assert len(calls) == 3
            return
        assert len(calls) == 4
        assert {str(public), str(futures)} <= set(calls[-1]["refs"])
    assert calls[-1]["args"][-len(forwarded):] == forwarded
    assert calls[-1]["args"][:3] == [
        "train.py", "--config", "configs/markets/tw_stock_futures_day_trade_0845_minute.yaml"
    ]
