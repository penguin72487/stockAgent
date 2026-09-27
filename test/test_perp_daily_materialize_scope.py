"""Exercise actual shell provider functions without starting any downloader."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

import pytest


@pytest.mark.parametrize("provider", ["okx", "bybit", "binance"])
@pytest.mark.parametrize("tail_only", ["0", "1"])
@pytest.mark.parametrize("enabled,source_rc", [("1", 0), ("0", 0), ("1", 7)])
def test_projection_scope_matches_successful_source_scope(
    tmp_path, provider, tail_only, enabled, source_rc
):
    repo = Path(__file__).resolve().parents[1]
    wrapper = Path(os.environ.get(
        "STOCKAGENT_DAILY_WRAPPER_UNDER_TEST",
        str(repo / "downloader/run_daily_all_markets.sh"),
    ))
    script = "\n".join([
        f"source {shlex.quote(str(wrapper))}",
        "RUN_CEX_PERP=1 RUN_BINANCE_PERP=1",
        f"CRYPTO_TAIL_ONLY={tail_only} RUN_CRYPTO_DAILY_MATERIALIZE={enabled}",
        "CRYPTO_COLUMNAR_THREADS=2 CRYPTO_DAILY_MATERIALIZE_WORKERS=3",
        f"BYBIT_SOURCE_LOCK_FILE={shlex.quote(str(tmp_path / 'source.lock'))}",
        "flock() { :; }",
        "run_step() {",
        "  \"$PYTHON_BIN\" -c 'import json,sys; print(\"ARGS:\"+json.dumps(sys.argv[1:]))' \"$@\"",
        f"  if [[ $1 == *_1m_update ]]; then return {source_rc}; fi",
        "}",
        f"if run_{provider}_perp_incremental; then exit 0; else exit $?; fi",
    ])
    completed = subprocess.run(
        ["bash", "-c", script], cwd=repo,
        env={**os.environ, "PYTHON_BIN": sys.executable},
        capture_output=True, text=True, timeout=15,
    )
    assert completed.returncode == source_rc, completed.stderr
    commands = [json.loads(line[5:]) for line in completed.stdout.splitlines()
                if line.startswith("ARGS:")]
    assert commands[0][0] == f"{provider}_perp_1m_update"
    assert ("--tail-only" in commands[0]) == (tail_only == "1")
    if source_rc or enabled == "0":
        assert len(commands) == 1
        return
    assert len(commands) == 2
    command = commands[1]
    assert command[0] == f"{provider}_perp_daily_materialize"
    assert ("--refresh" in command) == (tail_only == "0")
    assert command.count("downloader/materialize_ohlcv_daily.py") == 1
    assert command[command.index("--input-dir") + 1] == f"data_{provider}/1m"
    assert command[command.index("--output-dir") + 1] == f"data_{provider}/daily"
    assert command[command.index("--workers") + 1] == "3"
    for setting in ["POLARS_MAX_THREADS=2", "OMP_NUM_THREADS=2", "OMP_THREAD_LIMIT=2"]:
        assert setting in command
