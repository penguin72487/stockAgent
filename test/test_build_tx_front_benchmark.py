"""Migration must preserve reference returns and reject unverified prices."""
from datetime import date
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.build_tx_front_benchmark import build_reference
from stockagent.data.tw_futures_benchmark import load_tx_front_rolling_benchmark


def source_bundle(tmp_path):
    raw_root = tmp_path / "moved/raw"
    raw_root.mkdir(parents=True)
    original = raw_root / "original.csv"
    original.write_bytes(b"retained official archive fixture")
    digest = sha256_file(original)
    original.rename(raw_root / f"{digest}.csv")
    rows = [(21, "202601", 100.), (21, "202602", 110.),
            (22, "202602", 115.), (23, "202602", 120.)]
    source = tmp_path / "observations.parquet"
    pl.DataFrame([{"date": date(2026, 1, day), "product": "TX", "contract": contract,
                   "series_type": "monthly", "session": "一般", "close": close,
                   "volume": 100, "source_sha256": digest} for day, contract, close in rows]).write_parquet(source)
    source.with_name("manifest.json").write_text(json.dumps({
        "outputs": {source.name: {"sha256": sha256_file(source)}},
        "sources": [{"path": "/missing/old/location/official.csv", "sha256": digest}],
    }))
    return {"source": source, "raw_root": raw_root, "output": tmp_path / "reference",
            "start": date(2026, 1, 21), "end": date(2026, 1, 23), "audit_paths": []}


def test_moved_sources_roll_using_the_new_contract_own_prior_close(tmp_path):
    args = source_bundle(tmp_path)
    result = build_reference(**args)
    dates = np.array(["2026-01-21", "2026-01-22", "2026-01-23"], dtype="datetime64[D]")
    actual = load_tx_front_rolling_benchmark(args["output"] / "continuous_daily.parquet", dates)
    np.testing.assert_allclose(actual["benchmark_log_returns"], [0., np.log(115 / 110), np.log(120 / 115)])
    np.testing.assert_array_equal(actual["front_month_roll_mask"], [False, True, False])
    assert result["date_end"] == "2026-01-23"
    assert result["action_universe_changed"] is False
    assert result["raw_sources"][0]["original_path"].startswith("/missing/")


@pytest.mark.parametrize("corrupt", ["normalized", "raw"])
def test_migration_rejects_changed_prices_or_raw_provenance(tmp_path, corrupt):
    args = source_bundle(tmp_path)
    path = args["source"] if corrupt == "normalized" else next(args["raw_root"].glob("*.csv"))
    with path.open("ab") as handle:
        handle.write(b"altered")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        build_reference(**args)
    assert not args["output"].exists()


def test_missing_front_close_does_not_promote_far_month_or_fill_a_price(tmp_path):
    args = source_bundle(tmp_path)
    frame = pl.read_parquet(args["source"]).with_columns(
        pl.when(pl.col("contract") == "202601").then(None).otherwise(pl.col("close")).alias("close"))
    frame.write_parquet(args["source"])
    path = args["source"].with_name("manifest.json")
    manifest = json.loads(path.read_text())
    manifest["outputs"][args["source"].name]["sha256"] = sha256_file(args["source"])
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="exactly one front"):
        build_reference(**args)
    assert not (args["output"] / "manifest.json").exists()


def test_retained_audit_is_a_comparison_and_never_a_price_source(tmp_path):
    args = source_bundle(tmp_path)
    build_reference(**args)
    dates = np.array(["2026-01-21", "2026-01-22", "2026-01-23"], dtype="datetime64[D]")
    actual = load_tx_front_rolling_benchmark(args["output"] / "continuous_daily.parquet", dates)
    actual["benchmark_log_returns"][1] += .1
    audit = tmp_path / "old_audit.npz"
    np.savez(audit, **actual)
    args.update(output=tmp_path / "different_reference", audit_paths=[audit])
    with pytest.raises(ValueError, match="differs from retained audit"):
        build_reference(**args)
    assert not (args["output"] / "manifest.json").exists()
