import json

import numpy as np
import pytest

from downloader.artifact_io import sha256_file
from scripts import benchmark_live_base_panel_cutoff as bench
from stockagent.data.panel import PanelData


def panel():
    values = np.array([[0.0, -0.0]], dtype=np.float32)
    return PanelData(dates=np.array(["2026-10-13"], dtype="datetime64[ns]"),
        symbols=["2330", "1101"], feature_names=["raw"], features=values[..., None],
        returns_1d=values.copy(), tradable_mask=np.ones_like(values, dtype=bool),
        alive_mask=np.ones_like(values, dtype=bool), benchmark_returns=np.zeros((1,), dtype=np.float32),
        close_prices=values.copy())


@pytest.mark.parametrize("changed", ["feature", "mask", "universe", "dtype", "clock", "optional"])
def test_complete_panel_oracle_retains_all_fields_signed_bits_and_causal_metadata(changed):
    baseline = panel()
    reference = bench.panel_identity(baseline)
    current = panel()
    assert bench.panel_identity(current) == reference
    if changed == "feature":
        current.features[0, 1, 0] = 0.0
    elif changed == "mask":
        current.tradable_mask[0, 1] = False
    elif changed == "universe":
        current.symbols.reverse()
    elif changed == "dtype":
        current.features = current.features.astype(np.float64)
    elif changed == "clock":
        current.dates = current.dates + np.timedelta64(1, "D")
    else:
        current.short_capacity_shares = np.zeros((1, 2), dtype=np.float64)
    assert bench.panel_identity(current)["sha256"] != reference["sha256"]


def test_unknown_lazy_fields_must_not_disappear_from_oracle():
    value = panel()
    value.day_trade_carry_source = object()
    with pytest.raises(TypeError):
        bench.panel_identity(value)


@pytest.mark.parametrize("changed", ["bytes", "missing", "new_stock", "new_tail", "reference"])
def test_frozen_workload_checks_original_reference_all_bytes_and_complete_membership(tmp_path, changed):
    reference = tmp_path / "baseline.py"
    reference.write_bytes(b"original exact source")
    stocks = tmp_path / "data_tw_public/stocks"
    stocks.mkdir(parents=True)
    original = stocks / "2330_features.parquet"
    original.write_bytes(b"full original source")
    manifest = {"baseline_sha256": sha256_file(reference),
                "members": [{"path": str(original), "sha256": sha256_file(original)}]}
    assert bench.fence(tmp_path, manifest) == {str(original): sha256_file(original)}
    if changed == "bytes":
        original.write_bytes(b"changed full source")
    elif changed == "missing":
        original.unlink()
    elif changed == "new_stock":
        (stocks / "1101_features.parquet").write_bytes(b"unplanned observation")
    elif changed == "new_tail":
        (stocks / "_hot_tail").mkdir()
        (stocks / "_hot_tail/2330_features.parquet").write_bytes(b"unplanned increment")
    else:
        reference.write_bytes(b"rewritten comparator")
    with pytest.raises((ValueError, OSError)):
        bench.fence(tmp_path, manifest)


def test_full_fixture_requires_matching_roots_and_official_precision_layout(monkeypatch, tmp_path):
    roots = [{"data": {"parquet_root": str(tmp_path / name)}} for name in ("first", "second")]
    with pytest.raises(ValueError, match="share one"):
        bench.source_members(roots)
    with pytest.raises(ValueError, match="official TW path"):
        bench.source_members(roots[:1])


def test_evidence_path_does_not_overwrite_or_escape_the_requested_engineering_root(monkeypatch, tmp_path):
    monkeypatch.setattr(bench, "ROOT", tmp_path)
    root = tmp_path / "artifacts/benchmarks"
    root.mkdir(parents=True)
    new = root / "new"
    assert bench.new_path(new) == new
    new.write_bytes(b"preserve acceptance")
    for path in (new, tmp_path / "outside"):
        with pytest.raises(ValueError):
            bench.new_path(path)


def original_reference(tmp_path):
    self_name = str(bench.Path(bench.__file__))
    saved = tmp_path / "original_controller.py"
    saved.write_bytes(b"independent original complete controller")
    original_codes = {self_name: sha256_file(saved), "canonical_panel.py": "unchanged"}
    manifest = {"code_sha256": original_codes, "recipes": [{"lookback": 32}]}
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "fixture.json").write_text(json.dumps(manifest))
    expected = [{"mode": "v8", "panel": bench.panel_identity(panel())}]
    control = {"state": "accepted_complete_base_panel_only", "all_panel_fields_equal": True,
               "fixture": str(fixture), "fixture_sha256": sha256_file(fixture / "fixture.json"),
               "code_sha256": original_codes, "trials": [{"result": expected} for _ in range(4)]}
    control_path = tmp_path / "accepted_original.json"
    control_path.write_text(json.dumps(control))
    capture = {"state": "captured_accepted_base_panel_controller", "control": str(control_path),
               "control_sha256": sha256_file(control_path), "saved": str(saved),
               "source": self_name, "saved_sha256": sha256_file(saved)}
    receipt = tmp_path / "reference.json"
    receipt.write_text(json.dumps(capture))
    codes = {**original_codes, self_name: "new benchmark-only controller",
             str(bench.SOURCE_CACHE): "new raw cache owner"}
    return receipt, fixture, manifest, codes, control, capture


def test_cache_probe_reuses_independent_original_complete_oracle_not_its_own_first_trial(tmp_path):
    receipt, fixture, manifest, codes, control, capture = original_reference(tmp_path)
    proof = bench.independent_reference(receipt, fixture, manifest, codes)
    assert proof["expected"] == control["trials"][0]["result"]
    assert proof["saved_sha256"] == capture["saved_sha256"]
    assert proof["control_sha256"] == capture["control_sha256"]
    assert proof["receipt_sha256"] == sha256_file(receipt)


@pytest.mark.parametrize("changed", [
    "receipt_state", "source_name", "control_bytes", "saved_bytes", "original_code_sha",
    "control_state", "parity_flag", "trial_count", "trial_fields", "fixture_identity",
    "fixture_bytes", "manifest_code", "canonical_code", "unplanned_code", "missing_owner",
])
def test_independent_oracle_rejects_source_recipe_code_or_evidence_drift(tmp_path, changed):
    receipt, fixture, manifest, codes, control, capture = original_reference(tmp_path)
    if changed == "receipt_state":
        capture["state"] = "running"
    elif changed == "source_name":
        capture["source"] = "some_other_controller.py"
    elif changed == "control_bytes":
        bench.Path(capture["control"]).write_bytes(b"{}")
    elif changed == "saved_bytes":
        bench.Path(capture["saved"]).write_bytes(b"replaced comparator")
    elif changed == "original_code_sha":
        control["code_sha256"][capture["source"]] = "unbound original source"
    elif changed == "control_state":
        control["state"] = "running"
    elif changed == "parity_flag":
        control["all_panel_fields_equal"] = False
    elif changed == "trial_count":
        control["trials"].pop()
    elif changed == "trial_fields":
        control["trials"][1] = {"result": [{"mode": "v8", "panel": "wrong mask"}]}
    elif changed == "fixture_identity":
        control["fixture"] = str(tmp_path / "different_universe")
    elif changed == "fixture_bytes":
        (fixture / "fixture.json").write_text(json.dumps({**manifest, "recipes": []}))
    elif changed == "manifest_code":
        manifest = {**manifest, "code_sha256": {"different_panel.py": "different"}}
    elif changed == "canonical_code":
        codes["canonical_panel.py"] = "changed feature formula"
    elif changed == "unplanned_code":
        codes["another_runtime.py"] = "unexpected extension"
    else:
        codes.pop(str(bench.SOURCE_CACHE))
    if changed != "control_bytes":
        bench.Path(capture["control"]).write_text(json.dumps(control))
        capture["control_sha256"] = sha256_file(bench.Path(capture["control"]))
    receipt.write_text(json.dumps(capture))
    with pytest.raises(ValueError):
        bench.independent_reference(receipt, fixture, manifest, codes)
