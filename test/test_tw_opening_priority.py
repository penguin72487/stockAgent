from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.live import opening_price_priority as priority
from stockagent.live import quote_provider

SESSION = "2026-10-22"
PREOPEN = datetime.fromisoformat(SESSION + "T08:50:00+08:00")


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts"))
    priority._ranked_hint.cache_clear()
    priority._loaded_hint.cache_clear()
    priority._permutation.cache_clear()


def row(symbol, *, weight=0., traded="2026-10-21", volume=100.):
    return {"symbol": symbol, "abs_model_weight": weight, "last_trade_date": traded,
            "mean_volume_shares_20d": volume, "volume_observed_days": 20 if volume is not None else 0}


def publish(root, rows, *, market="mode", prepared=PREOPEN, **kwargs):
    return priority.publish_preopen_priority(parquet_root=root, session_date=SESSION,
        market=market, feature_date="2026-10-21", rows=rows, model_identity={"checkpoint": market},
        prepared_at=prepared, **kwargs)


def order(root, symbols, session=SESSION):
    indices, evidence = priority.priority_indices(symbols, parquet_root=root, session_date=session)
    return [symbols[i] for i in indices], evidence


def test_six_mode_union_recency_then_history_then_share_volume_without_capital_bias(tmp_path):
    rows = [row("zero", volume=999999.), row("old_signal", weight=.8, traded="2026-10-19"),
            row("volume_signal", weight=.01, volume=1000.), row("weight_signal", weight=.9, volume=100.),
            row("no_history", traded=None, volume=None)]
    for i in range(6):
        publish(tmp_path, rows, market=f"mode{i}")
    symbols = ["zero", "no_history", "weight_signal", "old_signal", "volume_signal", "NEW"]
    ranked, evidence = order(tmp_path, symbols)
    assert ranked == ["volume_signal", "weight_signal", "old_signal", "zero", "no_history", "NEW"]
    assert sorted(ranked) == sorted(symbols)
    assert evidence["prepared_market_count"] == 6 and evidence["candidate_count"] == 3
    assert evidence["unprepared_symbol_count"] == 1
    assert symbols[0] == "zero", "the caller axis is immutable"


def test_different_model_previews_union_all_nonzero_weights(tmp_path):
    publish(tmp_path, [row("A", weight=.2), row("B")], market="one")
    publish(tmp_path, [row("A"), row("B", weight=.3)], market="two")
    ranked, evidence = order(tmp_path, ["C", "A", "B"])
    assert ranked == ["B", "A", "C"] and evidence["candidate_count"] == 2


def test_latest_source_owns_volume_not_sum_of_replicated_models(tmp_path):
    publish(tmp_path, [row("A", volume=50), row("B", volume=100)], market="one")
    publish(tmp_path, [row("A", volume=50)], market="two")
    publish(tmp_path, [row("A", volume=50)], market="three")
    assert order(tmp_path, ["A", "B"])[0] == ["B", "A"]


def test_volume_missing_is_not_zero_or_a_trade_and_only_completed_rows_are_read():
    panel = SimpleNamespace(num_symbols=3, symbols=["A", "B", "C"],
        dates=np.array(["2026-10-20", "2026-10-21", "2026-10-22"], dtype="datetime64[D]"),
        close_prices=np.ones((3, 3)), alive_mask=np.ones((3, 3), dtype=bool),
        daily_volumes=np.array([[1000., np.nan, 0.], [3000., np.nan, 0.], [1.e9, 1.e9, 1.e9]]))
    rows = priority.build_priority_rows(panel, 1, [{"symbol": s, "model_weight": -.1} for s in panel.symbols],
        np.array(["2026-10-21", "NaT", "2026-10-19"], dtype="datetime64[D]"))
    assert rows[0]["mean_volume_shares_20d"] == 2000., "shares are not multiplied/divided by 1000"
    assert rows[0]["abs_model_weight"] == .1 and rows[0]["volume_observed_days"] == 2
    assert rows[1]["last_trade_date"] is None and rows[1]["mean_volume_shares_20d"] is None
    assert rows[2]["mean_volume_shares_20d"] == 0 and rows[2]["last_trade_date"] == "2026-10-19"
    with pytest.raises(ValueError, match="cutoff"):
        priority.build_priority_rows(panel, 1, [], np.array(["2026-10-22"] * 3, dtype="datetime64[D]"))


@pytest.mark.parametrize("prepared", ["2026-10-22T09:00:00+08:00", "2026-10-22T23:00:00+08:00",
    "2026-10-21T08:50:00+08:00", "2026-10-22T08:50:00"])
def test_late_other_day_or_unclocked_warmup_cannot_backdate_preview(tmp_path, prepared):
    with pytest.raises(ValueError, match="pre-09:00"):
        publish(tmp_path, [row("A")], prepared=datetime.fromisoformat(prepared))
    assert order(tmp_path, ["A", "B"])[1]["status"] == "unavailable"


@pytest.mark.parametrize("bad", [
    {"abs_model_weight": float("nan")}, {"abs_model_weight": True},
    {"last_trade_date": "2026-10-22"}, {"last_trade_date": "bad"},
    {"mean_volume_shares_20d": -1.}, {"mean_volume_shares_20d": float("inf")},
    {"volume_observed_days": 21}, {"volume_observed_days": True}])
def test_invalid_numbers_or_future_evidence_rejected_without_dropping_scope(tmp_path, bad):
    with pytest.raises(ValueError):
        publish(tmp_path, [{**row("A"), **bad}])
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]


def test_hint_missing_corrupt_foreign_or_next_day_keeps_complete_caller_order(tmp_path):
    symbols = ["C", "A", "B"]
    assert order(tmp_path, symbols)[0] == symbols
    result = publish(tmp_path, [row("A", weight=1.), row("B")])
    assert order(tmp_path, symbols)[0] == ["A", "B", "C"]
    assert order(tmp_path, symbols, "2026-10-23")[0] == symbols
    path = priority._path(tmp_path, SESSION)
    assert path.stat().st_mode & 0o777 == 0o600
    from downloader.artifact_io import atomic_write_bytes
    atomic_write_bytes(path, b"{}", durable=True, mode=0o600)
    assert order(tmp_path, symbols)[0] == symbols
    assert result["measurement_boundary"] == "acquisition_hint_not_signal_or_fill"


@pytest.mark.parametrize("payload", [None, True, 42, "invalid", []])
def test_wrong_root_json_type_is_advisory_failure_not_source_failure(tmp_path, payload):
    from downloader.artifact_io import atomic_write_bytes
    publish(tmp_path, [row("A", weight=1.)])
    atomic_write_bytes(priority._path(tmp_path, SESSION), json.dumps(payload).encode(), mode=0o600)
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]
    # A later genuine warmup can repair the advisory file, never source data.
    publish(tmp_path, [row("A", weight=1.)])
    assert order(tmp_path, ["B", "A"])[0] == ["A", "B"]


def test_model_publication_invalidates_cache_and_old_completion_cannot_replace_new(tmp_path):
    publish(tmp_path, [row("A", weight=1.), row("B")])
    assert order(tmp_path, ["A", "B"])[0] == ["A", "B"]
    newer = PREOPEN.replace(minute=55)
    publish(tmp_path, [row("A"), row("B", weight=1.)], prepared=newer)
    assert order(tmp_path, ["A", "B"])[0] == ["B", "A"]
    assert publish(tmp_path, [row("A", weight=1.)])["status"] == "superseded"
    assert order(tmp_path, ["A", "B"])[0] == ["B", "A"]


def test_model_dependency_change_ignores_only_stale_mode_without_cold_hashing(tmp_path):
    from downloader.artifact_io import atomic_write_bytes
    config = tmp_path / "selected.yaml"
    atomic_write_bytes(config, b"identity: old", durable=True)
    identity = priority.model_identity_from_summary({"config_path": str(config), "config_fingerprint": "old"})
    priority.publish_preopen_priority(parquet_root=tmp_path, session_date=SESSION, market="changes",
        feature_date="2026-10-21", rows=[row("A", weight=1.)], model_identity=identity, prepared_at=PREOPEN)
    publish(tmp_path, [row("B", weight=.1)], market="stable")
    assert order(tmp_path, ["A", "B"])[0] == ["A", "B"]
    atomic_write_bytes(config, b"identity: changed", durable=True)
    ranked, evidence = order(tmp_path, ["A", "B"])
    assert ranked == ["B", "A"]
    assert evidence["prepared_market_count"] == 1 and evidence["stale_market_count"] == 1


def test_prefix_benchmark_reports_each_mode_even_if_reordering_hurts_one():
    from scripts.benchmark_opening_quote_priority import coverage_report
    original = [str(i) for i in range(600)]
    reordered = list(reversed(original))
    result = coverage_report(original, reordered, [
        {"market": "early", "rows": [row("0", weight=1.)]},
        {"market": "late", "rows": [row("599", weight=1.)]}])
    assert result["early"]["original"]["first_80"] == 1
    assert result["early"]["priority"]["first_80"] == 0
    assert result["late"]["priority"]["first_80"] == 1
    assert result["all_modes_union"]["candidate_count"] == 2


def test_concurrent_model_preparation_keeps_all_six_modes(tmp_path):
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda i: publish(tmp_path, [row(str(i), weight=.1)], market=str(i)), range(6)))
    ranked, evidence = order(tmp_path, [str(i) for i in reversed(range(6))])
    assert ranked == [str(i) for i in range(6)] and evidence["prepared_market_count"] == 6
    payload = json.loads(priority._path(tmp_path, SESSION).read_bytes())
    assert len(payload["markets"]) == 6


def test_private_preview_clears_a_known_future_open_gap_without_changing_formal_panel_path():
    from stockagent.live import signal_engine as signals
    panel = SimpleNamespace(num_symbols=2, symbols=["A", "B"],
        dates=np.array(["2026-10-20", "2026-10-21"], dtype="datetime64[D]"),
        close_prices=np.array([[100., 50.], [102., 51.]]), daily_volumes=np.ones((2, 2)),
        alive_mask=np.ones((2, 2), dtype=bool), feature_names=[signals.DAY_TRADE_OPEN_GAP_FEATURE],
        features=np.full((2, 2, 1), .3, dtype=np.float32))
    kwargs = dict(panel_idx=1, lookback=2, price_snapshot=quote_provider.PriceSnapshot(
        prices=np.array([102., 51.]), source="panel"), resolved_asof="2026-10-22 08:50:00",
        source_timezone="Asia/Taipei")
    ordinary = signals._day_trade_live_model_window(panel, **kwargs)[0]
    preview, cutoff, clock, live, observed = signals._day_trade_live_model_window(
        panel, preopen_quote_priority=True, **kwargs)
    np.testing.assert_array_equal(preview[-1, :, 0], [0., 0.])
    np.testing.assert_array_equal(preview[0], ordinary[0])
    assert np.all(ordinary == .3) and np.all(panel.features == .3)
    assert not live and not observed.any() and cutoff == clock == "2026-10-21"


@pytest.mark.parametrize("final_arm", [False, True])
@pytest.mark.parametrize("at,expected", [("08:50:00", True), ("09:05:00", False)])
def test_both_warmup_hooks_reuse_one_model_call_and_never_backdate_after_open(
    monkeypatch, tmp_path, final_arm, at, expected,
):
    from services.discord_bot import bot
    from stockagent.live.signal_engine import LiveSignalResult
    fixed = datetime.fromisoformat(SESSION + "T" + at + "+08:00")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz)
    monkeypatch.setattr(bot, "datetime", Clock)
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei", config_path="not-opened.yaml",
                          overnight_simulation_enabled=False, day_trade_rule_data_dir=None)
    monkeypatch.setattr(bot, "load_config", lambda _: SimpleNamespace(data=SimpleNamespace(parquet_root=tmp_path)))
    monkeypatch.setattr(bot, "_signal_kwargs", lambda **_: {"price_source": "panel"})
    monkeypatch.setattr(bot, "_runtime_status", lambda _: SimpleNamespace(data=SimpleNamespace(fresh=True)))
    monkeypatch.setattr(bot, "resolve_day_trade_rule_data_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(bot, "require_exact_session_eligibility", lambda **_: {})
    monkeypatch.setattr(bot, "_warm_tw_opening_sources", lambda **_: {"ready": True})
    monkeypatch.setattr(bot, "prepare_tw_price_limit_snapshot", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(bot, "_preopen_market_ready_for_session", lambda *_: True)
    for name in ("_run_pre_signal_command", "_clear_runtime_status_cache", "_sync_latest_live_weights_to_market_artifact",
                 "_touch_opening_attempt_progress", "_write_preopen_readiness", "_write_preopen_final_arm"):
        monkeypatch.setattr(bot, name, lambda *_args, **_kwargs: None)
    result = LiveSignalResult({"feature_cutoff_date": "2026-10-21", "live_latency": {
        "panel_cache_hit": True, "model_cache_hit": True, "checkpoint_cache_hit": True}},
        [{"symbol": "A", "current_price": 100.}], [], [], "", _opening_priority_rows=[row("A", weight=.2)])
    calls = []
    def generate(**kwargs):
        calls.append(kwargs)
        return result
    monkeypatch.setattr(bot, "generate_live_signal", generate)
    run = bot._final_arm_market_signal_sync if final_arm else bot._prewarm_market_signal_serialized
    assert run(cfg) is result
    assert len(calls) == 1 and calls[0]["write"] is False
    assert calls[0]["preopen_quote_priority"] is expected
    assert result.summary["opening_quote_priority"]["status"] == ("prepared" if expected else "not_preopen")
    assert order(tmp_path, ["B", "A"])[0] == (["A", "B"] if expected else ["B", "A"])


def test_priority_status_is_retained_in_both_existing_readiness_receipts(monkeypatch, tmp_path):
    from services.discord_bot import bot
    path = tmp_path / "readiness.json"
    monkeypatch.setattr(bot, "_preopen_readiness_path", lambda: path)
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei")
    proof = {"status": "prepared", "sha256": "pinned", "candidate_count": 3}
    for writer in (bot._write_preopen_readiness, bot._write_preopen_final_arm):
        writer(cfg, status="ready", started_at=PREOPEN.isoformat(), elapsed_seconds=.1,
               summary={"opening_quote_priority": proof})
    state = json.loads(path.read_bytes())["markets"]["test"]
    assert state["opening_quote_priority"] == state["final_arm"]["opening_quote_priority"] == proof


def test_cached_indices_and_metadata_cannot_be_mutated_by_a_consumer(tmp_path):
    publish(tmp_path, [row("A", weight=1.), row("B")])
    indices, metadata = priority.priority_indices(["B", "A"], parquet_root=tmp_path, session_date=SESSION)
    indices.reverse()
    metadata["candidate_count"] = 99
    metadata["markets"].append("foreign")
    indices, metadata = priority.priority_indices(["B", "A"], parquet_root=tmp_path, session_date=SESSION)
    assert indices == [1, 0] and metadata["candidate_count"] == 1
    assert metadata["markets"] == ["mode"]


@pytest.mark.parametrize("identity", ["bad", 1, {"dependencies": "bad"},
    {"dependencies": [{"path": "relative", "identity": [1, 2, 3, 4]}]},
    {"dependencies": [{"path": "/absolute", "identity": [True, 2, 3, 4]}]}])
def test_malformed_dependency_hint_cannot_break_source_acquisition(tmp_path, identity):
    with pytest.raises(ValueError):
        priority.publish_preopen_priority(parquet_root=tmp_path, session_date=SESSION, market="mode",
            feature_date="2026-10-21", rows=[row("A", weight=1.)], model_identity=identity, prepared_at=PREOPEN)
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]


def test_dependency_symlink_retarget_invalidates_priority(tmp_path):
    from downloader.artifact_io import atomic_write_bytes
    old, new, selected = tmp_path / "old.yaml", tmp_path / "new.yaml", tmp_path / "selected.yaml"
    atomic_write_bytes(old, b"old")
    atomic_write_bytes(new, b"new")
    selected.symlink_to(old)
    identity = priority.model_identity_from_summary({"config_path": str(selected)})
    priority.publish_preopen_priority(parquet_root=tmp_path, session_date=SESSION, market="mode",
        feature_date="2026-10-21", rows=[row("A", weight=1.)], model_identity=identity, prepared_at=PREOPEN)
    assert order(tmp_path, ["B", "A"])[0] == ["A", "B"]
    selected.unlink()  # This test-owned link only; source files stay intact.
    selected.symlink_to(new)
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]


def test_inherited_config_generation_is_not_relabelled_by_later_preparation(tmp_path):
    from downloader.artifact_io import atomic_write_bytes
    parent = tmp_path / "parent.yaml"
    atomic_write_bytes(parent, b"parent: old")
    captured = {"path": str(parent), "identity": list(priority._dependency_identity(parent))}
    # The generator captured this dependency while it loaded the model config.
    # An edit after inference must not get a new identity during publication.
    atomic_write_bytes(parent, b"parent: changed")
    model = priority.model_identity_from_summary({"preopen_priority_config_dependencies": [captured]})
    priority.publish_preopen_priority(parquet_root=tmp_path, session_date=SESSION, market="mode",
        feature_date="2026-10-21", rows=[row("A", weight=1.)], model_identity=model, prepared_at=PREOPEN)
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]


def test_deep_annual_inheritance_is_valid_and_every_parent_stays_fenced(tmp_path):
    from downloader.artifact_io import atomic_write_bytes
    dependencies = []
    for i in range(30):
        parent = tmp_path / f"parent{i}.yaml"
        atomic_write_bytes(parent, b"parent: original")
        dependencies.append({"path": str(parent), "identity": list(priority._dependency_identity(parent))})
    model = priority.model_identity_from_summary({"preopen_priority_config_dependencies": dependencies})
    priority.publish_preopen_priority(parquet_root=tmp_path, session_date=SESSION, market="annual",
        feature_date="2026-10-21", rows=[row("A", weight=1.)], model_identity=model, prepared_at=PREOPEN)
    assert order(tmp_path, ["B", "A"])[0] == ["A", "B"]
    atomic_write_bytes(tmp_path / "parent29.yaml", b"parent: changed")
    assert order(tmp_path, ["B", "A"])[0] == ["B", "A"]
