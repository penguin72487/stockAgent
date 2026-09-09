from datetime import date, datetime
import json

import numpy as np
import polars as pl
import pytest
import torch

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_stock_futures_day_trade import run_tw_stock_futures_day_trade_integer_torch
from stockagent.config import load_config
from stockagent.data.tw_stock_futures_minute import (
    EVENT_MINUTES, MINUTE_CONTRACT_VERSION, MINUTE_DATASET, MINUTE_MODE, TAPE_FIELDS,
    build_futures_minute_bars, load_futures_minute_tape,
)


def tape(rows=1, symbols=1):
    x = torch.zeros(rows, symbols, 2, TAPE_FIELDS)
    x[..., 0] = torch.tensor([2000, 100])
    x[..., 1] = 40
    x[..., 2] = 0.00002
    bars = x[..., 3:].reshape(rows, symbols, 2, 12, 5)
    bars[..., 0, :] = torch.tensor([100, 100, 100, 100, 100])
    bars[..., 1, :] = torch.tensor([105, 105, 105, 105, 100])
    bars[..., 6, :] = torch.tensor([110, 110, 110, 110, 100])
    return x


def execute(weights, x, **kw):
    return run_tw_stock_futures_day_trade_integer_torch(
        weights, x, initial_capital=1_000_000, scheduled_events=True, **kw,
    )


@pytest.mark.parametrize("direction", [1, -1])
def test_integer_round_trip_costs_capacity_and_gradients(direction):
    weights = torch.tensor([[direction * 0.25]], requires_grad=True)
    result = execute(weights, tape())
    assert result.contract_quantities_history.tolist() == [[[direction, direction * 4]]]
    # 1 standard and 4 mini; each side has fixed fee and rounded notional tax.
    expected = direction * 24_000 - (88 + 4 * 80)
    assert result.strategy_returns.item() == pytest.approx(np.log1p(expected / 1e6), abs=1e-7)
    assert result.turnovers.item() == pytest.approx(0.24 + 0.264)
    assert not result.residual_contract_quantities_history.any()
    assert result.final_alive
    result.strategy_returns.sum().backward()
    assert torch.isfinite(weights.grad).all() and weights.grad.abs().sum() > 0


def test_limit_requires_strict_future_cross_and_market_starts_after_1324():
    x = tape()
    bars = x[..., 3:].reshape(1, 1, 2, 12, 5)
    # 13:21 only touches limit: no fill. 13:22 crosses with one contract capacity.
    bars[..., 2, :] = torch.tensor([105, 105, 104, 105, 99])
    bars[..., 3, :] = torch.tensor([106, 107, 105, 106, 1])
    # 13:24 is still a limit bar. It must not execute this low at market.
    bars[..., 5, :] = torch.tensor([90, 91, 89, 90, 99])
    result = execute(torch.tensor([[0.25]]), x)
    expected = 2000 * 5 + 100 * 5 + 3 * 100 * 10 - (88 + 4 * 80)
    assert result.strategy_returns.item() == pytest.approx(np.log1p(expected / 1e6), abs=1e-7)


def test_future_exit_shortfall_never_reduces_entry_and_stops_account():
    x = tape(rows=2)
    x[0, ..., 3 + 6 * 5:] = 0
    weights = torch.tensor([[0.25], [0.25]], requires_grad=True)
    result = execute(weights, x)
    assert result.contract_quantities_history[0].tolist() == [[1, 4]]
    assert result.residual_contract_quantities_history[0].tolist() == [[1, 4]]
    assert result.default_history.tolist() == [True, False]
    assert not result.final_alive
    assert not result.contract_quantities_history[1].any()
    (-result.strategy_returns.sum()).backward()
    assert weights.grad[0, 0] > 0 and torch.isfinite(weights.grad).all()


def test_entry_capacity_no_redistribution_and_padding():
    x = tape(rows=2, symbols=2)
    x[:, 0, :, 7] = 0
    weights = torch.tensor([[0.25, -0.25], [0.25, -0.25]], requires_grad=True)
    result = execute(weights, x, state_advance_mask=torch.tensor([True, False]))
    assert result.contract_quantities_history[0].tolist() == [[0, 0], [-1, -4]]
    assert not result.contract_quantities_history[1].any()
    assert result.strategy_returns[1] == 0
    assert result.equity_scale_history[1] == result.equity_scale_history[0]


def test_chunk_equity_and_alive_match_whole_path():
    x = tape(rows=4)
    w = torch.full((4, 1), 0.25)
    whole = execute(w, x)
    first = execute(w[:2], x[:2])
    second = execute(w[2:], x[2:], initial_alive=first.final_alive,
                     initial_equity_scale=first.final_equity_scale)
    torch.testing.assert_close(whole.strategy_returns, torch.cat([first.strategy_returns, second.strategy_returns]))
    torch.testing.assert_close(whole.final_equity_scale, second.final_equity_scale)


def transactions():
    day = date(2026, 9, 3)
    times = [84500, 84559, 84600, 131959, 132000, 132359, 132400, 132959, 133000, 134400]
    return pl.DataFrame({
        "trading_date": [day] * len(times), "event_date": [day] * len(times),
        "session": ["day"] * len(times), "product": ["CDF"] * len(times),
        "delivery_month_week": ["202609"] * len(times), "event_time": list(map(str, times)),
        "event_ts": [datetime(2026, 9, 3, t // 10000, t // 100 % 100, t % 100) for t in times],
        "source_row_number": list(range(len(times))), "source_sha256": ["a" * 64] * len(times),
        "price": [100., 102., 999., 103., 104., 105., 106., 107., 888., 777.],
        "matched_quantity": [2.] * len(times),
    })


def test_bar_boundaries_ignore_later_open_and_daily_close():
    frame = build_futures_minute_bars(transactions())
    assert frame.filter(pl.col("minute") == 526)["vwap"].item() == 101
    assert frame.filter(pl.col("minute") == 800)["close"].item() == 103
    assert frame.filter(pl.col("minute") == 804)["close"].item() == 105
    assert frame.filter(pl.col("minute") == 805)["vwap"].item() == 106
    assert frame.filter(pl.col("minute") == 810)["vwap"].item() == 107
    assert set(frame["minute"].to_list()) <= set(EVENT_MINUTES)


def test_receipt_identity_coverage_hash_and_duplicates(tmp_path):
    frame = build_futures_minute_bars(transactions())
    path = tmp_path / "minutes.parquet"
    atomic_write_parquet(path, frame)
    receipt = {"dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
               "status": "complete", "source_daily_sha256": "daily",
               "covered_dates": ["2026-09-03"],
               "sources": [{"date": "2026-09-03", "sha256": "a" * 64,
                            "day_session_rows": 100, "day_last_time": 134459}],
               "outputs": {"minutes": {"sha256": sha256_file(path)}}}
    manifest = tmp_path / "manifest.json"
    atomic_write_json(manifest, receipt)
    keys = pl.DataFrame({"date": [date(2026, 9, 3)], "physical_contract": ["CDF:202609"],
                         "underlying_symbol": ["2330"], "candidate_slot": [0], "contract_multiplier": [2000.]})
    def load(dates=("2026-09-03",)):
        return load_futures_minute_tape(path, keys, np.array(dates, dtype="datetime64[D]"),
                                       ("2330",), daily_sha256="daily", fee=40, participation=.5)
    x, _ = load()
    assert x.shape == (1, 1, 2, TAPE_FIELDS) and x[0, 0, 0, 7] == 2
    receipt["sources"][0]["day_session_rows"] = 0
    atomic_write_json(manifest, receipt)
    with pytest.raises(ValueError, match="day-session"):
        load()
    receipt["sources"][0]["day_session_rows"] = 100
    atomic_write_json(manifest, receipt)
    with pytest.raises(ValueError, match="misses"):
        load(("2026-09-02", "2026-09-03"))
    atomic_write_parquet(path, pl.concat([frame, frame]))
    with pytest.raises(ValueError, match="SHA"):
        load()
    receipt["outputs"]["minutes"]["sha256"] = sha256_file(path)
    atomic_write_json(manifest, receipt)
    with pytest.raises(ValueError, match="duplicate"):
        load()


def test_canonical_dispatch_and_native_audit_roundtrip(tmp_path):
    from stockagent.training.trainer import _save_backtest_artifact, _load_backtest_artifact
    w = torch.tensor([[0.25]])
    result = run_backtest_torch(w, torch.zeros_like(w), torch.ones_like(w, dtype=torch.bool),
                               torch.zeros(1), buy_fee_rate=0, sell_fee_rate=0,
                               execution_mode=MINUTE_MODE, long_only=False,
                               max_turnover_ratio=0, portfolio_activation="pre_normalized",
                               overnight_returns=tape(), day_trade_execution_initial_capital=1e6)
    assert result.futures_contract_quantities_history.tolist() == [[[1, 4]]]
    path = tmp_path / "backtest.npz"
    _save_backtest_artifact(path, result.to_numpy(), np.array(["2026-09-03"], dtype="datetime64[D]"))
    restored, _ = _load_backtest_artifact(path)
    assert restored.futures_contract_quantities_history.tolist() == [[[1, 4]]]
    with np.load(path) as source:
        malformed = {name: source[name].copy() for name in source.files}
    malformed["futures_residual_contract_quantities_history"] = np.full((1, 1, 2), 0.5)
    np.savez(path, **malformed)
    with pytest.raises(ValueError, match="integer"):
        _load_backtest_artifact(path)
    failed = result.to_numpy()
    failed.futures_residual_contract_quantities_history[0, 0, 0] = 1
    with pytest.raises(ValueError, match="successful flat"):
        _save_backtest_artifact(path, failed, np.array(["2026-09-03"], dtype="datetime64[D]"))


def test_canonical_training_loss_uses_scheduled_tape_and_backpropagates():
    from stockagent.training.loss import risk_aware_loss
    weights = torch.tensor([[0.25]], requires_grad=True)
    loss = risk_aware_loss(
        weights, torch.zeros_like(weights), torch.ones_like(weights, dtype=torch.bool),
        buy_fee_rate=0, sell_fee_rate=0, objective="log_utility",
        execution_mode=MINUTE_MODE, long_only=False, max_turnover_ratio=0,
        log_utility_periods_per_year=1, gamma_turnover=0, concentration_weight=0,
        portfolio_activation="pre_normalized", overnight_log_returns=tape(),
        day_trade_execution_initial_capital=1e6,
    )
    assert loss.item() == pytest.approx(-np.log1p((24000 - 408) / 1e6), abs=1e-7)
    loss.backward()
    assert torch.isfinite(weights.grad).all() and weights.grad.item() < 0


@pytest.mark.parametrize('quarantine', [(), ('2026-09-04',)])
def test_dataset_preserves_no_fill_sessions_and_prior_only_feature_window(quarantine):
    from stockagent.data.panel import PanelData
    from stockagent.data.tw_stock_futures_day_trade import TaiwanStockFuturesDayTradeDaily
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    shape = (6, 2)
    dates = np.arange("2026-09-01", "2026-09-07", dtype="datetime64[D]")
    ones = np.ones(shape, bool)
    zeros = np.zeros(shape, np.float32)
    panel = PanelData(
        dates=dates, symbols=["2330", "2317"], feature_names=["date_code"],
        features=np.broadcast_to(np.arange(6, dtype=np.float32)[:, None, None], (6, 2, 1)).copy(),
        returns_1d=zeros, tradable_mask=ones, alive_mask=ones, can_buy_mask=ones,
        can_sell_mask=ones, can_short_open_mask=ones, benchmark_returns=np.zeros(6, np.float32),
        close_prices=np.ones(shape, np.float32),
    )
    x = tape(rows=6, symbols=2).numpy()
    x[5, ..., 3:] = 0  # Known contracts, but no actual trades on the last day.
    available = ones.copy()
    available[5] = False
    panel.stock_futures_day_trade_daily = TaiwanStockFuturesDayTradeDaily(
        dates=dates, symbols=tuple(panel.symbols), intraday_log_returns=zeros,
        policy_eligible_mask=ones, executable_mask=available,
        round_trip_cost_rate_per_open_notional=zeros, prior_volume_notional=zeros,
        benchmark_log_returns=np.zeros(6, np.float32), selected_rows=12, selected_underlyings=2,
        source_path="fixture", manifest_path="fixture", integer_candidate_execution=x,
        quarantined_decision_dates=quarantine,
    )
    dataset = CrossSectionalDataset(panel, np.arange(6), 2, execution_mode=MINUTE_MODE)
    expected = [2, 4, 5] if quarantine else [2, 3, 4, 5]
    assert dataset.valid_indices.tolist() == expected
    if quarantine:
        # All symbols lose this decision label, but the next day's feature
        # window still contains that independently valid stock session.
        assert dataset[1]['x'][:, 0, 0].tolist() == [2, 3]
    assert dataset[0]["x"][:, 0, 0].tolist() == [0, 1]
    assert dataset[len(dataset)-1]["tradable_mask"].all() and not dataset[len(dataset)-1]["can_buy_mask"].any()
    before = dataset[0]["x"].clone()
    panel.features[2:] = 999  # A future price/feature cannot enter the 08:45 window.
    torch.testing.assert_close(dataset[0]["x"], before)
    windowed = dataset_to_windowed_tensors(dataset)
    assert windowed.valid_indices.tolist() == expected
    assert windowed.overnight_log_returns.shape == (6, 2, 2, TAPE_FIELDS)
    torch.testing.assert_close(windowed.overnight_log_returns, torch.from_numpy(x))


def test_config_clock_and_checkpoint_are_separate():
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract
    config = load_config("configs/markets/tw_stock_futures_day_trade_0845_minute.yaml")
    assert config.trading.execution_mode == MINUTE_MODE
    assert not config.data.day_trade_open_feature
    assert config.data.feature_exclude == ["next_session_open_gap_logret"]
    assert config.training.epochs == 1000
    assert config.environment.amp_dtype == "bf16"
    contract = _trading_checkpoint_contract(config)["taiwan_stock_futures_day_trade"]
    assert not contract["current_session_stock_open_feature"]
    assert len(contract["execution_tensor_channels"]) == TAPE_FIELDS
    assert "1330" in contract["execution_clock"]
    assert "proxy" not in json.dumps(contract)


@pytest.mark.parametrize("check_only", [False, True])
@pytest.mark.parametrize("source_state", ["complete", "night_only", "missing", "missing_root", "partial"])
def test_builder_rejects_incomplete_source_without_replacing_accepted_pair(tmp_path, monkeypatch, source_state, check_only):
    import argparse
    from scripts import build_tw_stock_futures_0900_entries as builder
    from test_tw_stock_futures_day_trade import _candidate
    from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    daily = tmp_path / "daily.parquet"
    candidates = [_candidate(
        day=date(2026, 9, 3), product="CDF", prior_volume=100, current_volume=200,
    )]
    if source_state == "partial":
        candidates.append(_candidate(day=date(2026, 9, 2), product="CDF", prior_volume=100, current_volume=200))
    atomic_write_parquet(daily, pl.DataFrame(candidates))
    atomic_write_json(daily.with_name("manifest.json"), {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "outputs": {"continuous_daily": {"sha256": sha256_file(daily)}},
    })
    raw = tmp_path / "raw"
    if source_state != "missing_root":
        raw.mkdir()
    archive = raw / "Daily_2026_09_03.zip"
    if source_state not in {"missing", "missing_root"}:
        archive.write_bytes(b"immutable raw parser fixture")
    output = tmp_path / "minutes"
    output.mkdir()
    (output / "minutes.parquet").write_bytes(b"accepted data")
    (output / "manifest.json").write_bytes(b"accepted receipt")
    monkeypatch.setattr(builder, "parse_args", lambda: argparse.Namespace(
        start_date="2026-09-02", end_date="2026-09-03", daily_data_path=daily,
        ticks_root=raw, output_dir=output, execution_policy="scheduled_0846", archive_override=[],
        check_only=check_only,
    ))
    def parse_archive(*args, **kwargs):
        assert not check_only and source_state not in {"missing", "missing_root", "partial"}
        return transactions().with_columns(
            pl.lit("day" if source_state == "complete" else "night").alias("session"),
            pl.lit(kwargs["source_sha256"]).alias("source_sha256"),
        )
    monkeypatch.setattr(builder, "_parse_zip", parse_archive)
    result = builder.main()
    if check_only:
        assert result == (2 if source_state in {"missing", "missing_root", "partial"} else 0)
        assert (output / "minutes.parquet").read_bytes() == b"accepted data"
        assert (output / "manifest.json").read_bytes() == b"accepted receipt"
        assert not (output / "build_failure.json").exists()
    elif source_state == "complete":
        assert result == 0
        proof = json.loads((output / "manifest.json").read_text())
        assert proof["status"] == "complete" and proof["covered_dates"] == ["2026-09-03"]
        assert proof["outputs"]["minutes"]["sha256"] == sha256_file(output / "minutes.parquet")
    else:
        assert result == 2
        assert (output / "minutes.parquet").read_bytes() == b"accepted data"
        assert (output / "manifest.json").read_bytes() == b"accepted receipt"
        assert json.loads((output / "build_failure.json").read_text())["status"] == "partial"


def test_builder_inherits_effective_minute_config(monkeypatch):
    import sys
    from pathlib import Path
    from scripts import build_tw_stock_futures_0900_entries as builder
    path = "configs/markets/tw_stock_futures_day_trade_0845_minute.yaml"
    config = load_config(path)
    monkeypatch.setattr(sys, "argv", ["builder", "--config", path, "--ticks-root", "raw", "--check-only"])
    args = builder.parse_args()
    assert args.execution_policy == "scheduled_0846"
    assert args.start_date == config.data.panel_start_date
    assert args.daily_data_path == Path(config.trading.tw_stock_futures_day_trade_data_path)
    assert args.output_dir == Path(config.trading.tw_stock_futures_day_trade_minute_data_path).parent
    assert args.check_only


def test_builder_refuses_output_inside_materialized_release(tmp_path, monkeypatch):
    import argparse
    from scripts import build_tw_stock_futures_0900_entries as builder
    root = tmp_path / "tw-futures-fixture"
    root.mkdir()
    (tmp_path / ".tw-futures-fixture.READY.json").write_text("{}")
    output = root / "minutes"
    monkeypatch.setattr(builder, "parse_args", lambda: argparse.Namespace(
        start_date="2026-09-03", end_date=None, output_dir=output, check_only=False,
    ))
    with pytest.raises(ValueError, match="immutable packed/materialized"):
        builder.main()
    assert not output.exists()


def test_training_rejects_missing_minutes_before_panel_or_ddp(tmp_path, monkeypatch):
    import sys
    import train
    from stockagent.data import panel
    config = load_config("configs/markets/tw_stock_futures_day_trade_0845_minute.yaml")
    config.trading.tw_stock_futures_day_trade_data_path = str(tmp_path / "daily.parquet")
    config.trading.tw_stock_futures_day_trade_minute_data_path = str(tmp_path / "minutes.parquet")
    monkeypatch.setattr(train, "load_config", lambda _: config)
    monkeypatch.setattr(sys, "argv", ["train.py", "--config", "selected config.yaml"])
    def forbidden(*args, **kwargs):
        pytest.fail("missing minute input must fail before panel construction or DDP launch")
    monkeypatch.setattr(train, "_maybe_relaunch_for_ddp", forbidden)
    monkeypatch.setattr(panel, "build_panel", forbidden)
    with pytest.raises(SystemExit) as exc:
        train.main()
    message = str(exc.value)
    assert "--config 'selected config.yaml'" in message
    assert "--check-only" in message and "epochs=1000" in message
    assert "panel_start_date=2014-01-01" in message
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", ["valid", "recent_only", "panel_gap", "daily_hash_mismatch"])
def test_check_data_only_validates_actual_panel_dates_without_training(tmp_path, monkeypatch, case):
    import sys
    from types import SimpleNamespace
    import train
    from stockagent.data import panel
    from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    from test_tw_stock_futures_day_trade import _candidate

    config = load_config("configs/markets/tw_stock_futures_day_trade_0845_minute.yaml")
    config.walk_forward.expected_first_year = 2014 if case == "recent_only" else 2026
    daily = tmp_path / "daily" / "continuous_daily.parquet"
    daily.parent.mkdir()
    atomic_write_parquet(daily, pl.DataFrame([_candidate(
        day=date(2026, 9, 3), product="CDF", prior_volume=100, current_volume=100,
    )]))
    daily_sha = sha256_file(daily)
    atomic_write_json(daily.with_name("manifest.json"), {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "outputs": {"continuous_daily": {"sha256": daily_sha}},
    })
    minute = tmp_path / "minute" / "minutes.parquet"
    atomic_write_parquet(minute, build_futures_minute_bars(transactions()))
    atomic_write_json(minute.with_name("manifest.json"), {
        "dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
        "status": "complete", "source_daily_sha256": daily_sha,
        "covered_dates": ["2026-09-03"],
        "sources": [{"date": "2026-09-03", "sha256": "a" * 64,
                     "day_session_rows": 100, "day_last_time": 134459}],
        "outputs": {"minutes": {"sha256": sha256_file(minute)}},
    })
    if case == "daily_hash_mismatch":
        daily.write_bytes(b"changed daily source")
    config.trading.tw_stock_futures_day_trade_data_path = str(daily)
    config.trading.tw_stock_futures_day_trade_minute_data_path = str(minute)
    monkeypatch.setattr(train, "load_config", lambda _: config)
    monkeypatch.setattr(sys, "argv", ["train.py", "--check-data-only"])
    def forbidden(*args, **kwargs):
        pytest.fail("data-only validation must never launch training/DDP")
    monkeypatch.setattr(train, "_maybe_relaunch_for_ddp", forbidden)
    def build_panel(*args, **kwargs):
        assert case not in {"recent_only", "daily_hash_mismatch"}
        dates = ["2026-09-02", "2026-09-03"] if case == "panel_gap" else ["2026-09-03"]
        return SimpleNamespace(dates=np.array(dates, dtype="datetime64[D]"), symbols=("2330",))
    monkeypatch.setattr(panel, "build_panel", build_panel)
    if case == "valid":
        assert train.main() is None
    else:
        message = {"recent_only": "first panel year 2014", "panel_gap": "misses 1 panel dates",
                   "daily_hash_mismatch": "daily candidate source contract or SHA mismatch"}[case]
        with pytest.raises(SystemExit, match=message):
            train.main()
