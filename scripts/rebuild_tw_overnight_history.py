#!/usr/bin/env python3
"""Recompute existing 13:25 overnight adapters in an isolated historical ledger."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.rebuild_tw_day_trade_open_price_replay import (
    _load_price_limits, _official_aggregate_daily_rows, _sha256,
)
from scripts.run_tw_overnight_simulation import _mode_specs
from scripts.switch_tw_day_trade_strategy import _latest_completed_session
from stockagent.config import load_config
from stockagent.data.tw_overnight import (
    OVERNIGHT_CLOSE_FALLBACK_CAVEAT, overnight_minute_manifest,
    read_overnight_1325_partition,
)
from stockagent.live.market_config import load_market_config
from stockagent.live.signal_engine import _build_panel, generate_live_signal
from stockagent.live.tw_day_trade_simulation import TAIPEI, load_symbol_metadata
from stockagent.live.tw_overnight_replay import TwOvernightHistoricalReplayEngine


HISTORY_LINEAGE_CONTRACT = "tw_overnight_counterfactual_history_v2"


def _history_lineage(
    start_date: str,
    markets: list[dict[str, Any]],
) -> dict[str, Any]:
    identity = {
        "contract": HISTORY_LINEAGE_CONTRACT,
        "start_date": start_date,
        "execution_clock": "close_entry_next_session_open_exit",
        "markets": [
            {
                "market": row["market"],
                "checkpoint_sha256": row["checkpoint_sha256"],
                "market_config_sha256": row["market_config_sha256"],
            }
            for row in sorted(markets, key=lambda value: value["market"])
        ],
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return {**identity, "fingerprint_sha256": hashlib.sha256(encoded).hexdigest()}


def configured_history_lineage(markets_dir: Path, start_date: str) -> dict[str, Any]:
    specs, _configs, errors = _mode_specs(markets_dir)
    if errors:
        raise ValueError(errors)
    markets = []
    for spec in specs:
        market_path = markets_dir / f"{spec.market}.yaml"
        markets.append(
            {
                "market": spec.market,
                "checkpoint_sha256": _sha256(Path(spec.checkpoint_path)),
                "market_config_sha256": _sha256(market_path),
            }
        )
    if not markets:
        raise ValueError("No enabled overnight market is configured")
    return _history_lineage(start_date, markets)


def _at(day: str, hour: int, minute: int) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(hour, minute), tzinfo=TAIPEI)


def _positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) and number > 0 else None


def _write_decision_csv(frame: pl.DataFrame, path: Path, day: str) -> None:
    frame.filter(pl.col('price').is_not_null()).with_columns(
        pl.when(pl.col('decision_source') == 'observed_1325')
        .then(pl.lit(int(_at(day, 13, 25).timestamp() * 1000)))
        .otherwise(pl.lit(int(_at(day, 13, 30).timestamp() * 1000)))
        .alias('effective_timestamp_ms'),
    ).select(
        'symbol', 'price', 'open_price', 'upper_limit_price', 'lower_limit_price',
        'reference_price', 'effective_timestamp_ms',
    ).write_csv(path)


def _plot_history(output: Path, result: dict[str, Any], marks: pl.DataFrame) -> None:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axis = plt.subplots(figsize=(13, 6.5))
    for item in result['markets']:
        rows = marks.filter(pl.col('market') == item['market']).sort('minute')
        values = rows['total_equity_twd'].to_numpy() / item['initial_capital_twd'] - 1
        axis.plot([datetime.fromisoformat(value) for value in rows['minute']], values * 100,
                  label=item['market'].replace('tw_overnight_', ''), linewidth=1.5)
    axis.axhline(0, color='gray', linewidth=0.7)
    axis.axhline(-100, color='gray', linewidth=0.7, linestyle='--')
    axis.set(title=f'Counterfactual overnight replay | {result["start_date"]} to {result["end_date"]}',
             ylabel='Raw cumulative net PnL / initial capital (%)', xlabel='Session')
    axis.legend()
    axis.grid(alpha=0.2)
    fig.text(0.5, 0.01, 'Each cohort is sized from pre-entry account equity. '
             'Official daily auction prices are counterfactual proxies: see report.',
             ha='center', fontsize=9)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(output / 'equity_curve.png', dpi=160)
    plt.close(fig)


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    end, days, calendar_sha = _latest_completed_session(
        tw_public_dir=args.public_root, start_date=date.fromisoformat(args.start_date),
    )
    if args.end_date != "latest":
        end = min(end, date.fromisoformat(args.end_date))
        days = [day for day in days if day <= end]
    if not days:
        raise ValueError("No completed sessions in requested range")
    specs, configs, errors = _mode_specs(args.markets_dir)
    if errors:
        raise ValueError(errors)
    if args.market:
        specs = [spec for spec in specs if spec.market in args.market]
    if not specs or (args.market and set(args.market) != {spec.market for spec in specs}):
        raise ValueError("Requested overnight market is not enabled")
    manifest_path = args.minute_root / "manifest.json"
    manifest_sha = _sha256(manifest_path)
    minute = overnight_minute_manifest(args.minute_root)
    parts = {part['trade_date']: part for part in minute['partitions']}
    source_paths = {
        'twse_daily': args.public_root / 'twse_daily_ohlcv.parquet',
        'tpex_daily': args.public_root / 'tpex_daily_ohlcv.parquet',
        'minute_manifest': manifest_path,
        'calendar': args.public_root / 'twse_taiex_ohlc.parquet',
    }
    source_hashes = {key: _sha256(path) for key, path in source_paths.items()}
    if source_hashes['calendar'] != calendar_sha:
        raise RuntimeError('Official calendar changed during preparation')
    markets = []
    for spec in specs:
        path = args.markets_dir / f'{spec.market}.yaml'
        cfg = configs[spec.market]
        fold_dir = Path(spec.checkpoint_path).parent
        complete_path = fold_dir / 'fold_complete.json'
        complete = json.loads(complete_path.read_text())
        if (complete.get('status') != 'complete' or complete.get('fold_id') != cfg.fold_id
                or max(complete['train_years'] + complete['val_years']) >= days[0].year):
            raise ValueError(f'Checkpoint scope is not completed and out of sample: {fold_dir}')
        if fold_dir.resolve() != (Path(cfg.output_dir) / f'fold_{cfg.fold_id:02d}').resolve():
            raise ValueError('Checkpoint is outside its configured fold')
        # These are the already deployed models, with no replacement selector.
        # Record legacy sidecar absence; canonical inference must validate the
        # embedded checkpoint manifest against the actual panel/config.
        mode_contract = fold_dir / 'mode_artifact_contract.json'
        evidence = {
            'fold_complete': str(complete_path.resolve()),
            'fold_complete_sha256': _sha256(complete_path),
            'legacy_mode_artifact_contract_missing': not mode_contract.is_file(),
            'mode_artifact_contract_sha256': _sha256(mode_contract) if mode_contract.is_file() else None,
            'compatibility_gate': 'generate_live_signal.validate_checkpoint_manifest',
        }
        markets.append({
            'market': spec.market, 'label': spec.label, 'capital': spec.initial_capital_twd,
            'market_config': str(path.resolve()), 'market_config_sha256': _sha256(path),
            'checkpoint': str(spec.checkpoint_path), 'checkpoint_sha256': _sha256(Path(spec.checkpoint_path)),
            'lifecycle': evidence,
        })
    plan = {
        'schema_version': 1, 'start_date': str(days[0]), 'end_date': str(days[-1]),
        'sessions': list(map(str, days)), 'markets': markets,
        'source_files': {key: {'path': str(path.resolve()), 'sha256': source_hashes[key]}
                         for key, path in source_paths.items()},
        'simulation_only': True, 'production_order_possible': False,
        'missing_1325_policy': 'same_session_close', 'caveat': OVERNIGHT_CLOSE_FALLBACK_CAVEAT,
        'auction_contract': 'official_daily_open_close_counterfactual_no_exchange_timestamp_or_queue_proof',
        'latest_close_cohort': 'retain_open_until_next_observed_session_open',
        'model_scope': 'existing_four_day_trade_checkpoint_adapters_not_new_overnight_training',
    }
    plan['history_lineage'] = _history_lineage(plan['start_date'], markets)
    args.output.mkdir(parents=True, exist_ok=True)
    old = args.output / 'plan.json'
    if old.is_file():
        previous = json.loads(old.read_text())
        old_sessions = list(previous.get('sessions') or ())
        if (
            previous.get('start_date') != plan['start_date']
            or old_sessions != plan['sessions'][:len(old_sessions)]
            or [
                (row.get('market'), row.get('checkpoint_sha256'), row.get('market_config_sha256'))
                for row in previous.get('markets') or ()
            ] != [
                (row.get('market'), row.get('checkpoint_sha256'), row.get('market_config_sha256'))
                for row in plan['markets']
            ]
        ):
            raise ValueError('Existing history is not an immutable prefix of the requested plan')
    atomic_write_json(old, plan)
    if args.stage == 'plan':
        return plan
    for index, day in enumerate(days, 1):
        day_text = str(day)
        destination = args.output / 'inputs' / day_text
        receipt_path = destination / 'source.json'
        prices_path = destination / 'prices.parquet'
        if receipt_path.is_file():
            prior = json.loads(receipt_path.read_text())
            if prices_path.is_file() and _sha256(prices_path) == prior['prices_sha256']:
                csv_path = destination / 'decision_prices.csv'
                if _sha256(csv_path) != prior['decision_csv_sha256']:
                    raise RuntimeError(f'Historical decision input changed: {day}')
                if 'timestamp_contract' not in prior:
                    _write_decision_csv(pl.read_parquet(prices_path), csv_path, day_text)
                    prior['decision_csv_sha256'] = _sha256(csv_path)
                    prior['timestamp_contract'] = 'historical_effective_observation_time_not_exchange_timestamp'
                    atomic_write_json(receipt_path, prior)
                continue
            raise RuntimeError(f'Cached historical input changed: {day}')
        official = _official_aggregate_daily_rows(
            source_paths['twse_daily'], source_paths['tpex_daily'], day,
        )
        if not official:
            raise RuntimeError(f'Official completed session has no price rows: {day}')
        limits_path = args.price_limit_dir / f'{day}.parquet'
        limits = _load_price_limits(limits_path)
        partition = args.minute_root / f'trade_date={day}' / 'data.parquet'
        if not partition.resolve().is_relative_to(args.minute_root.resolve()):
            raise ValueError('Minute partition escapes selected source')
        minute_prices = (read_overnight_1325_partition(
            partition, day_text, parts[day_text]['output_sha256'],
        ) if day_text in parts and partition.exists() else {})
        rows = []
        for symbol, row in sorted(official.items()):
            observed = minute_prices.get(symbol)
            price = observed if observed is not None else _positive(row.get('close'))
            limit = limits.get(symbol) or {}
            rows.append({
                **row, 'date': day_text, 'symbol': symbol, 'price': price,
                'open_price': _positive(row.get('open')),
                'decision_source': 'observed_1325' if observed is not None else 'same_session_close' if price else 'missing',
                'upper_limit_price': limit.get('upper_limit_price'),
                'lower_limit_price': limit.get('lower_limit_price'),
                'reference_price': limit.get('reference_price'),
            })
        destination.mkdir(parents=True, exist_ok=True)
        frame = pl.DataFrame(rows)
        frame.write_parquet(prices_path)
        _write_decision_csv(frame, destination / 'decision_prices.csv', day_text)
        receipt = {
            'session_date': day_text, 'source_hashes': {**source_hashes, 'price_limits': _sha256(limits_path)},
            'minute_partition_sha256': parts.get(day_text, {}).get('output_sha256') if partition.is_file() else None,
            'prices_sha256': _sha256(prices_path), 'decision_csv_sha256': _sha256(destination / 'decision_prices.csv'),
            'timestamp_contract': 'historical_effective_observation_time_not_exchange_timestamp',
            'counts': {key: sum(row['decision_source'] == key for row in rows)
                       for key in ('observed_1325', 'same_session_close', 'missing')},
        }
        atomic_write_json(receipt_path, receipt)
        if index == 1 or index % 20 == 0 or index == len(days):
            print(f'[inputs] {index}/{len(days)} {day}', flush=True)
    if _sha256(manifest_path) != manifest_sha:
        raise RuntimeError('Minute source manifest changed during preparation')
    return plan


def infer_signals(args: argparse.Namespace, plan: dict[str, Any]) -> None:
    for item in plan['markets']:
        cfg = load_market_config(item['market_config'])
        config = load_config(cfg.config_path)
        config.data.live_tail_panel_rows = max(
            int(config.data.live_tail_panel_rows or 0),
            len(plan['sessions']) + int(config.training.lookback) + 32,
        )
        panel, _, _ = _build_panel(config, live_tail=True)
        calendar = np.asarray(panel.dates, dtype='datetime64[D]')
        for index, day in enumerate(plan['sessions'], 1):
            directory = args.output / 'signals' / item['market'] / day
            receipt_path = directory / 'replay_signal.json'
            if receipt_path.is_file():
                receipt = json.loads(receipt_path.read_text())
                if all(_sha256(Path(receipt[key])) == receipt[key + '_sha256']
                       for key in ('summary_path', 'weights_path')):
                    continue
                raise RuntimeError(f'Historical signal changed: {receipt_path}')
            prior = calendar[calendar < np.datetime64(day)]
            if len(prior) < config.training.lookback:
                raise ValueError(f'Not enough prior context for {item["market"]}/{day}')
            input_dir = args.output / 'inputs' / day
            provenance = json.loads((input_dir / 'source.json').read_text())
            kwargs = cfg.signal_kwargs(
                live_output_dir=str(directory), panel_date=str(prior[-1]),
                asof_date=_at(day, 13, 25).isoformat(), price_source='csv',
                prices_csv=str(input_dir / 'decision_prices.csv'),
                signal_id=f'{item["market"]}-counterfactual-1325-{day}',
                ensure_previous_signal=False, previous_signal_backfill_limit=0,
                market_notice='歷史 13:25 隔日沖反事實研究；缺價使用同日收盤替代，不是當時已發出的訊號。',
                write=True,
            )
            kwargs.update(_panel_override=panel, publish_latest=False,
                          day_trade_model_observation='latest_quote')
            result = generate_live_signal(**kwargs)
            if not result.summary.get('live_session_latest_quote_feature_applied'):
                raise RuntimeError(f'{day}: 13:25 observation was not applied')
            result_path = Path(result.output_dir)
            summary_path = result_path / 'summary.json'
            summary = {**result.summary,
                       'counterfactual_signal_regeneration': True,
                       'counterfactual_generated_at': result.summary.get('generated_at'),
                       'replay_effective_signal_at': _at(day, 13, 25).isoformat(),
                       'counterfactual_1325_provenance': provenance,
                       'simulation_only': True, 'production_order_possible': False}
            atomic_write_json(summary_path, summary)
            weights_path = result_path / 'target_weights.parquet'
            atomic_write_json(receipt_path, {
                'summary_path': str(summary_path.resolve()), 'summary_path_sha256': _sha256(summary_path),
                'weights_path': str(weights_path.resolve()), 'weights_path_sha256': _sha256(weights_path),
            })
            if index == 1 or index % 10 == 0 or index == len(plan['sessions']):
                print(f'[signals] {item["market"]} {index}/{len(plan["sessions"])} {day}', flush=True)


def replay(args: argparse.Namespace, plan: dict[str, Any]) -> None:
    specs, _, errors = _mode_specs(args.markets_dir)
    if errors:
        raise ValueError(errors)
    specs = [spec for spec in specs if spec.market in {m['market'] for m in plan['markets']}]
    signal_validation = []
    for spec in specs:
        identities = set()
        for day in plan['sessions']:
            receipt = json.loads((args.output / 'signals' / spec.market / day / 'replay_signal.json').read_text())
            if not all(_sha256(Path(receipt[key])) == receipt[key + '_sha256']
                       for key in ('summary_path', 'weights_path')):
                raise RuntimeError(f'Historical signal changed: {spec.market}/{day}')
            summary = json.loads(Path(receipt['summary_path']).read_text())
            if (summary.get('replay_effective_signal_at') != _at(day, 13, 25).isoformat()
                    or str(summary.get('feature_cutoff_date', ''))[:10] >= day
                    or summary.get('live_session_latest_quote_feature_applied') is not True
                    or summary.get('day_trade_model_observation') != 'latest_quote'
                    or datetime.fromisoformat(summary['generated_at']) < _at(day, 13, 25)):
                raise ValueError(f'Historical signal timing contract failed: {spec.market}/{day}')
            identities.add((summary['checkpoint_fingerprint'], summary['config_fingerprint']))
        if len(identities) != 1:
            raise ValueError(f'Model/config changed within history: {spec.market}')
        signal_validation.append({'market': spec.market, 'sessions': len(plan['sessions']),
                                  'checkpoint_fingerprint': next(iter(identities))[0],
                                  'config_fingerprint': next(iter(identities))[1],
                                  'prior_completed_features_verified': True})
    metadata = {spec.market: {s: row.get('security_type', 'stock') for s, row in load_symbol_metadata(spec.parquet_root).items()}
                for spec in specs}
    ledger = args.output / 'ledgers' / datetime.now(TAIPEI).strftime('%Y%m%dT%H%M%S%f')
    engine = TwOvernightHistoricalReplayEngine(ledger)
    engine.update_readiness(specs, now=_at(plan['start_date'], 8, 30))
    for index, day in enumerate(plan['sessions'], 1):
        source = json.loads((args.output / 'inputs' / day / 'source.json').read_text())
        rows = pl.read_parquet(args.output / 'inputs' / day / 'prices.parquet').to_dicts()
        by_symbol = {row['symbol']: row for row in rows}
        engine.select_session(day, by_symbol, source)
        def quotes(column: str) -> dict[str, dict[str, Any]]:
            return {symbol: {
                'last': _positive(row.get(column)), 'open': _positive(row.get('open')),
                'upper_limit': row.get('upper_limit_price'), 'lower_limit': row.get('lower_limit_price'),
                'historical_limits_session_date': day,
                'historical_limits_sha256': source['source_hashes']['price_limits'],
            } for symbol, row in by_symbol.items()}
        engine.process_quotes(quotes=quotes('open'), now=_at(day, 9, 0))
        for spec in specs:
            receipt = json.loads((args.output / 'signals' / spec.market / day / 'replay_signal.json').read_text())
            summary = json.loads(Path(receipt['summary_path']).read_text())
            signal_rows = pl.read_parquet(receipt['weights_path']).to_dicts()
            if not signal_rows:
                raise ValueError(f'Historical signal has no model rows: {spec.market}/{day}')
            outcome = engine.register_close_signal(
                spec=spec, summary=summary, signal_rows=signal_rows,
                quotes=quotes('price'), security_types=metadata[spec.market], now=_at(day, 13, 25),
            )
            if outcome != 'registered':
                mode = engine.state['modes'][spec.market]
                reason = mode.get('blocked_reason') or outcome
                unresolved = [
                    f"{row.get('session_date')}:{row.get('symbol')}:{row.get('opening_exit_order_status')}"
                    for row in (mode.get('positions') or {}).values()
                    if int(row.get('signed_shares') or 0) != 0
                ]
                detail = f"; unresolved={','.join(unresolved[:10])}" if unresolved else ''
                raise ValueError(
                    f'Historical signal rejected: {spec.market}/{day}: {reason}{detail}'
                )
        engine.process_quotes(quotes=quotes('close'), now=_at(day, 13, 30))
        # Expire unfilled close orders without inventing an execution price.
        engine.process_quotes(quotes=quotes('close'), now=_at(day, 13, 34), append_mark_history=False)
        if index == 1 or index % 20 == 0 or index == len(plan['sessions']):
            print(f'[replay] {index}/{len(plan["sessions"])} {day}', flush=True)
    for proof in plan['source_files'].values():
        if _sha256(Path(proof['path'])) != proof['sha256']:
            raise RuntimeError(f'Source changed while computing history: {proof["path"]}')
    for market in plan['markets']:
        if _sha256(Path(market['checkpoint'])) != market['checkpoint_sha256']:
            raise RuntimeError('Checkpoint changed during history calculation')
    result = {
        'status': 'computed_counterfactual_history', 'start_date': plan['start_date'], 'end_date': plan['end_date'],
        'sessions': len(plan['sessions']), 'ledger': str(ledger.resolve()),
        'simulation_only': True, 'production_order_possible': False,
        'live_state_modified': False, 'new_overnight_training_model_used': False,
        'signal_validation': signal_validation,
        'observation_frequency': 'opening_and_closing_events',
        'markets': [{
            'market': spec.market, 'label': spec.label,
            **{key: engine.state['modes'][spec.market].get(key) for key in (
                'initial_capital_twd', 'total_equity_twd', 'cumulative_realized_net_pnl_twd',
                'open_net_liquidation_pnl_twd', 'open_position_count', 'engine_status', 'valuation_stale',
            )},
        } for spec in specs],
    }
    marks = pl.read_ndjson(engine.marks_path).select(
        'market', 'session_date', 'minute', 'initial_capital_twd', 'total_equity_twd',
        'cumulative_realized_net_pnl_twd', 'open_net_liquidation_pnl_twd',
        'open_position_count', 'valuation_stale', 'valuation_basis',
    )
    marks.write_parquet(args.output / 'equity_history.parquet')
    marks.write_csv(args.output / 'equity_history.csv')
    if marks.height != len(plan['sessions']) * len(specs) * 2:
        raise RuntimeError('Opening/closing equity event coverage is incomplete')
    fills = [json.loads(line) for line in engine.fills_path.read_text().splitlines()]
    price_maps = {
        day: {row['symbol']: row for row in pl.read_parquet(
            args.output / 'inputs' / day / 'prices.parquet').to_dicts()}
        for day in plan['sessions']
    }
    for fill in fills:
        if (fill.get('exchange_match_at') is not None or fill.get('counterfactual') is not True
                or 'counterfactual' not in fill.get('fill_contract', '')
                or int(fill['quantity']) % 1000):
            raise ValueError('Historical fill provenance/lot validation failed')
        column = 'close' if fill['purpose'] == 'close_auction_entry' else 'open'
        actual_price = price_maps[fill['session_date']][fill['symbol']][column]
        if not np.isclose(float(fill['price']), float(actual_price), rtol=0, atol=1e-9):
            raise ValueError('Historical fill differs from selected official price')
    for item in result['markets']:
        realized = sum(float(fill.get('net_pnl_twd') or 0)
                       for fill in fills if fill['market'] == item['market'])
        if not np.isclose(realized, item['cumulative_realized_net_pnl_twd'], rtol=0, atol=0.01):
            raise ValueError('Realized account PnL disagrees with fills ledger')
        if not np.isclose(item['total_equity_twd'], item['initial_capital_twd'] + realized
                          + item['open_net_liquidation_pnl_twd'], rtol=0, atol=0.01):
            raise ValueError('Ending equity disagrees with realized and open PnL')
        positions = engine.state['modes'][item['market']]['positions'].values()
        item['unresolved_prior_cohorts'] = sum(
            int(position.get('signed_shares') or 0) != 0
            and position['session_date'] < plan['end_date'] for position in positions)
        item['net_return_pct'] = (item['total_equity_twd'] / item['initial_capital_twd'] - 1) * 100
        item['entry_fill_count'] = sum(fill['market'] == item['market'] and fill['purpose'] == 'close_auction_entry'
                                       for fill in fills)
        item['exit_fill_count'] = sum(fill['market'] == item['market'] and fill['purpose'] != 'close_auction_entry'
                                      for fill in fills)
    result['accounting_validation'] = {
        'official_fill_prices_verified': True, 'whole_lots_verified': True,
        'no_exchange_timestamps_fabricated': True, 'realized_pnl_and_ending_equity_reconciled': True,
        'fills': len(fills),
    }
    _plot_history(args.output, result, marks)
    result['outputs'] = {name: _sha256(args.output / name) for name in (
        'equity_history.parquet', 'equity_history.csv', 'equity_curve.png',
    )}
    result['equity_event_rows'] = marks.height
    result['decision_price_source_counts'] = {
        key: sum(json.loads((args.output / 'inputs' / day / 'source.json').read_text())['counts'][key]
                 for day in plan['sessions'])
        for key in ('observed_1325', 'same_session_close', 'missing')
    }
    lines = [
        '# 隔日沖歷史計算', '',
        f'區間：{plan["start_date"]} 至 {plan["end_date"]}，共 {len(plan["sessions"])} 個完成交易日。', '',
        '使用現有看板四個策略的 fold 11 checkpoint：訓練至 2024 年、驗證 2025 年。'
        '它們是當沖模型的 13:25 隔日沖適配器；新建的隔日沖訓練配置尚未提供正式訓練完成的模型。', '',
        '每次使用前一完成交易日的日線特徵與當日 13:25 價格。缺 13:25 時依授權採同日收盤替代，'
        '這部分含前視資訊。整張數量以決策價格及進場前帳戶權益計算，費稅與持倉沿用共同隔日沖模擬帳本。', '',
        '進場以官方日線 CLOSE、退出以下一有效交易日 OPEN 作歷史撮合價格近似；'
        '沒有捏造交易所成交時間，也不主張排隊順位或實際全量成交。缺少退出價格的持倉保留並揭露。', '',
        f'最新 {plan["end_date"]} 收盤持倉依該收盤價估值，包含預估平倉費稅；不使用下一日尚未發生的開盤價。', '',
        '| 策略 | 初始資金 | 期末權益 | 淨報酬 | 期末持倉檔數 | 較早未解決持倉 |',
        '|---|---:|---:|---:|---:|---:|',
    ]
    lines.extend(
        f'| {item["market"]} | {item["initial_capital_twd"]:,.0f} | {item["total_equity_twd"]:,.2f} | '
        f'{item["net_return_pct"]:.4f}% | {item["open_position_count"]} | {item["unresolved_prior_cohorts"]} |'
        for item in result['markets']
    )
    lines += ['', '![權益曲線](equity_curve.png)', '',
              '[逐次開盤與收盤估值 CSV](equity_history.csv) · [結果與核對證據](result.json) · [模型與資料版本](plan.json)', '',
              '曲線為開盤／收盤事件估值，不是補造的每分鐘價格。完整訊號、來源收據與獨立帳本保存於本目錄。']
    (args.output / 'report.md').write_text('\n'.join(lines) + '\n')
    result['outputs']['report.md'] = _sha256(args.output / 'report.md')
    atomic_write_json(args.output / 'result.json', result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


def audit_unresolved_exits(output: Path) -> None:
    """Separate arithmetic completion from source, exit and funding acceptance."""
    result_path = output / 'result.json'
    result = json.loads(result_path.read_text())
    ledger = ((output / result['ledger_relative_path']).resolve()
              if result.get('ledger_relative_path') else Path(result['ledger']).resolve())
    if not ledger.is_relative_to(output.resolve()):
        raise ValueError('Historical ledger escapes the result directory')
    state = json.loads((ledger / 'state.json').read_text())
    unresolved = []
    for item in result['markets']:
        mode = state['modes'][item['market']]
        for position in mode['positions'].values():
            if not position.get('signed_shares') or position['session_date'] >= result['end_date']:
                continue
            unresolved.append({
                'market': item['market'],
                **{key: position.get(key) for key in (
                    'symbol', 'name', 'session_date', 'signed_shares', 'entry_price',
                    'opening_exit_order_status', 'last_mark_at', 'last_mark_price', 'valuation_stale',
                )},
            })
        item['valuation_status'] = (
            'stale_open_position_value' if item['valuation_stale']
            else 'unresolved_prior_open_exit' if item['unresolved_prior_cohorts']
            else 'latest_close_mark_pending_next_open' if item['open_position_count']
            else 'flat'
        )
    if unresolved:
        symbols = {row['symbol'] for row in unresolved}
        observed = (
            pl.scan_parquet(list((output / 'inputs').glob('*/prices.parquet')))
            .filter(pl.col('symbol').is_in(sorted(symbols)))
            .select('date', 'symbol', 'open', 'close')
            .filter(pl.col('close').is_finite() & (pl.col('close') > 0))
            .sort('date').collect()
        )
        for row in unresolved:
            prices = observed.filter(pl.col('symbol') == row['symbol'])
            row['last_official_price_session'] = prices['date'][-1] if prices.height else None
            row['last_official_close'] = prices['close'][-1] if prices.height else None
    result['unresolved_positions'] = unresolved
    marks = pl.read_parquet(output / 'equity_history.parquet')
    fills = [json.loads(line) for line in (ledger / 'fills.jsonl').read_text().splitlines()]
    entries = {row['position_id']: row for row in fills if row['purpose'] == 'close_auction_entry'}
    entry_orders = {}
    rejected_opens = []
    with (ledger / 'orders.jsonl').open() as handle:
        for line in handle:
            row = json.loads(line)
            if row.get('purpose') == 'close_auction_entry' and row.get('status') == 'working':
                entry_orders[row['order_id']] = row
            if row.get('status') == 'blocked_open_outside_legal_limits':
                rejected_opens.append({key: row.get(key) for key in (
                    'market', 'session_date', 'symbol', 'position_id', 'status',
                )})
    plan = json.loads((output / 'plan.json').read_text())
    date_index = {day: index for index, day in enumerate(plan['sessions'])}
    trades = []
    for fill in fills:
        if fill['purpose'] == 'close_auction_entry':
            continue
        entry = entries[fill['position_id']]
        order = entry_orders[entry['order_id']]
        direction = 1 if order['side'] == 'buy' else -1
        gross = direction * fill['quantity'] * (fill['price'] - entry['price'])
        net = gross - fill['entry_fee_allocated_twd'] - fill['fee_and_tax_twd']
        if not (np.isclose(gross, fill['gross_pnl_twd'], rtol=0, atol=0.01)
                and np.isclose(net, fill['net_pnl_twd'], rtol=0, atol=0.01)):
            raise ValueError(f'Trade-level price/direction/fee reconciliation failed: {fill["position_id"]}')
        trades.append({
            'market': fill['market'], 'symbol': fill['symbol'],
            'side': 'long' if direction > 0 else 'short',
            'entry_date': entry['session_date'], 'exit_date': fill['session_date'],
            'sessions_held': date_index[fill['session_date']] - date_index[entry['session_date']],
            'shares': fill['quantity'], 'entry_price': entry['price'], 'exit_price': fill['price'],
            'gross_pnl_twd': gross, 'net_pnl_twd': net,
        })
    pl.DataFrame(trades).write_csv(output / 'trade_reconciliation.csv')
    for item in result['markets']:
        market = item['market']
        rows = marks.filter(pl.col('market') == market).sort('minute')
        nonpositive = rows.filter(pl.col('total_equity_twd') <= 0)
        first_at = nonpositive['minute'][0] if nonpositive.height else None
        item['funding_audit'] = {
            'sizing_capital_basis': 'current_pre_entry_total_equity',
            'nonpositive_equity_blocks_new_exposure': True,
            'first_nonpositive_equity_at': first_at,
            'entry_fills_after_first_nonpositive_equity': sum(
                row['market'] == market and first_at is not None
                and datetime.fromisoformat(row['fill_at']) > datetime.fromisoformat(first_at)
                for row in entries.values()),
            'minimum_equity_twd': rows['total_equity_twd'].min(),
        }
        item['delayed_exit_count'] = sum(row['market'] == market and row['sessions_held'] > 1 for row in trades)
    # A dated file and its checksum prove provenance, not correctness of its
    # reconstructed product rules. Surface contradictory observed prices. The
    # selected official OPEN/CLOSE remains the counterfactual price authority,
    # so an ETF misclassified by the auxiliary band builder cannot defer an
    # otherwise observed auction event.
    prices = pl.scan_parquet(list((output / 'inputs').glob('*/prices.parquet')))
    conflicts = prices.filter(
        ((pl.col('open') > 0) & ((pl.col('open') < pl.col('lower_limit_price') - 1e-8)
                                | (pl.col('open') > pl.col('upper_limit_price') + 1e-8)))
        | ((pl.col('close') > 0) & ((pl.col('close') < pl.col('lower_limit_price') - 1e-8)
                                   | (pl.col('close') > pl.col('upper_limit_price') + 1e-8)))
    ).select('date', 'symbol', 'open', 'close', 'lower_limit_price', 'upper_limit_price').sort('date', 'symbol').collect()
    conflicts.write_csv(output / 'price_limit_conflicts.csv')
    result['funding_contract'] = 'current_pre_entry_equity_whole_lot_no_new_exposure_after_nonpositive_nav'
    result['price_limit_audit'] = {
        'official_prices_outside_reconstructed_bounds': conflicts.height,
        'rejected_open_events': rejected_opens,
        'product_rule_review_required': bool(conflicts.height),
        'reconstructed_bounds_used_as_fill_authority': False,
        'regulatory_reference': 'https://www.twse.com.tw/downloads/zh/ETF/qanda01.pdf',
        'limitation': 'TWSE reconstruction applies ordinary-stock limits/ticks to some ETFs; conflicts remain audit evidence and do not override official OPEN/CLOSE.',
    }
    result['accounting_validation']['trade_direction_prices_and_fees_independently_reconciled'] = True
    result['accounting_validation']['closed_trades'] = len(trades)
    result['accounting_validation']['delayed_exits'] = sum(row['sessions_held'] > 1 for row in trades)
    result['computation_complete'] = True
    result['dashboard_history_ready'] = True
    result['fit_for_funded_performance_comparison'] = not unresolved and all(
        not item['funding_audit']['entry_fills_after_first_nonpositive_equity']
        for item in result['markets']
    )
    result['status'] = (
        'computed_counterfactual_history_ready_with_stale_unresolved_position'
        if unresolved
        else 'computed_counterfactual_history_ready'
    )
    result['ledger_state_sha256'] = _sha256(ledger / 'state.json')
    result['ledger_relative_path'] = str(ledger.relative_to(output.resolve()))
    marker = '<!-- unresolved-exits-audit -->'
    report_path = output / 'report.md'
    report = report_path.read_text().split(marker)[0].rstrip() + '\n'
    notice = ('> 本表是歷史反事實試算：官方日 CLOSE/OPEN 是價格近似，沒有交易所時間戳或排隊成交證據。'
              '新隔日沖模型未完成正式訓練。\n\n')
    if notice not in report:
        report = report.replace('# 隔日沖歷史計算\n\n', '# 隔日沖歷史計算\n\n' + notice)
    report += '\n' + marker + '\n\n## 驗收範圍與資金契約\n\n'
    report += ('每一批新進場張數以當時已對帳總權益計算；權益非正時停止增加曝險。'
               '這仍是紙上反事實帳戶，不主張券商購買力、借券成交、集合競價排隊或 T+2 實際交割已獲證明。\n\n')
    for item in result['markets']:
        audit = item['funding_audit']
        if audit['first_nonpositive_equity_at']:
            report += (f'- {item["market"]}：{audit["first_nonpositive_equity_at"]} 首次非正權益；'
                       f'之後進場成交 {audit["entry_fills_after_first_nonpositive_equity"]} 筆。\n')
    report += ('\n## 價格範圍資料衝突\n\n'
               f'共 {conflicts.height} 個 symbol-session 的官方 OPEN/CLOSE 超出歷史重建範圍；'
               f'帳本記錄 {len(rejected_opens)} 次因開盤價超界而拒絕退出。'
               '原重建器對 TWSE 標的通用一般股票的 ±10% 與股票跳動單位，不能直接視為 ETF 的合法範圍。'
               '例如 00685L 的 2026-07-31 官方開盤 10.37 高於重建上限 10.05，導致延遲退出。'
               '[證交所 ETF 規則](https://www.twse.com.tw/downloads/zh/ETF/qanda01.pdf) 區分國內槓桿與海外標的；'
               '本次保留衝突作資料品質稽核；官方 OPEN/CLOSE 為歷史價格權威，輔助重建範圍不再延遲反事實退出。\n\n'
               '[價格衝突明細](price_limit_conflicts.csv) · [逐筆成交損益核對](trade_reconciliation.csv)\n')
    if unresolved:
        report += '\n## 未解決的退出與估值限制\n\n'
        report += '以下較早持倉未能在後續開盤退出；它們會阻擋該策略的新一批進場。來源缺價時，期末權益包含明示的舊估值，不能視為當日可變現金額。\n\n'
        for row in unresolved:
            report += (f'- {row["market"]}：{row["symbol"]}，{row["session_date"]} 進場，'
                       f'尚餘 {row["signed_shares"]:,} 股；最後有官方收盤價的日期 '
                       f'{row["last_official_price_session"]}。狀態：{row["opening_exit_order_status"]}。\n')
    report_path.write_text(report)
    _plot_history(output, result, marks)
    result['outputs']['report.md'] = _sha256(report_path)
    result['outputs'].update({name: _sha256(output / name) for name in (
        'trade_reconciliation.csv', 'price_limit_conflicts.csv', 'equity_curve.png',
    )})
    atomic_write_json(result_path, result)
    print(json.dumps({'status': result['status'], 'unresolved_positions': unresolved}, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', default='2026-02-25')
    parser.add_argument('--end-date', default='latest')
    parser.add_argument('--markets-dir', type=Path, default=Path('services/discord_bot/markets'))
    parser.add_argument('--public-root', type=Path, default=Path('/srv/stockagent-live/data_tw_public'))
    parser.add_argument('--minute-root', type=Path, default=Path('data_tw_minute/research_dataset'))
    parser.add_argument('--price-limit-dir', type=Path, default=Path('artifacts/live/tw_price_limits'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--market', action='append')
    parser.add_argument('--stage', choices=('plan', 'inputs', 'signals', 'replay', 'report', 'all'), default='all')
    args = parser.parse_args()
    plan = prepare(args)
    if args.stage in ('signals', 'all'):
        infer_signals(args, plan)
    if args.stage in ('replay', 'all'):
        replay(args, plan)
    if args.stage in ('report', 'replay', 'all'):
        audit_unresolved_exits(args.output)


if __name__ == '__main__':
    main()
