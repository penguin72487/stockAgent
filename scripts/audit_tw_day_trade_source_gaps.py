"""Read-only, symbol/session-grained inventory for the physical carry candidate.

This does not authorize training, change eligibility, or certify every minute.
It distinguishes missing source observations, locally retained evidence, and
unresolved accounting terms. Reports never turn a partial source into READY.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
import numpy as np

from downloader.download_tw_corporate_action_entitlements import _write_json_atomic
from downloader.download_tw_public_data import _validated_taiex_session_dates
from scripts.rebuild_tw_day_trade_minute_curves import (
    DEFAULT_LOCAL_MINUTE_CACHE_ROOTS, DEFAULT_LOCAL_MINUTE_ROOTS,
    MinutePriceStore, _atomic_text, _sha256,
)


def exact_gap_keys(symbol: str, days: list[str], *, start: date, end: date) -> list[tuple[str, str]]:
    if not re.fullmatch(r"[0-9A-Z]{4,6}", symbol):
        raise ValueError("invalid gap symbol")
    if len(set(days)) != len(days):
        raise ValueError("duplicate source gap date")
    result = []
    for text in days:
        day = date.fromisoformat(text)
        if str(day) != text:
            raise ValueError("source gap requires canonical ISO date")
        if start <= day <= end:
            result.append((symbol, text))
    return sorted(result)


def verify_raw_manifest(root: Path, receipt: dict) -> int:
    root = root.resolve(strict=True)
    path = (root / receipt["relative_path"]).resolve(strict=True)
    if not path.is_relative_to(root) or _sha256(path) != receipt["sha256"] or path.stat().st_size != receipt["size"]:
        raise ValueError("raw manifest receipt mismatch")
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if len(rows) != receipt["entries"]:
        raise ValueError("raw manifest entry count mismatch")
    for row in rows:
        source = (root / row["path"]).resolve(strict=True)
        request = json.dumps(row["request"], ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        if (not source.is_relative_to(root) or source.stat().st_size != row["response_size"]
                or _sha256(source) != row["response_sha256"]
                or hashlib.sha256(request).hexdigest() != row["request_sha256"]):
            raise ValueError(f"raw request/response proof mismatch: {row['path']}")
    return len(rows)


def assessment_domains(actions: list[dict]) -> dict:
    """Catalogue completeness is not a position-dependent accounting gate.

    In particular, `avoid` classifies issuer terms; it neither proves an
    attempted download failed nor authorizes skipping an owned entitlement.
    The physical executor checks the actual chronological exposure separately.
    """
    return {
        'source_inventory': {'status': 'inventory_only',
            'unresolved_entitlement_catalogue_rows': sum(r['len'] for r in actions if r['handling'] != 'exact_cash'),
            'catalogue_rows_are_global_training_blockers': False},
        'trajectory_accounting': {'status': 'not_assessed',
            'requires_actual_physical_inventory_intersection': True,
            'unheld_action_terms_required': False,
            'recognized_exact_cash_claim_requires_stock_quote': False,
            'missing_held_price_or_action_may_be_masked_away': False},
        'annual_training_integration': {'status': 'blocked',
            'reason': 'accepted source adapter, annual state/report plumbing not integrated'},
    }


def compare_paper_minute_marks(root: Path, limits_path: Path, day: date, symbols: list[str]) -> dict:
    """Read-only real-source comparison; not model inference or fill acceptance."""
    from scripts.rebuild_tw_day_trade_open_price_replay import _minute_bar_rows
    from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
    from stockagent.data.tw_security import classify_tw_stock_or_etf

    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError('minute comparison requires unique explicit symbols')
    manifest_path = root / 'manifest.json'
    partition = root / f'trade_date={day}/data.parquet'
    hashes = {str(p.resolve()): _sha256(p) for p in (manifest_path, partition, limits_path)}
    manifest = json.loads(manifest_path.read_text())
    proofs = [r for r in manifest['partitions'] if r['trade_date'] == str(day)]
    if (len(proofs) != 1 or proofs[0]['status'] != 'ok'
            or proofs[0]['output'] != str(partition.relative_to(root))
            or proofs[0]['output_sha256'] != hashes[str(partition.resolve())]):
        raise ValueError('minute comparison partition receipt mismatch')
    frame = pl.read_parquet(partition)
    if frame.height != proofs[0]['rows']:
        raise ValueError('minute comparison partition row count mismatch')
    frame = frame.filter(pl.col('symbol').is_in(symbols))
    if frame.select('symbol', 'ts').unique().height != frame.height:
        raise ValueError('minute comparison duplicate source key')
    limits = pl.read_parquet(limits_path).filter(pl.col('symbol').is_in(symbols))
    if (limits.height != len(symbols) or limits['symbol'].n_unique() != len(symbols)
            or set(limits['trading_date']) != {str(day)}):
        raise ValueError('minute comparison requires exact dated limits for every symbol')
    by_symbol = {r['symbol']: r for r in limits.to_dicts()}
    dense = np.full((len(symbols), 270, 6), np.nan)
    slots = {s: i for i, s in enumerate(symbols)}
    for row in frame.iter_rows(named=True):
        stamp = row['ts']
        offset = stamp.hour * 60 + stamp.minute - 541
        if stamp.date() != day or stamp.second or stamp.microsecond or not 0 <= offset < 270:
            raise ValueError('minute comparison has wrong date/clock')
        volume, amount = row['volume_shares'], row['Amount']
        vwap = (amount / volume if amount is not None and np.isfinite(amount)
                and amount > 0 and volume is not None and volume > 0 else row['Close'])
        dense[slots[row['symbol']], offset] = [row[k] for k in ('Open', 'High', 'Low', 'Close')] + [vwap, volume]
    opportunities = paper_minute_opportunities(dense, trading_date=np.datetime64(day),
        lower_limit=np.array([by_symbol[s]['lower_limit_price'] for s in symbols]),
        upper_limit=np.array([by_symbol[s]['upper_limit_price'] for s in symbols]),
        security_types=np.array([classify_tw_stock_or_etf(s) for s in symbols]))
    # Independent existing paper loader, including its zero-volume filter.
    paper, coverage = _minute_bar_rows((root,), trading_date=day, symbols=set(symbols))
    expected = np.full_like(opportunities.marks, np.nan)
    expected_sources = np.full_like(opportunities.mark_source_index, -1)
    for symbol, i in slots.items():
        price, source = np.nan, -1
        for minute in range(270):
            hour, clock_minute = divmod(541 + minute, 60)
            key = f'{day}T{hour:02d}:{clock_minute:02d}+08:00'
            if key in paper.get(symbol, {}):
                price, source = paper[symbol][key]['close'], minute
            expected[i, minute], expected_sources[i, minute] = price, source
    if (not np.array_equal(opportunities.marks, expected, equal_nan=True)
            or not np.array_equal(opportunities.mark_source_index, expected_sources)):
        raise ValueError('candidate minute valuation differs from canonical paper sources')
    for path, digest in hashes.items():
        if _sha256(Path(path)) != digest:
            raise RuntimeError(f'minute comparison source changed: {path}')
    return {'status': 'passed', 'scope': 'source_marks_only_not_model_or_fill_acceptance',
        'date': str(day), 'symbols': symbols, 'source_rows': frame.height,
        'comparison_cells': int(expected.size),
        'unavailable_cells': int(np.isnan(expected).sum()),
        'carried_cells': int(((expected_sources >= 0) & (expected_sources < np.arange(270))).sum()),
        'coverage': coverage, 'source_sha256': hashes,
        'source_files_unchanged': True, 'valuation_is_execution_price': False}


def audit(args: argparse.Namespace) -> dict:
    start, end, focus = map(date.fromisoformat, (args.start_date, args.end_date, args.focus_start))
    if not start <= focus <= end:
        raise ValueError("audit requires start <= focus <= end")
    inputs, signatures = {}, {}

    def observe(path):
        path = Path(path).resolve(strict=True)
        signature = lambda: (path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_ctime_ns)
        before = signature()
        inputs[str(path)] = {"sha256": _sha256(path), "size": before[0]}
        if before != signature():
            raise RuntimeError(f"source changed during hashing: {path}")
        signatures[path] = before
        return path

    summary = json.loads(observe(args.download_root / "download_summary.json").read_text())
    with observe(args.download_root / "download_report.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != summary["selected_symbols"] or len({r['symbol'] for r in rows}) != len(rows):
        raise ValueError("download report universe/count mismatch")
    observed = Counter(r['status'] for r in rows)
    expected = {'complete': summary['complete_symbols'],
        'complete_with_source_gaps': summary['complete_with_source_gap_symbols'],
        'contract_unavailable': summary['contract_unavailable_symbols'],
        'partial': summary['partial_symbols'], 'failed': summary['failed_symbols']}
    if any(n != observed[k] for k, n in expected.items()) or set(observed) - set(expected):
        raise ValueError('download report status counts differ from summary')
    if observed['partial'] or observed['failed']:
        raise ValueError('in-flight or failed downloads require collector repair before sealed-gap audit')
    if date.fromisoformat(summary['start_date']) > start or date.fromisoformat(summary['end_date']) < end:
        raise ValueError('requested audit exceeds declared collection horizon')
    sessions, calendar_sha = _validated_taiex_session_dates(args.public_root, start, end)
    calendar = {str(d) for d in sessions}
    declared, unavailable, unavailable_summary = [], [], []
    for row in rows:
        symbol = row["symbol"]
        exact_gap_keys(symbol, [], start=start, end=end)
        if row['status'] == 'complete_with_source_gaps':
            payload = json.loads(observe(args.download_root / 'symbols' / f'{symbol}.manifest.json').read_text())
            if payload['symbol'] != symbol or len(payload['source_gap_dates']) != payload['source_gap_sessions']:
                raise ValueError("symbol gap manifest identity/count mismatch")
            declared.extend(exact_gap_keys(symbol, payload['source_gap_dates'], start=start, end=end))
        elif row['status'] == 'contract_unavailable':
            path = observe(args.public_root / 'stocks' / f'{symbol}_features.parquet')
            frame = pl.read_parquet(path, columns=['date', 'Trading_Volume', 'data_source']).filter(
                (pl.col('date') >= start) & (pl.col('date') <= end) & (pl.col('Trading_Volume') > 0))
            if frame['date'].n_unique() != frame.height:
                raise ValueError(f"duplicate daily source key: {symbol}")
            pairs = [(symbol, str(d)) for d in frame['date'] if str(d) in calendar]
            unavailable.extend(pairs)
            unavailable_summary.append({'symbol': symbol, 'observed_positive_volume_sessions': len(pairs),
                'focus_sessions': sum(d >= str(focus) for _, d in pairs)})
    non_session_gaps = [(s, d) for s, d in declared if d not in calendar]
    declared = [(s, d) for s, d in declared if d in calendar]
    print(f'[gap-audit] declared={len(declared)} contract_unavailable_observations={len(unavailable)}', flush=True)
    required = defaultdict(set)
    for symbol, day in declared + unavailable:
        required[symbol].add(day)
    local_roots = (args.download_root, *DEFAULT_LOCAL_MINUTE_ROOTS)
    store = MinutePriceStore(local_roots, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS, require_receipts=True)
    store.prepare(required)
    records = []
    for kind, pairs in [('declared_provider_gap', declared), ('contract_unavailable', unavailable)]:
        for symbol, day in sorted(pairs):
            prices = store.prices(symbol, day)
            records.append({'symbol': symbol, 'date': day, 'reason': kind,
                'local_receipted_marks': len(prices),
                'local_entry_mark_present': any('T09:01' in t for t in prices),
                'status': 'local_evidence_requires_ingestion' if prices else 'unresolved',
                'historical_market_ineligibility_claimed': False})
    store.assert_sources_unchanged()
    print(f'[gap-audit] local_evidence_pairs={sum(r["local_receipted_marks"] > 0 for r in records)}', flush=True)
    entitlement_path = observe(args.public_root / 'tw_corporate_action_entitlements.parquet')
    entitlement_summary = json.loads(observe(entitlement_path.with_suffix('.summary.json')).read_text())
    proof = entitlement_summary['output_receipt']
    if (inputs[str(entitlement_path)]['sha256'] != proof['sha256']
            or inputs[str(entitlement_path)]['size'] != proof['size']):
        raise ValueError('entitlement output receipt mismatch')
    ent = pl.read_parquet(entitlement_path).filter((pl.col('date') >= start) & (pl.col('date') <= end))
    if ent.select('symbol', 'date').unique().height != ent.height:
        raise ValueError('duplicate entitlement symbol-day')
    actions = ent.group_by('handling', 'handling_reason').len().sort('handling', 'handling_reason').to_dicts()
    action_root = args.public_root / 'execution_actions'
    attempt = json.loads(observe(action_root / 'tw_share_replacement_reference.attempt.summary.json').read_text())
    verified_raw = verify_raw_manifest(action_root, attempt['raw_receipt_manifest'])
    comparison = None
    if getattr(args, 'minute_comparison_date', None):
        if not args.minute_comparison_symbols or not args.minute_comparison_limits:
            raise ValueError('minute comparison needs explicit symbols and dated limit path')
        comparison = compare_paper_minute_marks(args.minute_comparison_root,
            args.minute_comparison_limits, date.fromisoformat(args.minute_comparison_date),
            args.minute_comparison_symbols.split(','))
    for path, expected in signatures.items():
        actual = (path.stat().st_size, path.stat().st_mtime_ns, path.stat().st_ctime_ns)
        if actual != expected:
            raise RuntimeError(f'source changed during gap audit: {path}')
    return {'schema_version': 2, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'retrospective_source_quality_not_historical_market_eligibility',
        'training_ready': False, 'automatic_mask_activation': False,
        'start': str(start), 'end': str(end), 'focus_start': str(focus),
        'calendar_sessions': len(calendar), 'calendar_sha256': calendar_sha,
        'declared_gap_symbols': len({s for s, _ in declared}), 'declared_gap_sessions': len({d for _, d in declared}),
        'declared_gap_pairs': len(declared), 'non_session_gaps': non_session_gaps,
        'contract_unavailable_symbols': unavailable_summary,
        'contract_unavailable_positive_volume_pairs': len(unavailable),
        'minute_pairs': records, 'entitlement_classification': actions,
        'assessment_domains': assessment_domains(actions),
        'paper_minute_comparison': comparison,
        'local_search_roots': [str(p.resolve()) for p in store.kbar_roots],
        'share_replacement_attempt': attempt, 'verified_action_raw_receipts': verified_raw,
        'input_receipts': inputs,
        'limitations': ['No full symbol x 270-bar coverage certification.',
            'Positive daily volume is not proof of a regular-session 09:01 fill.',
            'Retained minute marks require canonical ingestion, volume and execution validation.',
            'A minute-source mask cannot erase physical inventory; exact cash claims settle independently.',
            'Unheld action catalogue gaps are not global blockers or permission to mask held rights.',
            'Source adapter and full annual carry trainer remain pending.']}


def markdown(report: dict) -> str:
    attempt, pairs = report['share_replacement_attempt'], report['minute_pairs']
    retained = Counter(r['reason'] for r in pairs if r['local_receipted_marks'])
    lines = ['# 當沖候選訓練資料缺口實查', '', '## 1. 執行進度', '',
        f"範圍：{report['start']} ～ {report['end']}；官方交易日 {report['calendar_sessions']}。",
        '這是來源品質盤點，不是完整分鐘／會計驗收；沒有啟用正式訓練或線上遮罩。', '',
        '**沿用網頁標準：先查實際交易與持倉需要的資料，不把整份來源目錄的缺項一律當成整段訓練失敗。**',
        '`training_ready=false` 目前還包含年度 trainer 串接未完成，不能全部歸因於下載缺資料。', '',
        '| 類別 | 實查數量 |', '| --- | ---: |',
        f"| 有合約但來源缺日 | {report['declared_gap_pairs']} 股票日 / {report['declared_gap_symbols']} 檔 / {report['declared_gap_sessions']} 日期 |",
        f"| 無現行合約，但期間有日成交資料 | {report['contract_unavailable_positive_volume_pairs']} 股票日 / {len(report['contract_unavailable_symbols'])} 檔 |",
        f"| 上述無合約股票日已有本機 receipt 分鐘證據 | {retained['contract_unavailable']} |",
        f"| 換股參考資料成功解析（含已公告的未來復牌） | {attempt.get('completed_reference_rows', 0)} |",
        f"| 換股參考資料仍失敗 | {attempt['failure_count']} |",
        f"| 換股 request/response 已核對 | {report['verified_action_raw_receipts']} |", '',
        '無現行合約不等於歷史不存在；已有分鐘證據不等於已進入訓練資料。',
        '換股來源按恢復交易日查詢，包含已公告但尚未到來的復牌事件；不聲稱已觀測未來行情。', '',
        '## 2. 除權息分類（不是全部精確入帳證明）', '',
        '| 處理 | 原因 | 事件數 |', '| --- | --- | ---: |']
    lines += [f"| {r['handling']} | {r['handling_reason']} | {r['len']} |" for r in report['entitlement_classification']]
    lines += ['', '`avoid` 是條款分類，不是下載失敗次數，也不是全域阻擋數。',
        '有舊持倉跨過事件才需要該筆精確股數／現金條款；無舊持倉不領取該次權益。',
        '不能為了避開企業行動而回溯刪掉持倉、改寫訊號，或偷偷強制無限量平倉。']
    lines += ['', '## 3. 最新區間來源缺日', '', '| 股票 | 日期 | 狀態 |', '| --- | --- | --- |']
    lines += [f"| {r['symbol']} | {r['date']} | {r['status']} |" for r in pairs
              if r['reason'] == 'declared_provider_gap' and r['date'] >= report['focus_start']]
    lines += ['', '## 4. 換股未解決事件', '', '| 股票 | 恢復交易日 | 原因 |', '| --- | --- | --- |']
    lines += [f"| {r.get('symbol', '?')} | {r.get('resume_date', '?')} | {r['error'].split('issuer fallback: ')[-1]} |"
              for r in attempt['failures']]
    lines += ['', '## 5. 遮罩邊界', '',
        '只略過明列的股票日新單，不重分配權重、不刪整天、不修改價格。',
        '跨日股票庫存碰到整日分鐘來源缺口仍拒絕；不能清空庫存、歸零損益或捏造平倉。',
        '已確定金額／付款日的現金應收應付不需要股票行情，保留原帳並按日結算；未解或修訂條款仍须查核。',
        '企業行動避開期須另有公告及可成交平倉證據，不能僅遮住恢復交易日。', '',
        '## 6. 限制', '']
    lines += [f'- {text}' for text in report['limitations']]
    comparison = report.get('paper_minute_comparison')
    if comparison:
        lines += ['', '## 7. 保留真實分鐘來源比對', '',
            f"{comparison['date']}：{', '.join(comparison['symbols'])}；來源 {comparison['source_rows']} 根，",
            f"共比對 {comparison['comparison_cells']} 個分鐘格；缺少首次觀測 {comparison['unavailable_cells']} 格維持 NaN，",
            f"明示沿用先前成交價 {comparison['carried_cells']} 格；與既有 paper loader 完全一致。",
            '來源與限價檔雜湊前後不變；這是來源估值測試，不是實際模型績效或成交驗收。']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--download-root', type=Path, default=Path('data_tw_minute/shioaji_1m'))
    parser.add_argument('--public-root', type=Path, required=True)
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--focus-start', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--minute-comparison-date')
    parser.add_argument('--minute-comparison-symbols', help='Explicit comma-separated symbol scope')
    parser.add_argument('--minute-comparison-root', type=Path, default=Path('data_tw_minute/research_dataset'))
    parser.add_argument('--minute-comparison-limits', type=Path)
    args = parser.parse_args()
    report = audit(args)
    _write_json_atomic(args.inventory, report)
    _atomic_text(args.output, markdown(report))
    print(f'[gap-audit] report={args.output} training_ready=false', flush=True)


if __name__ == '__main__':
    main()
