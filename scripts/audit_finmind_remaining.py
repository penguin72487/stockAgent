"""Source-backed FinMind inventory; no provider calls or queue mutations.

One row per provider dataset, resolved to its existing primary owner. History
candidates, physical queue work and source observations are different measures.
The public projection is metadata-only; no historical Parquet tree is scanned.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import io
from pathlib import Path
from typing import Any

from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.finmind_eta_work import build_finmind_workload
from downloader.finmind_supplemental import SOURCES
from downloader.finmind_history_order import BY_DATASET, HISTORY_STAGES
from scripts.audit_finmind_query_ranges import registry
from stockagent.live.finmind_dashboard import build_finmind_public_status


def inventory(repo: Path, now: datetime) -> dict[str, Any]:
    public = build_finmind_public_status(repo, now=now)
    workload = build_finmind_workload(repo / 'data_finmind', now)
    plans = {row['dataset']: row for row in workload['datasets']}
    result = []
    for dataset, contract in sorted(registry().items()):
        matches = [row for row in public['datasets'] if row['id'].split(':', 1)[0] == dataset]
        # Owner aliases remain in the public catalog, not extra downloads.
        expected_id = dataset + ':all_market' if contract['primary_owner'] == 'sponsor' else dataset
        row = next((item for item in matches if item['id'] == expected_id and item['state'] != 'delegated'),
                   next((item for item in matches if item['state'] != 'delegated'), {}))
        plan = plans.get(dataset, {})
        source = SOURCES.get(dataset)
        frontier = plan.get('historical_frontier', {})
        history_stage = BY_DATASET.get(dataset)
        stage = history_stage.key if history_stage else 'core'
        release = row.get('update_monitor', {}).get('release', {})
        result.append({
            'dataset': dataset, 'owner': contract['primary_owner'], 'stage': stage,
            'history_rank': HISTORY_STAGES.index(history_stage) + 1 if history_stage else None,
            'state': row.get('state', plan.get('state', 'unknown')),
            'query_shape': contract['query_shape'], 'configured_first_date': contract['configured_first_date'],
            'first_observed_date': row.get('first_data_date'), 'last_observed_date': row.get('last_data_date'),
            'rows': row.get('rows'), 'local_bytes': row.get('local_bytes'),
            'materialized_tasks': row.get('materialized_partitions', row.get('target_partitions')),
            'nonempty_partitions': row.get('complete_partitions'),
            'observed_empty_partitions': row.get('observed_empty_partitions'),
            'pending_queued_tasks': plan.get('pending_tasks'), 'inflight_tasks': plan.get('inflight_tasks'),
            'blocked_tasks': plan.get('blocked_tasks'), 'retry_tasks': plan.get('retry_tasks'),
            'unseeded_candidates': plan.get('candidate_requests'),
            'raw_calendar_candidates': frontier.get('raw_unseeded_calendar_candidates'),
            'excluded_calendar_candidates': frontier.get('excluded_calendar_candidates'),
            'already_materialized_candidates': frontier.get('already_materialized_candidates'),
            'planned_requests': plan.get('current_plan_requests'),
            'known_identifiers': frontier.get('known_identifiers'),
            'release_hour_taipei': source.release_hour if source else release.get('hour'),
            'release_minute_taipei': source.release_minute if source else release.get('minute'),
            'historical_universe_verified_complete': plan.get('historical_universe_verified_complete'),
            'all_history_complete_claim': False,
        })
    return {
        'schema_version': 2, 'observed_at_utc': now.isoformat(),
        'provider_calls': 0, 'queue_writes': 0, 'parquet_history_scans': 0,
        'datasets': result, 'catalog_rows': len(public['datasets']),
        'unique_datasets': len(result), 'acquisition': public['acquisition'],
        'workload_summary': workload['summary'],
        'quota': {key: public['quota'].get(key) for key in (
            'official_requests_per_hour', 'observed_requests_60m', 'observed_at_utc')},
        'caveats': [
            'A checked empty response is not a nonempty observation or proof of complete source history.',
            'Candidates are an enumerated search plan, not missing record counts or verified instrument lifetimes.',
            'Queue-current core data may still have source-quality, historical-universe or point-in-time gaps.',
            'Global stage estimates share one account; per-dataset full-quota projections must not be added as independent ETAs.',
        ],
    }


def write_report(output: Path, report: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output / 'inventory.json', report)
    stream = io.StringIO()
    rows = report['datasets']
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]) if rows else ['dataset'])
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(output / 'inventory.csv', stream.getvalue())
    lines = ['# FinMind 尚未下載與歷史搜尋清冊', '', f"取樣：{report['observed_at_utc']}", '',
             f"{report['unique_datasets']} 個獨立資料集，{report['catalog_rows']} 個含 owner 別名的面板項目。",
             '只讀佇列與既有 metadata；沒有額外呼叫 FinMind，沒有改佇列或掃歷史 Parquet。', '',
             '已知佇列無待辦不代表完整商品歷史、來源品質或 PIT 均已核實。', '',
             '## 未完成的可執行／候選工作', '',
             '| 資料集 | 最早設定搜尋日 | 已取得首日 | 已取得末日 | 已抓筆數 | 已建待辦 | 未建候選 |',
             '| --- | --- | --- | --- | ---: | ---: | ---: |']
    for row in rows:
        if any(row.get(key) for key in ('planned_requests', 'blocked_tasks', 'inflight_tasks')):
            lines.append('| ' + ' | '.join(str(row.get(key)) if row.get(key) is not None else '未知' for key in (
                'dataset', 'configured_first_date', 'first_observed_date', 'last_observed_date',
                'rows', 'pending_queued_tasks', 'unseeded_candidates')) + ' |')
    lines += ['', '## 分階段條件估時', '',
              '| 階段 | 排程請求／候選 | 最快完成（UTC） | 中間完成（UTC） | 保守完成（UTC） |',
              '| --- | ---: | --- | --- | --- |']
    for stage in report['acquisition']['completion_estimate'].get('stages', []):
        scenarios = stage.get('scenarios', {})
        lines.append('| ' + ' | '.join([
            str(stage.get('scope_label', stage.get('key'))),
            str(stage.get('workload', {}).get('planned_requests')),
            *(str(scenarios.get(name, {}).get('estimated_complete_at_utc') or '未知')
              for name in ('fastest', 'central', 'slowest'))]) + ' |')
    lines += ['', '長期估時包含原排程的先後順序與未來追新模型，不是完工保證；',
              '未知歷史代號、無可驗證的存續期間及來源本身缺失仍需另行查證。', '',
              '所有資料集（包括當前無待辦與未知範圍）見同目錄 inventory.csv / inventory.json。', '']
    atomic_write_text(output / 'inventory.md', '\n'.join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = inventory(args.repo, datetime.now(UTC))
    write_report(args.output, report)
    print({'output': str(args.output), 'asof': report['observed_at_utc'],
           'datasets': report['unique_datasets'], 'workload': report['workload_summary']['current_plan_requests']})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
