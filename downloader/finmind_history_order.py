"""One explicit history sequence for dispatch, ETA and the read-only dashboard.

Ranks are secondary to due refreshes and existing finite operator repairs.
They never change a source's query shape, acquisition priority or receipts.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import sqlite3


CONTRACT_VERSION = 1


@dataclass(frozen=True)
class HistoryStage:
    key: str
    dataset: str
    label: str
    family: str


HISTORY_STAGES = (
    HistoryStage('tw_stock_minute', 'TaiwanStockKBar', '台股分鐘 K', 'detail'),
    HistoryStage('tw_futures_minute', 'TaiwanFuturesKBar', '期貨分鐘 K', 'detail'),
    HistoryStage('tw_stock_brokers', 'TaiwanStockTradingDailyReport', '台股分點明細', 'detail'),
    HistoryStage('tw_warrant_brokers', 'TaiwanStockWarrantTradingDailyReport', '權證分點明細', 'detail'),
    HistoryStage('tw_stock_tick', 'TaiwanStockPriceTick', '台股 tick', 'tick'),
    HistoryStage('tw_futures_tick', 'TaiwanFuturesTick', '期貨 tick', 'tick'),
    HistoryStage('tw_option_tick', 'TaiwanOptionTick', '選擇權 tick', 'tick'),
    HistoryStage('tw_futures_spread_tick', 'TaiwanFuturesSpreadTick', '期貨價差 tick', 'tick'),
    HistoryStage('us_stock_minute', 'USStockPriceMinute', '美股分鐘 K', 'detail'),
)
BY_DATASET = {stage.dataset: stage for stage in HISTORY_STAGES}
# Mandatory local children belong to their parent's phase, not to an earlier
# network stage. This is grouping only; it never creates an additional call.
LOCAL_DERIVED_STAGES = {
    'TaiwanStockTradingDailyReportSecIdAgg': BY_DATASET['TaiwanStockTradingDailyReport'],
}
STAGES = (('priority', '到期追新／指定有限優先回補'),
          ('core', '主要歷史／日資料／財報／總經／新聞'),
          *((stage.key, stage.label) for stage in HISTORY_STAGES),
          ('validation', '剩餘跨來源校驗'))


def metadata() -> dict:
    return {'contract_version': CONTRACT_VERSION, 'mode': 'ordered_history_stages',
            'preemptions': ['due_incremental', 'finite_operator_repair'],
            'stages': [{'key': stage.key, 'dataset': stage.dataset, 'label': stage.label, 'rank': rank}
                       for rank, stage in enumerate(HISTORY_STAGES, 1)]}


def first_unfinished_dataset(connection: sqlite3.Connection, now: datetime, *,
                             delegated=frozenset(), datasets=None) -> str | None:
    """Bounded seeks, not an ID x day expansion or a full frontier count.

    A drained 128-item working set is NOT a finished history. Keep later stages
    behind unseeded cursors and cooling failures, until the worker refills the
    same frontier. This read-only guard also applies to status previews.
    """
    from downloader.finmind_supplemental import SOURCES, _eligible_anchor
    from downloader.finmind_storage_objects import effective_sources, identity_clause, forward_identity_clause
    sources = effective_sources(connection, SOURCES)

    has_frontiers = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE name='finmind_source_frontiers'").fetchone()
    for stage in HISTORY_STAGES:
        dataset = stage.dataset
        if dataset in delegated or (datasets is not None and dataset not in datasets):
            continue
        if connection.execute("SELECT 1 FROM tasks WHERE dataset=? "
                              "AND state IN ('pending','failed','inflight') LIMIT 1", (dataset,)).fetchone():
            return dataset
        if has_frontiers:
            identity = identity_clause(dataset) if sources[dataset].universe == 'market' else ''
            # An old cursor may overlap accepted tasks or verified closures;
            # seeding advances it without calls. Do not silently skip it.
            if connection.execute('SELECT 1 FROM finmind_source_frontiers WHERE dataset=?'
                                  + identity + ' AND older_than IS NOT NULL LIMIT 1', (dataset,)).fetchone():
                return dataset
            anchor = _eligible_anchor(sources[dataset], now).isoformat()
            forward = forward_identity_clause(dataset) if sources[dataset].endpoint == 'storage_objects' else identity
            if connection.execute('SELECT 1 FROM finmind_source_frontiers WHERE dataset=?'
                                  + forward + ' AND newer_than<? LIMIT 1', (dataset, anchor)).fetchone():
                return dataset
    return None
