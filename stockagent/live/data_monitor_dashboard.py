"""Public-safe, source-backed status for every registered data collection.

The physical registry remains ``configs/data_sync/packed_datasets.json``.  This
module projects that registry together with the existing Shioaji/OpenBB status
builders and receipt-backed dataset manifests.  It intentionally distinguishes
freshness, historical completeness, process activity, and ETA; those concepts
must not be collapsed into one optimistic health flag.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import time as clock
from typing import Any, Final, Iterable, Mapping
from zoneinfo import ZoneInfo

from stockagent.data.taifex_sessions import next_taifex_capture_window
from stockagent.live.data_monitor_inventory import PHYSICAL_FAMILIES, build_feature_inventory, build_record_inventory
from stockagent.live.openbb_archive_dashboard import build_openbb_public_status
from stockagent.live.shioaji_api_dashboard import build_shioaji_public_status
from stockagent.live.tw_public_acquisition_progress import (
    ADDED_DATASETS,
    build_tw_public_acquisition_progress,
)
from scripts.download_finlab_history import (
    AUTOMATICALLY_DEFERRED_REASONS,
    attempt_retry_at,
)
from downloader.download_finmind_complement import (
    ALL_DATASETS as FINMIND_COMPLEMENT_DATASETS,
    SNAPSHOTS as FINMIND_COMPLEMENT_SNAPSHOTS,
    WIDE_INSTITUTIONAL as FINMIND_DERIVED_WIDE,
)
from downloader.download_finmind_sponsor import SOURCES as FINMIND_SPONSOR_SOURCES


DATA_MONITOR_SCHEMA_VERSION: Final[int] = 8
DATA_MONITOR_SUMMARY_KEYS: Final[tuple[str, ...]] = (
    "schema_version",
    "generated_at_utc",
    "health",
    "read_only",
    "production_control_possible",
    "summary",
    "endpoint_inventory",
    "provider_summaries",
    "market_categories",
    "record_inventory_progress",
    "integrity_checks",
    "definitions",
    "tw_public_acquisition",
    "finlab_acquisition",
    "groups",
)


def project_data_monitor_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Keep the first-paint projection identical across producer and gateway."""

    return {key: payload[key] for key in DATA_MONITOR_SUMMARY_KEYS if key in payload}


TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
_MARKET_CATEGORY_LABELS: Final[dict[str, str]] = {
    "taiwan_equity": "台股",
    "taiwan_derivatives": "臺灣期貨／選擇權",
    "taiwan_public": "臺灣公開資料",
    "global_equity": "海外股票／ETF",
    "forex": "外匯",
    "macro": "總體經濟／公開資訊",
    "cross_market": "跨市場／其他",
    "configuration": "設定／憑證",
    "crypto": "加密貨幣",
}
_GROUP_MARKET_CATEGORY: Final[dict[str, str]] = {
    **{name: "taiwan_equity" for name in (
        "tw-minute-train", "tw-minute-source-cold", "tw-microstructure-train",
        "tw-microstructure-captures-cold", "tw-shioaji-history",
    )},
    **{name: "taiwan_derivatives" for name in (
        "tw-index-futures", "tw-index-derivatives-ticks", "tw-index-options-daily",
        "taifex-public-history", "tw-futures",
    )},
    **{name: "crypto" for name in (
        "okx", "bybit", "binance", "binance-public-archive", "crypto-reference",
        "free-public-context", "coinmetrics-community", "dune-crypto",
        "crypto-etf-history", "fred-crypto-macro", "crypto-historical-public",
    )},
    "tw-public": "taiwan_public",
    "finlab-research": "taiwan_public",
    "finmind-free": "taiwan_public",
    "yahoo-market": "cross_market",
    "openbb-compact": "cross_market",
    "openbb-task-shards-local": "cross_market",
    "forex-frankfurter": "forex",
    "forex-pepperstone": "cross_market",
    "cftc-legacy-pre2000": "macro",
    "legacy-parquet": "cross_market",
}
OPENBB_L1_MAX_SOURCE_FILES_PER_RUN: Final[int] = 2_048
OPENBB_L1_MIN_FILES_PER_SEGMENT: Final[int] = 32
OPENBB_L1_WORST_CASE_RUN_SECONDS: Final[int] = 20 * 60

_GROUP_META: Final[dict[str, dict[str, Any]]] = {
    "finmind-free": {
        "title": "FinMind Free／Sponsor 歷史",
        "provider": "FinMind（Free／Sponsor 帳號）",
        "cadence": "來源配額共享；全市場與逐檔歷史持續回補",
        "owner": "FinMind Free／Sponsor 資料下載器",
        "window": 48 * 3600,
    },
    "finlab-research": {
        "title": "FinLab 帳號歷史研究資料",
        "provider": "FinLab（本機授權來源）",
        "cadence": "每日 16:10（Asia/Taipei）增量下載；依帳號配額",
        "owner": "FinLab 歷史下載器",
        "window": 48 * 3600,
    },
    "tw-public": {
        "title": "臺灣官方公開資料",
        "provider": "TWSE / TPEx / MOPS / CBC / TDCC",
        "cadence": "每個交易日收盤後",
        "owner": "不可變快照更新器",
        "window": 72 * 3600,
    },
    "tw-minute-train": {
        "title": "台股一分鐘研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘資料建置器",
        "window": 72 * 3600,
    },
    "tw-minute-source-cold": {
        "title": "台股一分鐘原始分片",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘回補器",
        "window": 72 * 3600,
    },
    "tw-microstructure-train": {
        "title": "台股微結構研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "每盤落盤後",
        "owner": "微結構資料建置器",
        "window": 7 * 86400,
    },
    "tw-microstructure-captures-cold": {
        "title": "股票／期權即時 Tick 與五檔",
        "provider": "永豐 Shioaji",
        "cadence": "盤中連續",
        "owner": "Shioaji 即時擷取服務",
        "window": 7 * 86400,
    },
    "openbb-compact": {
        "title": "OpenBB 驗證封存",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "openbb-task-shards-local": {
        "title": "OpenBB 可續傳任務分片",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "yahoo-market": {
        "title": "Yahoo 市場資料",
        "provider": "yfinance / Yahoo Finance",
        "cadence": "每日與盤前補齊",
        "owner": "Yahoo OHLCV 更新器",
        "window": 72 * 3600,
    },
    "okx": {
        "title": "OKX 永續合約",
        "provider": "OKX",
        "cadence": "1 分鐘增量",
        "owner": "OKX 公開行情更新器",
        "window": 6 * 3600,
    },
    "bybit": {
        "title": "Bybit 永續合約",
        "provider": "Bybit",
        "cadence": "1 分鐘增量",
        "owner": "Bybit 公開行情更新器",
        "window": 6 * 3600,
    },
    "binance": {
        "title": "Binance USD-M 永續合約",
        "provider": "Binance",
        "cadence": "1 分鐘增量",
        "owner": "Binance 公開行情與特徵更新器",
        "window": 6 * 3600,
    },
    "binance-public-archive": {
        "title": "Binance 現貨與交割合約官方封存",
        "provider": "Binance Data Vision",
        "cadence": "每日官方日檔；月檔發布後重整",
        "owner": "Binance 官方封存校驗更新器",
        "window": 72 * 3600,
    },
    "crypto-reference": {
        "title": "加密資料唯一主來源",
        "provider": "設定 API / 免費原生來源",
        "cadence": "1 分鐘至每日，依來源新鮮度與月配額",
        "owner": "加密來源分配與去重更新器",
        "window": 26 * 3600,
    },
    "free-public-context": {
        "title": "免費公開市場脈絡",
        "provider": "DeFi / Bitcoin / Ethereum / Coin Metrics",
        "cadence": "1 分鐘、事件與每日",
        "owner": "免費公開來源快照更新器",
        "window": 26 * 3600,
    },
    "coinmetrics-community": {
        "title": "Coin Metrics Community 全量日資料",
        "provider": "Coin Metrics Community",
        "cadence": "每日增量與版本保存",
        "owner": "Coin Metrics Community 回補器",
        "window": 72 * 3600,
    },
    "dune-crypto": {
        "title": "Dune 鏈上歷史資料",
        "provider": "Dune",
        "cadence": "每日增量；歷史分區可續傳",
        "owner": "Dune 版本化 SQL 回補器",
        "window": 72 * 3600,
    },
    "crypto-etf-history": {
        "title": "SEC／ETF 發行商歷史資料",
        "provider": "SEC EDGAR / ETF 發行商",
        "cadence": "每日增量；申報事件與發行商日檔",
        "owner": "Crypto ETF 歷史回補器",
        "window": 72 * 3600,
    },
    "fred-crypto-macro": {
        "title": "FRED 加密總體初始發布值",
        "provider": "FRED",
        "cadence": "每日與官方發布事件",
        "owner": "FRED point-in-time 更新器",
        "window": 72 * 3600,
    },
    "tw-index-futures": {
        "title": "TAIFEX 全期貨日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 日資料計時器",
        "window": 72 * 3600,
    },
    "tw-index-derivatives-ticks": {
        "title": "TAIFEX 台指期權逐筆成交",
        "provider": "TAIFEX",
        "cadence": "官方近 30 交易日檔案",
        "owner": "TAIFEX 逐筆回補器",
        "window": 4 * 86400,
    },
    "tw-index-options-daily": {
        "title": "TAIFEX 選擇權日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 選擇權回補器",
        "window": 72 * 3600,
    },
    "taifex-public-history": {
        "title": "TAIFEX 籌碼、風險與統計歷史",
        "provider": "TAIFEX",
        "cadence": "歷史回補；交易日收盤後增量",
        "owner": "TAIFEX 公開歷史更新器",
        "window": 72 * 3600,
    },
    "cftc-legacy-pre2000": {
        "title": "CFTC Legacy 2000 年前部位",
        "provider": "CFTC",
        "cadence": "官方靜態封存",
        "owner": "CFTC Legacy 缺口回補器",
        "window": None,
    },
    "tw-futures": {
        "title": "永豐期貨歷史 Tick",
        "provider": "永豐 Shioaji / TAIFEX",
        "cadence": "配額允許時持續回補",
        "owner": "Shioaji 期貨歷史回補器",
        "window": 15 * 60,
    },
    "forex-frankfurter": {
        "title": "Frankfurter 外匯",
        "provider": "Frankfurter",
        "cadence": "每日",
        "owner": "Frankfurter 更新器",
        "window": 72 * 3600,
    },
    "forex-pepperstone": {
        "title": "Pepperstone 市場資料",
        "provider": "Pepperstone",
        "cadence": "每日",
        "owner": "Pepperstone 更新器",
        "window": 72 * 3600,
    },
    "legacy-parquet": {
        "title": "舊版 Parquet 封存",
        "provider": "歷史封存",
        "cadence": "凍結／待遷移",
        "owner": "人工稽核",
        "window": None,
    },
}

_SUMMARY_CANDIDATES: Final[dict[str, tuple[str, ...]]] = {
    "finmind-free": ("status.json",),
    "tw-public": ("download_summary.json",),
    "yahoo-market": (
        "download_summary.json",
        "daily_update_summary.json",
        "incremental_update_summary.json",
    ),
    "okx": (
        "1m/download_summary.json",
        "daily/download_summary.json",
        "download_summary.json",
    ),
    "bybit": (
        "1m/download_summary.json",
        "daily/download_summary.json",
        "download_summary.json",
    ),
    "binance": (
        "1m/download_summary.json",
        "daily/download_summary.json",
        "download_summary.json",
    ),
    "binance-public-archive": (
        "download_summary.json",
        "plan_summary.json",
        "capacity_receipt.json",
    ),
    "crypto-reference": ("download_summary.json", "source_status.json"),
    "free-public-context": ("download_summary.json",),
    "coinmetrics-community": ("download_summary.json",),
    "dune-crypto": ("download_summary.json",),
    "crypto-etf-history": ("download_summary.json",),
    "fred-crypto-macro": ("download_summary.json",),
    "tw-index-futures": ("manifest.json",),
    "tw-index-derivatives-ticks": ("manifest.json",),
    "tw-index-options-daily": (
        "manifest.json",
        "manifest_weekly.json",
        "manifest_final_settlement.json",
    ),
    "taifex-public-history": (
        "manifest.json",
        "openapi_latest.json",
        "vix_latest.json",
        "progress.json",
    ),
    "cftc-legacy-pre2000": ("manifest.json",),
    "tw-futures": ("shioaji_contracts/manifest.json",),
    "tw-shioaji-history": ("summary.json",),
    "forex-frankfurter": ("download_summary.json",),
    "forex-pepperstone": ("download_summary.json",),
}

_STATUS_PRIORITY: Final[dict[str, int]] = {
    "blocked": 8,
    "unavailable": 7,
    "degraded": 6,
    "stale": 5,
    "waiting": 4,
    "updating": 3,
    "current": 2,
    "complete": 1,
    "deferred": 1,
    "legacy": 0,
}

_REFRESH_UNITS: Final[dict[str, dict[str, str | None]]] = {
    "finmind_free": {
        "service": "stockagent-finmind-free.service",
        "timer": None,
    },
    "finmind_complement": {
        "service": "stockagent-finmind-complement.service",
        "timer": None,
    },
    "finmind_sponsor": {
        "service": "stockagent-finmind-sponsor.service",
        "timer": None,
    },
    "finlab_local": {
        "service": "stockagent-finlab-local-refresh.service",
        "timer": "stockagent-finlab-local-refresh.timer",
    },
    "registered_daily": {
        "service": "stockagent-registered-data-daily.service",
        "timer": "stockagent-registered-data-daily.timer",
    },
    "registered_intraday": {
        "service": "stockagent-registered-data-intraday.service",
        "timer": "stockagent-registered-data-intraday.timer",
    },
    "registered_backfill": {
        "service": "stockagent-registered-data-backfill.service",
        "timer": "stockagent-registered-data-backfill.timer",
    },
    "registered_features": {
        "service": "stockagent-registered-data-features.service",
        "timer": "stockagent-registered-data-features.timer",
    },
    "taifex_futures": {
        "service": "stockagent-taifex-futures-daily.service",
        "timer": "stockagent-taifex-futures-daily.timer",
    },
    "taifex_auxiliary": {
        "service": "stockagent-taifex-auxiliary-daily.service",
        "timer": "stockagent-taifex-auxiliary-daily.timer",
    },
    "taifex_public_history": {
        "service": "stockagent-taifex-public-history.service",
        "timer": "stockagent-taifex-public-history.timer",
    },
    "shioaji_minute": {
        "service": "stockagent-shioaji-minute-backfill.service",
        "timer": "stockagent-shioaji-minute-backfill.timer",
    },
    "shioaji_fop_stream": {
        "service": "stockagent-shioaji-taifex-bidask.service",
        "timer": None,
    },
    "shioaji_stock_stream": {
        "service": "stockagent-shioaji-top200.service",
        "timer": None,
    },
    "shioaji_futures_history": {
        "service": "stockagent-shioaji-tx-history-backfill.service",
        "timer": None,
    },
    "shioaji_historical_market_data": {
        "service": "stockagent-shioaji-historical-market-data.service",
        "timer": None,
    },
    "openbb_archive": {
        "service": "stockagent-openbb-archive.service",
        "timer": None,
    },
    "openbb_l1_compaction": {
        "service": "stockagent-openbb-l1-compaction.service",
        "timer": "stockagent-openbb-l1-compaction.timer",
    },
    "binance_backfill": {
        "service": "stockagent-binance-public-backfill-v4.service",
        "timer": None,
    },
    "binance_public_archive": {
        "service": "stockagent-binance-public-archive.service",
        "timer": "stockagent-binance-public-archive.timer",
    },
    "tw_public_preopen": {
        "service": "stockagent-discord-bot.service",
        "timer": None,
    },
    "tw_public_publication": {
        "service": "stockagent-tw-public-publication-sweep.service",
        "timer": "stockagent-tw-public-publication-sweep.timer",
    },
    "tw_public_source_events": {
        "service": "stockagent-tw-public-source-events.service",
        "timer": None,
    },
    "tw_public_release_archives": {
        "service": "stockagent-tw-public-release-archives.service",
        "timer": "stockagent-tw-public-release-archives.timer",
    },
    "tw_mops_xbrl": {
        "service": "stockagent-tw-mops-xbrl.service",
        "timer": "stockagent-tw-mops-xbrl.timer",
    },
    "tw_public_0830": {
        "service": "stockagent-tw-public-0830-check.service",
        "timer": "stockagent-tw-public-0830-check.timer",
    },
    "tw_public_feature_reconcile": {
        "service": "stockagent-tw-public-feature-reconcile.service",
        "timer": "stockagent-tw-public-feature-reconcile.timer",
    },
    "tw_public_cold_publish": {
        "service": "stockagent-tw-public-cold-publish.service",
        "timer": "stockagent-tw-public-cold-publish.timer",
    },
    "tw_public_eligibility": {
        "service": "stockagent-tw-day-trade-eligibility.service",
        "timer": "stockagent-tw-day-trade-eligibility.timer",
    },
    "tw_day_trade_preopen_gate": {
        "service": "stockagent-tw-day-trade-preopen-gate.service",
        "timer": "stockagent-tw-day-trade-preopen-gate.timer",
    },
}

_AUTOMATION_PROFILES: Final[dict[str, dict[str, Any]]] = {
    "group:finmind-free": {
        "mode": "continuous_backfill",
        "service_keys": ("finmind_free", "finmind_complement", "finmind_sponsor"),
        "schedule_label": "常駐服務依官方每小時配額追新並續補歷史；交易日 08:20–09:10 暫停",
    },
    "group:finlab-research": {
        "mode": "quota_backfill",
        "service_keys": ("finlab_local",),
        "schedule_label": "每日 16:10（Asia/Taipei）；帳號配額不足則次日續抓",
        "requires_timer_active": True,
    },
    "group:tw-public": {
        "mode": "preopen_gate",
        "service_keys": (
            "tw_public_source_events",
            "tw_public_publication",
            "tw_public_0830",
            "tw_public_feature_reconcile",
            "tw_public_cold_publish",
            "tw_public_eligibility",
            "tw_day_trade_preopen_gate",
        ),
        "schedule_label": "來源版本持續監測、07:50 全量掃描、08:20/14:20/19:00 特徵憑證對帳、08:59:30 最終守門；23:50 背景冷備份",
        "active_means_running": False,
    },
    "group:tw-minute-train": {
        "mode": "timer",
        "service_keys": ("shioaji_minute",),
        "schedule_label": "交易日 14:45（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "14:45",
    },
    "group:tw-minute-source-cold": {
        "mode": "timer",
        "service_keys": ("shioaji_minute",),
        "schedule_label": "交易日 14:45（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "14:45",
    },
    "group:tw-microstructure-train": {
        "mode": "upstream_session",
        "service_keys": ("shioaji_stock_stream",),
        "schedule_label": "每個股票即時盤落盤後建置",
        "active_means_running": False,
    },
    "group:tw-microstructure-captures-cold": {
        "mode": "stream",
        "service_keys": ("shioaji_fop_stream", "shioaji_stock_stream"),
        "schedule_label": "依台股與 TAIFEX 交易時窗",
        "stream_kind": "mixed_tw",
        "active_means_running": False,
    },
    "group:openbb-compact": {
        "mode": "continuous_backfill",
        "service_keys": ("openbb_archive",),
        "schedule_label": "常駐回補；依供應商配額冷卻",
    },
    "group:openbb-task-shards-local": {
        "mode": "continuous_backfill",
        "service_keys": ("openbb_archive",),
        "schedule_label": "常駐回補；依供應商配額冷卻",
    },
    "group:yahoo-market": {
        "mode": "timer",
        "service_keys": ("registered_daily", "registered_intraday"),
        "schedule_label": "每日 06:30；Crypto 每輪完成後 1 分鐘",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:okx": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday", "registered_features", "registered_backfill"),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:bybit": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday", "registered_features", "registered_backfill"),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:binance": {
        "mode": "interval_after_completion",
        "service_keys": (
            "registered_intraday",
            "registered_features",
            "registered_backfill",
            "binance_backfill",
        ),
        "schedule_label": "尾端本輪完成後 1 分鐘；每週日 02:00 完整 head/backfill",
    },
    "group:binance-public-archive": {
        "mode": "daily_archive",
        "service_keys": ("binance_public_archive",),
        "schedule_label": "每日 12:30（Asia/Taipei）；官方月檔出現後自動重整",
        "calendar_weekdays": False,
        "calendar_time": "12:30",
    },
    "group:crypto-reference": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "每輪完成後；各端點再依 1 分鐘、15 分鐘或每日 cadence receipt 去重",
    },
    "group:free-public-context": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "本輪完成後 1 分鐘",
    },
    "group:coinmetrics-community": {
        "mode": "interval_after_completion",
        "service_keys": ("registered_intraday",),
        "schedule_label": "本輪完成後 1 分鐘",
    },
    "group:dune-crypto": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）；HTTP 402 時停止新增執行",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:crypto-etf-history": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:fred-crypto-macro": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:tw-index-futures": {
        "mode": "timer",
        "service_keys": ("taifex_futures",),
        "schedule_label": "交易日 16:30（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "16:30",
    },
    "group:tw-index-derivatives-ticks": {
        "mode": "timer",
        "service_keys": ("taifex_auxiliary",),
        "schedule_label": "交易日 17:00（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "17:00",
    },
    "group:tw-index-options-daily": {
        "mode": "timer",
        "service_keys": ("taifex_auxiliary",),
        "schedule_label": "交易日 17:00（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "17:00",
    },
    "group:taifex-public-history": {
        "mode": "timer",
        "service_keys": ("taifex_public_history",),
        "schedule_label": "歷史 receipt 續傳；交易日 17:30（Asia/Taipei）",
        "calendar_weekdays": True,
        "calendar_time": "17:30",
    },
    "group:cftc-legacy-pre2000": {
        "mode": "frozen",
        "service_keys": (),
        "schedule_label": "1986–1999 官方靜態缺口已封存；2000 年後由 OpenBB 更新",
        "active_means_running": False,
    },
    "group:tw-futures": {
        "mode": "quota_backfill",
        "service_keys": ("shioaji_futures_history",),
        "schedule_label": "常駐回補；依實測流量重置證據續跑",
    },
    "group:tw-shioaji-history": {
        "mode": "quota_backfill",
        "service_keys": ("shioaji_historical_market_data",),
        "schedule_label": "常駐 receipt 回補；即時行情優先並受實測流量閘門保護",
    },
    "group:forex-frankfurter": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:forex-pepperstone": {
        "mode": "timer",
        "service_keys": ("registered_daily",),
        "schedule_label": "每日 06:30（Asia/Taipei）",
        "calendar_weekdays": False,
        "calendar_time": "06:30",
    },
    "group:legacy-parquet": {
        "mode": "frozen",
        "service_keys": (),
        "schedule_label": "凍結封存；不再自動更新",
        "active_means_running": False,
    },
}

_OPERATION_ORDER: Final[dict[str, int]] = {
    "catching_up": 0,
    "streaming": 1,
    "complete": 2,
    "unable": 3,
    "deferred": 4,
    "control": 5,
    "reference": 6,
}
_OPERATION_LABELS: Final[dict[str, str]] = {
    "catching_up": "正在抓／還沒到最新",
    "streaming": "正在串流",
    "complete": "已完成／已到最新",
    "unable": "無法完成",
    "deferred": "已延後／未啟用",
    "control": "設定／憑證閘門",
    "reference": "清冊參照／不重複計算",
}
_ANSI_RE: Final[re.Pattern[str]] = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_TQDM_RE: Final[re.Pattern[str]] = re.compile(
    r"(?P<label>(?:precheck|repair|download):[A-Za-z0-9_:-]+):\s*"
    r"(?P<percent>\d+)%\|[^\r\n]*?\|\s*"
    r"(?P<current>\d+)/(?P<total>\d+)\s*"
    r"\[(?P<elapsed>[0-9:]+)<(?P<remaining>[0-9:?]+),"
)
_REFRESH_SERVICE_SNAPSHOT_MAX_AGE_SECONDS: Final[int] = 180


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return default


def _systemd_time(value: Any) -> datetime | None:
    """Parse systemd's localized display timestamp without trusting ``CST``."""

    text = str(value or "").strip()
    if not text or text == "n/a":
        return None
    parts = text.split()
    if len(parts) < 3:
        return None
    try:
        parsed = datetime.fromisoformat(f"{parts[1]}T{parts[2]}")
    except ValueError:
        return None
    return parsed.replace(tzinfo=TAIPEI).astimezone(UTC)


def _systemd_monotonic_time(value: Any) -> datetime | None:
    """Map systemd's absolute CLOCK_MONOTONIC deadline onto the UTC clock."""

    raw = str(value or "").strip()
    if not raw or raw in {"n/a", "infinity"}:
        return None
    units = {"d": 86400, "h": 3600, "min": 60, "s": 1, "ms": .001, "us": .000001}
    matches = list(re.finditer(r"(\d+(?:\.\d+)?)\s*(min|ms|us|d|h|s)", raw))
    if not matches or "".join(match.group(0) for match in matches).replace(" ", "") != raw.replace(" ", ""):
        return None
    deadline = sum(float(match.group(1)) * units[match.group(2)] for match in matches)
    remaining = deadline - clock.monotonic()
    if not 0 <= remaining <= 366 * 86400:
        return None
    return datetime.now(UTC) + timedelta(seconds=remaining)


def _systemd_properties(unit: str, properties: tuple[str, ...]) -> dict[str, str]:
    """Read allowlisted properties from one fixed unit."""

    fields: dict[str, str] = {}
    try:
        result = subprocess.run(
            (
                "systemctl",
                "show",
                unit,
                f"--property={','.join(properties)}",
                "--no-pager",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        result = None
    if result is not None and result.returncode == 0:
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
    return fields


def _systemd_property_sets(
    units: tuple[str, ...], properties: tuple[str, ...]
) -> dict[str, dict[str, str]]:
    """Read all fixed units through one D-Bus round trip."""

    if not units:
        return {}
    try:
        result = subprocess.run(
            (
                "systemctl",
                "show",
                *units,
                f"--property=Id,{','.join(properties)}",
                "--no-pager",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=3.0,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    output: dict[str, dict[str, str]] = {}
    fields: dict[str, str] = {}
    for line in [*result.stdout.splitlines(), ""]:
        if line:
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
            continue
        unit_id = str(fields.get("Id") or "")
        if unit_id:
            output[unit_id] = fields
        fields = {}
    return output


def _fresh_refresh_service_snapshot(
    path: Path,
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]] | None:
    payload = _read_json(path, {})
    if not isinstance(payload, Mapping):
        return None
    try:
        generated = datetime.fromisoformat(
            str(payload.get("generated_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=UTC)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    age_seconds = (observed - generated.astimezone(UTC)).total_seconds()
    if age_seconds < -30 or age_seconds > _REFRESH_SERVICE_SNAPSHOT_MAX_AGE_SECONDS:
        return None
    services = payload.get("services")
    if not isinstance(services, Mapping):
        return None
    output: dict[str, dict[str, Any]] = {}
    for key, value in services.items():
        if not isinstance(value, Mapping):
            continue
        output[str(key)] = {
            **dict(value),
            "evidence_source": "systemd_snapshot",
            "evidence_generated_at_utc": generated.astimezone(UTC).isoformat(),
        }
    return output or None


def _service_state(
    unit: str,
    timer: str | None = None,
    *,
    property_sets: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    """Read a fixed service and timer without exposing invocation identifiers."""

    fields = (
        dict(property_sets.get(unit, {}))
        if property_sets is not None
        else _systemd_properties(
            unit,
            (
                "ActiveState",
                "SubState",
                "NRestarts",
                "Result",
                "ExecMainStartTimestamp",
                "ExecMainExitTimestamp",
            ),
        )
    )
    active = fields.get("ActiveState") in {"active", "activating", "reloading"}
    if not fields:
        cgroup = Path("/sys/fs/cgroup/system.slice") / unit / "cgroup.procs"
        try:
            active = bool(cgroup.read_text(encoding="utf-8").strip())
        except OSError:
            active = False
        if active:
            fields = {"ActiveState": "active", "SubState": "running"}
    timer_fields = (
        dict(property_sets.get(timer, {}))
        if timer and property_sets is not None
        else _systemd_properties(
            timer,
            (
                "ActiveState",
                "SubState",
                "NextElapseUSecRealtime",
                "NextElapseUSecMonotonic",
                "LastTriggerUSec",
            ),
        )
        if timer
        else {}
    )
    next_options = [candidate for candidate in (
        _systemd_time(timer_fields.get("NextElapseUSecRealtime")),
        _systemd_monotonic_time(timer_fields.get("NextElapseUSecMonotonic")),
    ) if candidate is not None]
    output = {
        "active": active,
        "state": fields.get("SubState") or fields.get("ActiveState") or "unknown",
        "restarts": _integer(fields.get("NRestarts")),
        "result": fields.get("Result") or None,
        "started_at_utc": _iso(_systemd_time(fields.get("ExecMainStartTimestamp"))),
        "completed_at_utc": _iso(_systemd_time(fields.get("ExecMainExitTimestamp"))),
        "timer_active": timer_fields.get("ActiveState") == "active",
        "timer_state": timer_fields.get("SubState")
        or timer_fields.get("ActiveState")
        or ("not_applicable" if not timer else "unknown"),
        "next_run_at_utc": _iso(min(next_options)) if next_options else None,
        "last_trigger_at_utc": _iso(_systemd_time(timer_fields.get("LastTriggerUSec"))),
    }
    return output


def _refresh_service_states(
    *,
    snapshot_path: Path | None = None,
    now: datetime | None = None,
    prefer_snapshot: bool = False,
) -> dict[str, dict[str, Any]]:
    if prefer_snapshot and snapshot_path is not None:
        snapshot = _fresh_refresh_service_snapshot(snapshot_path, now=now)
        if snapshot is not None:
            return snapshot
    units = tuple(
        dict.fromkeys(
            str(unit)
            for values in _REFRESH_UNITS.values()
            for unit in (values.get("service"), values.get("timer"))
            if unit
        )
    )
    property_sets = _systemd_property_sets(
        units,
        (
            "ActiveState",
            "SubState",
            "NRestarts",
            "Result",
            "ExecMainStartTimestamp",
            "ExecMainExitTimestamp",
            "NextElapseUSecRealtime",
            "NextElapseUSecMonotonic",
            "LastTriggerUSec",
        ),
    )
    if not property_sets and snapshot_path is not None:
        snapshot = _fresh_refresh_service_snapshot(snapshot_path, now=now)
        if snapshot is not None:
            return snapshot
    states = {
        name: _service_state(
            str(units["service"]),
            str(units["timer"]) if units.get("timer") else None,
            property_sets=property_sets,
        )
        for name, units in _REFRESH_UNITS.items()
    }
    evidence_source = "systemd_live" if property_sets else "cgroup_fallback"
    for state in states.values():
        state["evidence_source"] = evidence_source
    return states


def _duration_seconds(value: str) -> int | None:
    if not value or "?" in value:
        return None
    try:
        parts = [int(part) for part in value.split(":")]
    except ValueError:
        return None
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return None


def _tail_text(path: Path, maximum_bytes: int = 2 * 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, 2)
            handle.seek(max(0, size - maximum_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _runtime_progress(repo_root: Path) -> list[dict[str, Any]]:
    roots = (
        repo_root / "artifacts/daily_downloader/registered_daily",
        repo_root / "artifacts/daily_downloader/registered_intraday",
    )
    paths: list[Path] = []
    for directory in roots:
        try:
            # A busy intraday collector leaves thousands of historical logs.
            # Only the newest by *mtime* is relevant: filenames are not a
            # safe proxy because a resumed older run may still be appending.
            # DirEntry avoids allocating/sorting one Path per old log.
            with os.scandir(directory) as entries:
                latest = max(
                    (entry for entry in entries if entry.name.endswith(".log")),
                    key=lambda entry: entry.stat().st_mtime,
                    default=None,
                )
        except OSError:
            latest = None
        if latest is not None:
            paths.append(directory / latest.name)
    output: list[dict[str, Any]] = []
    for path in paths:
        text = _ANSI_RE.sub("", _tail_text(path).replace("\r", "\n"))
        latest_by_label: dict[str, dict[str, Any]] = {}
        for match in _TQDM_RE.finditer(text):
            current = int(match.group("current"))
            total = int(match.group("total"))
            if total <= 0:
                continue
            remaining = _duration_seconds(match.group("remaining"))
            if remaining == 0 and current < total:
                remaining = 1
            latest_by_label[match.group("label")] = {
                "label": match.group("label"),
                "current": min(current, total),
                "total": total,
                "ratio": min(1.0, max(0.0, current / total)),
                "remaining_seconds": remaining,
            }
        output.extend(latest_by_label.values())
    return output


def _select_runtime_progress(
    rows: Iterable[Mapping[str, Any]],
    *,
    tokens: tuple[str, ...],
) -> dict[str, Any] | None:
    candidates = [
        dict(row)
        for row in rows
        if any(token in str(row.get("label") or "") for token in tokens)
        and (_integer(row.get("current")) or 0) < (_integer(row.get("total")) or 0)
    ]
    if not candidates:
        return None
    # Aggregated groups finish only when their slowest active phase finishes.
    return max(
        candidates,
        key=lambda row: (
            _integer(row.get("remaining_seconds")) or -1,
            _integer(row.get("total")) or 0,
        ),
    )


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        # A date-only ``data_through`` value means coverage through that UTC
        # date, not the first instant of it. Python accepts YYYY-MM-DD as
        # midnight, which made a successful 06:30 refresh appear stale roughly
        # one day too early.
        parsed = datetime.fromisoformat(
            f"{text}T23:59:59+00:00"
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text)
            else text.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _latest_time(
    paths: Iterable[Path], payloads: Iterable[Any] = ()
) -> datetime | None:
    values = [value for value in (_mtime(path) for path in paths) if value is not None]
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "generated_at_utc",
            "generated_at",
            "completed_at_utc",
            "completed_at_taipei",
            "updated_at",
        ):
            parsed = _parse_time(payload.get(key))
            if parsed is not None:
                values.append(parsed)
    return max(values) if values else None


def _coverage(
    current: Any,
    total: Any,
    *,
    unit: str,
    label: str,
) -> dict[str, Any] | None:
    current_value = _integer(current)
    total_value = _integer(total)
    if current_value is None or total_value is None or total_value <= 0:
        return None
    return {
        "current": min(current_value, total_value),
        "total": total_value,
        "ratio": min(1.0, max(0.0, current_value / total_value)),
        "unit": unit,
        "label": label,
    }


def _complete_eta(basis: str = "來源稽核已完成。") -> dict[str, Any]:
    return {
        "state": "complete",
        "remaining_seconds": 0,
        "estimated_complete_at_utc": None,
        "confidence": "high",
        "basis": basis,
    }


def _unknown_eta(state: str, basis: str) -> dict[str, Any]:
    return {
        "state": state,
        "remaining_seconds": None,
        "estimated_complete_at_utc": None,
        "confidence": "not_available",
        "basis": basis,
    }


def _not_applicable_eta(state: str, basis: str) -> dict[str, Any]:
    """Return an explicit non-work ETA without pretending the work completed."""

    return {
        "state": state,
        "remaining_seconds": None,
        "estimated_complete_at_utc": None,
        "confidence": "not_applicable",
        "basis": basis,
    }


def _normalize_eta(
    row: dict[str, Any],
    *,
    operation: str,
    execution: str,
    now: datetime,
) -> None:
    """Reject expired or phase-saturated ETA evidence before publication."""

    eta = row.get("eta")
    eta = (
        dict(eta)
        if isinstance(eta, Mapping)
        else _unknown_eta("unknown", "來源尚未提供可驗證 ETA。")
    )
    if operation == "deferred":
        row["eta"] = _not_applicable_eta(
            "deferred", "此端點依目前取得範圍未啟用，不存在完工倒數。"
        )
        return
    if operation == "control":
        row["eta"] = _not_applicable_eta(
            "not_applicable", "這是設定／憑證就緒狀態，不是資料下載工作。"
        )
        return
    if operation == "reference":
        row["eta"] = _not_applicable_eta(
            "reference", "這是清冊責任映射，不建立重複下載工作或完工倒數。"
        )
        return
    if operation == "complete" and execution == "idle_current":
        eta["state"] = "complete"
        eta["remaining_seconds"] = 0
        eta["estimated_complete_at_utc"] = None
        eta["confidence"] = "high"
        row["eta"] = eta
        return

    if str(eta.get("state") or "") == "complete":
        if operation == "unable":
            state = "blocked"
            basis = "歷史批次可能完成，但目前端點受阻；舊完成 ETA 不代表現在可完成。"
        elif execution == "running":
            state = "warming_up"
            basis = "舊批次已完成，但目前工作仍在執行；等待新吞吐樣本後重估。"
        else:
            state = "waiting_schedule"
            basis = "舊批次已完成，但端點尚未到最新；等待下一次有效取得排程。"
        row["eta"] = _unknown_eta(state, basis)
        return

    remaining = _integer(eta.get("remaining_seconds"))
    estimated_complete = _parse_time(eta.get("estimated_complete_at_utc"))
    expired = estimated_complete is not None and estimated_complete <= now
    phase_saturated = (
        operation == "catching_up" and execution == "running" and remaining == 0
    )
    if expired or phase_saturated:
        warnings = list(row.get("warnings") or [])
        warning = (
            "原 ETA 已過期但工作仍未完成；已清除倒數並等待下一個有效吞吐樣本。"
            if expired
            else "目前階段分母已完成但整體工作仍在執行；已清除假 100% 與假完工時間。"
        )
        if warning not in warnings:
            warnings.append(warning)
        row["warnings"] = warnings
        row["eta"] = _unknown_eta(
            "warming_up",
            "階段切換或舊 ETA 已失效；等待下一個完整邏輯單位後重新估算。",
        )
        return
    row["eta"] = eta


def _freshness(
    latest: datetime | None,
    *,
    now: datetime,
    window_seconds: int | None,
    continuous: bool = False,
) -> dict[str, Any]:
    age = max(0.0, (now - latest).total_seconds()) if latest else None
    if continuous:
        state = "continuous"
    elif latest is None or window_seconds is None:
        state = "not_applicable" if window_seconds is None else "unknown"
    else:
        state = "current" if age <= window_seconds else "stale"
    return {
        "state": state,
        "age_seconds": round(age, 3) if age is not None else None,
        "threshold_seconds": window_seconds,
    }


def _status_counts(payloads: Iterable[Any]) -> dict[str, int]:
    output: dict[str, int] = {}

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) == "status_counts" and isinstance(item, Mapping):
                    for raw_status, raw_count in item.items():
                        count = _integer(raw_count)
                        if count is not None:
                            status = str(raw_status)
                            output[status] = output.get(status, 0) + count
                elif isinstance(item, Mapping):
                    walk(item)

    for payload in payloads:
        walk(payload)
    return output


def _extract_data_through(payloads: Iterable[Any]) -> str | None:
    candidates: list[str] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "applied_end_date",
            "provider_end_date",
            "effective_end_date",
            "end_date",
            "date_end",
            "last_date",
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = str(quality.get("last_date") or "").strip()
            if value:
                candidates.append(value)
    return max(candidates) if candidates else None


def _extract_rows(payloads: Iterable[Any]) -> int | None:
    candidates: list[int] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "stored_row_count",
            "rows_total",
            "row_count",
            "success_rows",
            "rows",
            "rows_this_run",
        ):
            value = _integer(payload.get(key))
            if value is not None:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = _integer(quality.get("rows"))
            if value is not None:
                candidates.append(value)
        all_futures = payload.get("all_futures_daily")
        if isinstance(all_futures, Mapping) and isinstance(
            all_futures.get("quality"), Mapping
        ):
            value = _integer(all_futures["quality"].get("rows"))
            if value is not None:
                candidates.append(value)
    return max(candidates) if candidates else None


def _extract_group_rows(dataset: str, payloads: Iterable[Any]) -> int | None:
    materialized = list(payloads)
    if dataset != "taifex-public-history":
        return _extract_rows(materialized)

    total = 0
    found = False
    for payload in materialized:
        if not isinstance(payload, Mapping):
            continue
        payload_dataset = str(payload.get("dataset") or "")
        if payload_dataset in {
            "taifex_public_history",
            "taifex_openapi_point_in_time_catalog",
        }:
            datasets = payload.get("datasets")
            if isinstance(datasets, list):
                for item in datasets:
                    if not isinstance(item, Mapping):
                        continue
                    rows = _integer(item.get("rows"))
                    if rows is not None:
                        total += rows
                        found = True
        elif payload_dataset == "taifex_vix_daily_recent":
            rows = _integer(payload.get("rows"))
            if rows is not None:
                total += rows
                found = True
    return total if found else _extract_rows(materialized)


def _generic_group(
    root: Path,
    config: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    dataset = str(config.get("dataset") or "unknown")
    meta = _GROUP_META.get(dataset, {})
    source = str(config.get("source") or "")
    source_root = root / source
    candidates = [
        source_root / relative for relative in _SUMMARY_CANDIDATES.get(dataset, ())
    ]
    payloads = [_read_json(path) for path in candidates]
    payloads = [payload for payload in payloads if payload is not None]
    progress_path = (
        source_root / "1m/progress.json"
        if dataset in {"okx", "bybit", "binance"}
        else source_root / "progress.json"
    )
    progress_payload = _read_json(progress_path, {})
    latest = _latest_time(candidates, payloads)
    data_through = _extract_data_through(payloads)
    data_through_time = _parse_time(data_through)
    # Daily datasets are fresh only through their audited data date.  A newly
    # rewritten manifest must not make old market rows look current.
    intraday_current_day = (
        dataset in {"okx", "bybit", "binance"}
        and data_through_time is not None
        and data_through_time.date() >= now.date()
    )
    if (
        data_through_time is not None
        and dataset
        not in {
            "openbb-compact",
            "openbb-task-shards-local",
        }
        and not intraday_current_day
    ):
        latest = data_through_time
    window = meta.get("window")
    fresh = _freshness(
        latest,
        now=now,
        window_seconds=int(window) if isinstance(window, int) else None,
    )
    counts = _status_counts(payloads)
    total = sum(counts.values())
    failure = sum(
        count
        for status, count in counts.items()
        if any(
            token in status.lower()
            for token in (
                "fail",
                "error",
                "partial",
                "mismatch",
                "repair",
                "quarantine",
                "invalid",
                "blocked",
            )
        )
    )
    batch_incomplete = any(
        str(payload.get("state") or "").lower()
        in {"failed", "error", "blocked", "partial"}
        for payload in payloads
        if isinstance(payload, Mapping)
    )
    completed_cycle_with_gaps = any(
        str(payload.get("state") or "").lower() == "partial"
        and str(payload.get("cycle_state") or "").lower() == "complete"
        for payload in payloads
        if isinstance(payload, Mapping)
    )
    completed = max(0, total - failure)
    coverage = _coverage(completed, total, unit="項", label="最近批次")

    if not source_root.exists():
        status = "unavailable"
        status_label = "資料根目錄不存在"
    elif dataset == "legacy-parquet":
        status = "legacy"
        status_label = "凍結封存"
    elif failure or batch_incomplete:
        status = "degraded"
        status_label = (
            "本輪維護完成；歷史資料仍有未驗證缺口"
            if completed_cycle_with_gaps and not failure
            else f"最近批次有 {failure:,} 個失敗" if failure else "最近批次未完整收斂"
        )
    elif fresh["state"] == "stale":
        status = "stale"
        status_label = "需要補到最新"
    elif latest is None:
        status = "degraded"
        status_label = "缺少可驗證摘要"
    else:
        status = "current"
        status_label = "在新鮮度範圍內"

    if (
        status in {"current", "legacy"}
        and (coverage is None or failure == 0)
        and not batch_incomplete
    ):
        eta = _complete_eta("最近可驗證批次沒有待處理失敗。")
    elif status == "unavailable":
        eta = _unknown_eta("blocked", "資料根目錄不存在，無法開始估算。")
    else:
        eta = _unknown_eta(
            "waiting_schedule",
            "尚無執行中吞吐率；更新器開始並寫入進度後才能估算。",
        )

    if isinstance(progress_payload, Mapping):
        progress_state = str(progress_payload.get("state") or "").lower()
        progress_updated = _parse_time(progress_payload.get("updated_at_utc"))
        progress_age = (
            max(0.0, (now - progress_updated).total_seconds())
            if progress_updated is not None
            else None
        )
        progress_live = (
            progress_state == "running"
            and progress_age is not None
            and progress_age <= 15 * 60
        )
        if progress_live:
            progress_phase = str(progress_payload.get("phase") or "")
            raw_progress_counts = progress_payload.get("status_counts")
            raw_current = _integer(progress_payload.get("current"))
            raw_total = _integer(progress_payload.get("total"))
            legacy_page_denominator_saturated = bool(
                isinstance(raw_progress_counts, Mapping)
                and (_integer(raw_progress_counts.get("page_fetched")) or 0) > 0
                and raw_current is not None
                and raw_total is not None
                and raw_total > 0
                and raw_current >= raw_total
                and progress_phase != "complete"
            )
            status = "updating"
            status_label = (
                "正在建立精確容量計畫"
                if progress_phase == "discover"
                else str(progress_payload.get("label") or "資料更新正在執行")
            )
            coverage = (
                None
                if legacy_page_denominator_saturated
                else _coverage(
                    progress_payload.get("current"),
                    progress_payload.get("total"),
                    unit=str(progress_payload.get("unit") or "項"),
                    label="目前批次",
                )
            )
            remaining = (
                None
                if legacy_page_denominator_saturated
                else _integer(progress_payload.get("remaining_seconds"))
            )
            eta = {
                "state": "estimating" if remaining is not None else "warming_up",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": progress_payload.get(
                    "estimated_complete_at_utc"
                ),
                "confidence": "low" if remaining is not None else "not_available",
                "basis": (
                    "舊版進度把可變長度 request page 混入固定分母，分母已飽和但工作仍在執行；"
                    "本輪 ETA 保持未知，下一輪改用 symbol／feature-stage 邏輯單位。"
                    if legacy_page_denominator_saturated
                    else "僅為官方目錄規劃階段 ETA；全量下載會在物件傳輸開始後"
                    "依實測吞吐重新估算。"
                    if progress_phase == "discover"
                    else str(
                        progress_payload.get("basis")
                        or "依目前完整批次吞吐率線性外推。"
                    )
                ),
                "phase": progress_phase or None,
            }
            latest = progress_updated
        elif progress_state in {"failed", "partial"} and (
            progress_age is None or progress_age <= 7 * 86400
        ):
            status = "degraded"
            status_label = (
                "最近更新批次未完整收斂"
                if progress_state == "partial"
                else "最近更新批次失敗"
            )
            eta = _unknown_eta("waiting_schedule", "等待下一輪重試與新回執。")

    warning = []
    if completed_cycle_with_gaps:
        warning.append(
            "cycle_state=complete 只代表本輪維護成功；state=partial 代表歷史資料尚未完整。"
            "隔離來源或修補候選仍須獨立驗證，不能以重跑成功取代完整性證明。"
        )
    if failure or batch_incomplete:
        if not completed_cycle_with_gaps or failure:
            warning.append("最近一次摘要含失敗項目；成功檔案不代表整批完整。")
    if isinstance(progress_payload, Mapping):
        live_counts = progress_payload.get("status_counts")
        live_failures = 0
        if isinstance(live_counts, Mapping):
            live_failures = sum(
                _integer(count) or 0
                for item_status, count in live_counts.items()
                if any(
                    token in str(item_status).lower()
                    for token in (
                        "fail",
                        "error",
                        "partial",
                        "mismatch",
                        "repair",
                        "quarantine",
                        "invalid",
                        "blocked",
                    )
                )
            )
        if live_failures:
            warning.append(
                f"目前批次已有 {live_failures:,} 個失敗／部分完成項；"
                "更新器仍會完成其餘工作並保留錯誤明細。"
            )
        if (
            str(progress_payload.get("state") or "").lower() == "running"
            and isinstance(live_counts, Mapping)
            and (_integer(live_counts.get("page_fetched")) or 0) > 0
            and (_integer(progress_payload.get("total")) or 0) > 0
            and (_integer(progress_payload.get("current")) or 0)
            >= (_integer(progress_payload.get("total")) or 0)
            and str(progress_payload.get("phase") or "") != "complete"
        ):
            warning.append(
                "舊版 request-page 計數已塞滿分母但工作尚未結束；不顯示假 100% 或假 ETA。"
            )
    if not payloads and source_root.exists():
        warning.append("已登錄資料根，但尚未找到可驗證的摘要或 manifest。")
    return {
        "id": f"group:{dataset}",
        "parent_id": None,
        "scope": "storage_group",
        "title": str(meta.get("title") or dataset),
        "provider": str(meta.get("provider") or "其他"),
        "category": str(config.get("role") or "unknown"),
        "status": status,
        "status_label": status_label,
        "cadence": str(meta.get("cadence") or "依來源排程"),
        "update_owner": str(meta.get("owner") or "未指定"),
        "latest_at_utc": _iso(latest),
        "data_through": data_through,
        "freshness": fresh,
        "coverage": coverage,
        "eta": eta,
        "rows": _extract_group_rows(dataset, payloads),
        "publishable": bool(config.get("publish")),
        "automation_eligible": dataset != "legacy-parquet",
        "detail": str(config.get("note") or "已登錄資料群組。"),
        "warnings": warning,
        "detail_link": None,
    }


def _tw_public_publication_index(root: Path) -> dict[str, list[dict[str, Any]]]:
    """Index source-specific publication sweeps without inventing release SLAs.

    A phase receipt proves when our detector ran and which official product
    boundary motivated it.  It does not prove that every selected endpoint was
    published at that clock time, so callers must keep ``probe_boundary`` and
    actual content-change timestamps separate.
    """

    receipt_root = root / "artifacts/data_refresh/tw_public/publications"
    output: dict[str, list[dict[str, Any]]] = {}
    try:
        receipt_paths = sorted(receipt_root.glob("*/latest.json"))
    except OSError:
        receipt_paths = []
    for receipt_path in receipt_paths:
        payload = _read_json(receipt_path, {})
        if not isinstance(payload, Mapping):
            continue
        phase = str(payload.get("phase") or receipt_path.parent.name)
        boundary = str(payload.get("scheduled_boundary") or "").strip()
        selected = payload.get("selected_datasets")
        if not isinstance(selected, list):
            continue
        changed = {
            str(item.get("dataset") or "")
            for item in payload.get("changed_datasets", [])
            if isinstance(item, Mapping)
        }
        receipt = {
            "phase": phase,
            "scheduled_boundary": boundary or None,
            "official_basis": str(payload.get("official_basis") or "") or None,
            "last_started_at_utc": _iso(_parse_time(payload.get("started_at_taipei"))),
            "last_completed_at_utc": _iso(
                _parse_time(payload.get("completed_at_taipei"))
            ),
            "last_status": str(payload.get("status") or "unknown"),
        }
        for raw_name in selected:
            name = str(raw_name or "").strip()
            if not name:
                continue
            output.setdefault(name, []).append(
                {**receipt, "content_change_observed": name in changed}
            )
    for rows in output.values():
        rows.sort(
            key=lambda item: (
                str(item.get("scheduled_boundary") or "99:99:99"),
                str(item.get("phase") or ""),
            )
        )
    return output


def _tw_public_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    base = root / "data_tw_public"
    manifest = _read_json(base / "dataset_manifest.json", [])
    summary = _read_json(base / "download_summary.json", {})
    receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    event_receipt = _read_json(
        root / "artifacts/data_refresh/tw_public/events/latest.json", {}
    )
    event_rows = (
        event_receipt.get("datasets", {}) if isinstance(event_receipt, Mapping) else {}
    )
    event_rows = event_rows if isinstance(event_rows, Mapping) else {}
    accepted_fallbacks = (
        event_receipt.get("accepted_source_fallbacks", {})
        if isinstance(event_receipt, Mapping)
        else {}
    )
    accepted_fallbacks = (
        accepted_fallbacks if isinstance(accepted_fallbacks, Mapping) else {}
    )
    publication_rows = _tw_public_publication_index(root)
    if not isinstance(manifest, list):
        manifest = []
    report: dict[str, dict[str, str]] = {}
    try:
        with (base / "download_report.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            for row in csv.DictReader(handle):
                report[str(row.get("dataset") or "")] = dict(row)
    except (FileNotFoundError, OSError, UnicodeError, csv.Error):
        pass
    generated = _latest_time(
        [
            base / "download_summary.json",
            root / "artifacts/data_refresh/tw_public/latest.json",
        ],
        [summary, receipt],
    )
    fresh = _freshness(generated, now=now, window_seconds=72 * 3600)
    source_unavailable = (
        summary.get("source_unavailable_by_dataset", {})
        if isinstance(summary, Mapping)
        else {}
    )
    rows: list[dict[str, Any]] = []
    for item in manifest:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        audit = report.get(name, {})
        raw_status = str(audit.get("status") or "unknown").lower()
        failed = _integer(audit.get("failed_dates")) or 0
        missing = _integer(audit.get("missing_dates_after")) or 0
        coverage_flag = str(audit.get("coverage_complete", "")).lower()
        complete = coverage_flag == "true" or (
            raw_status in {"ok", "up_to_date", "complete"}
            and failed == 0
            and missing == 0
        )
        event_row = event_rows.get(name, {})
        event_row = event_row if isinstance(event_row, Mapping) else {}
        fallback_name = str(accepted_fallbacks.get(name) or "").strip()
        fallback_row = event_rows.get(fallback_name, {}) if fallback_name else {}
        fallback_row = fallback_row if isinstance(fallback_row, Mapping) else {}
        fallback_version = fallback_row.get("observed_version")
        fallback_applied = bool(
            fallback_name
            and event_receipt.get("status") == "ok"
            and fallback_row.get("last_probe_status") == "ok"
            and fallback_version
            and fallback_row.get("applied_version") == fallback_version
            and fallback_row.get("last_download_status")
            not in {"failed", "error", "incomplete", "pending", "running"}
        )
        publication_receipts = publication_rows.get(name, [])
        event_applied = bool(
            fallback_applied
            or (
                event_row.get("last_probe_status") == "ok"
                and event_row.get("observed_version")
                and event_row.get("observed_version")
                == event_row.get("applied_version")
            )
        )
        if event_row and not event_applied:
            status = "degraded"
            label = (
                "來源探測失敗"
                if event_row.get("last_probe_status") == "failed"
                else "已發現新版本，等待下載驗證"
            )
            eta = _unknown_eta(
                "running_unmeasured",
                "來源事件監測器會持續重試，直到下載與驗證收據成功。",
            )
        elif failed or missing or raw_status in {"failed", "incomplete", "error"}:
            status = "degraded"
            label = f"缺 {missing:,} 日／失敗 {failed:,} 日"
            eta = _unknown_eta(
                "waiting_schedule",
                "等待不可變公開資料更新器再次執行完整缺口掃描。",
            )
        elif complete and fresh["state"] == "current":
            status = "current"
            label = "完整且在新鮮度範圍內"
            eta = _complete_eta("缺口稽核為零，且最新快照已發佈。")
        elif complete:
            status = "stale"
            label = "歷史完整但需要更新"
            eta = _unknown_eta(
                "waiting_schedule",
                "目前沒有執行中吞吐率；下次更新會先掃描再補齊。",
            )
        elif (
            name in {dataset for dataset, _ in ADDED_DATASETS}
            and event_applied
            and (base / f"{name}.parquet").is_file()
            and (base / "metadata" / f"{name}.json").is_file()
            and (base / "raw" / name).is_dir()
            and any(item.is_file() for item in (base / "raw" / name).iterdir())
            and name not in report
        ):
            status = "waiting"
            label = "live 已下載，待完整批次稽核"
            eta = _unknown_eta(
                "waiting_schedule",
                "來源變更已套用；仍需下一次涵蓋此資料集的完整批次稽核。",
            )
        else:
            status = "degraded"
            label = "缺少完整度證據"
            eta = _unknown_eta("unknown", "缺少逐資料集完整度回執。")
        warnings = []
        if fallback_applied:
            warnings.append(
                f"原端點探測失敗仍保留為診斷；目前由已驗證且已套用的官方替代來源 {fallback_name} 提供最新資料。"
            )
        if not event_row:
            warnings.append("尚未取得此資料集的來源版本事件監測證據。")
        if isinstance(source_unavailable, Mapping) and name in source_unavailable:
            warnings.append("含官方來源已確認不可取得的日期；未將其偽裝成成功資料。")
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        provider = str(item.get("source") or "臺灣官方來源")
        if "mops" in name.lower() or any(str(tag).lower() == "mops" for tag in tags):
            provider = "MOPS / 公開資訊觀測站"
        rows.append(
            {
                "id": f"tw-public:{name}",
                "parent_id": "group:tw-public",
                "scope": "logical_source",
                "title": name,
                "provider": provider,
                "category": ", ".join(str(tag) for tag in tags[:3]),
                "status": status,
                "status_label": label,
                "cadence": (
                    f"來源版本每 {int(event_row.get('interval_seconds'))} 秒探測"
                    if _integer(event_row.get("interval_seconds"))
                    else "來源版本持續探測"
                ),
                "update_owner": "來源事件監測器＋不可變快照更新器",
                "latest_at_utc": _iso(
                    _parse_time(event_row.get("last_checked_at_taipei")) or generated
                ),
                "data_through": (
                    str(summary.get("end_date") or "") or None
                    if name in report else None
                ),
                "freshness": dict(fresh),
                "coverage": _coverage(
                    1 if complete else 0, 1, unit="資料集", label="缺口稽核"
                ),
                "eta": eta,
                "rows": _integer(audit.get("rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": (
                    str(item.get("description") or "官方公開資料集。")
                    + " 來源變更只有在定向下載、解析與驗證成功後才會確認套用。"
                ),
                "source_fallback": (
                    {
                        "accepted": True,
                        "replacement_dataset": fallback_name,
                        "replacement_observed_version": fallback_version,
                        "direct_probe_status": event_row.get("last_probe_status"),
                    }
                    if fallback_applied
                    else None
                ),
                "warnings": warnings,
                "detail_link": None,
                "_publication_hint": {
                    "schedule_kind": "probe_boundary",
                    "schedule_label": (
                        "來源版本持續探測；另於 "
                        + " / ".join(
                            str(item.get("scheduled_boundary") or "")[:5]
                            for item in publication_receipts
                            if item.get("scheduled_boundary")
                        )
                        + "（Asia/Taipei）執行公布邊界掃描"
                        if publication_receipts
                        else "來源版本持續探測；來源未承諾固定發布時刻"
                    ),
                    "exact_time_declared": False,
                    "probe_boundaries_taipei": [
                        str(item.get("scheduled_boundary") or "")
                        for item in publication_receipts
                        if item.get("scheduled_boundary")
                    ],
                    "detected_at_utc": _iso(
                        _parse_time(event_row.get("last_changed_at_taipei"))
                    ),
                    "last_checked_at_utc": _iso(
                        _parse_time(event_row.get("last_checked_at_taipei"))
                    ),
                    "applied_at_utc": _iso(
                        _parse_time(event_row.get("last_applied_at_taipei"))
                    ),
                    "next_check_at_utc": _iso(
                        _parse_time(event_row.get("next_probe_at_taipei"))
                    ),
                    "basis": (
                        "固定時刻是具名產品的掃描邊界；同批相關端點以實際內容版本變更時間為準，"
                        "不把掃描時間冒充成每個來源的官方發布 SLA。"
                    ),
                    "receipt_phases": publication_receipts,
                },
            }
        )
    archive = _read_json(base / "state/dgbas_release_vintages.json", {})
    archive = archive if isinstance(archive, Mapping) else {}
    archive_path = base / "dgbas_release_vintages.parquet"
    total = _integer(archive.get("total_releases")) or _integer(
        archive.get("registered_releases")
    )
    completed = _integer(archive.get("completed_releases"))
    if completed is None:
        completed = _integer(archive.get("saved_releases")) or 0
    running = archive.get("status") == "running"
    complete_archive = bool(archive.get("complete")) and archive_path.is_file()
    if running:
        archive_status = "updating"
        if archive.get("phase") == "discovering":
            done_sources = _integer(archive.get("completed_sources")) or 0
            total_sources = _integer(archive.get("total_sources"))
            archive_label = (
                f"已清點 {done_sources:,}/{total_sources:,} 類官方公告"
                if total_sources else "正在清點官方公告"
            )
        else:
            archive_label = f"已處理 {completed:,}/{total:,} 篇官方公告" if total else "正在清點官方公告"
        remaining = _integer(archive.get("estimated_seconds_remaining"))
        archive_eta = (
            {
                "state": "running_estimated",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=remaining)),
                "confidence": "low",
                "basis": "依本次已處理公告的實測平均速度估計；附件及來源節流可能使時間變動。",
            }
            if remaining is not None else _unknown_eta(
                "running_unmeasured", "正在清點來源或尚無足夠吞吐率樣本。"
            )
        )
    elif complete_archive:
        archive_status = "complete"
        archive_label = "本次官方清單及附件抓取完成"
        archive_eta = _complete_eta("已清點本次官方公告清單；不代表每篇都能解析為數值特徵。")
    elif archive:
        archive_status = "degraded"
        if archive.get("source_access_blocked_reason"):
            archive_label = "官方站點阻擋自動存取，歷史未完成"
            archive_eta = _unknown_eta(
                "waiting_source_access", "需等待官方站點允許正常取得；不能由已保存部分推算完成時間。"
            )
        else:
            archive_label = "官方公告仍有失敗或缺期"
            archive_eta = _unknown_eta("waiting_retry", "續跑下載器並檢查逐篇失敗與缺期收據。")
    else:
        archive_status = "waiting"
        archive_label = "尚未建立官方歷史公告收據"
        archive_eta = _unknown_eta("waiting_source", "啟動主計總處逐期公告封存下載器。")
    generated_archive = _parse_time(archive.get("generated_at_utc")) or _parse_time(
        archive.get("started_at_utc")
    )
    rows.append(
        {
            "id": "tw-public:dgbas_release_vintages",
            "parent_id": "group:tw-public",
            "scope": "logical_source",
            "title": "dgbas_release_vintages",
            "provider": "主計總處官方新聞稿",
            "category": "CPI, 失業率, GDP, 歷史公告",
            "status": archive_status,
            "status_label": archive_label,
            "cadence": "交易日 07:00／16:30／19:30；週日稽核",
            "update_owner": "stockagent-tw-public-release-archives.timer",
            "latest_at_utc": _iso(generated_archive),
            "data_through": (
                archive.get("latest_period", {}).get("cpi")
                if isinstance(archive.get("latest_period"), Mapping)
                else None
            ),
            "freshness": _freshness(generated_archive, now=now, window_seconds=45 * 86400),
            "coverage": (
                _coverage(_integer(archive.get("completed_sources")) or 0,
                          _integer(archive.get("total_sources")), unit="類", label="來源清點進度")
                if archive.get("phase") == "discovering" and running
                else _coverage(completed, total, unit="公告", label="逐篇下載進度")
            ),
            "eta": archive_eta,
            "rows": _integer(archive.get("saved_releases")),
            "publishable": complete_archive,
            "automation_eligible": True,
            "detail": "官方逐期新聞稿與原始附件；CPI、失業率、GDP 值來自當期標題或原始 PDF，發布時刻優先用原稿、缺時刻才依當年官方規則推定。",
            "warnings": [
                "公告日期不等於統計期間；GDP headline 與季調 GDP 表定義未核對前不進模型。",
                "缺少標題數值的早年公告保留原檔，不以現在的整包數值倒填。",
                *([str(archive["source_access_blocked_reason"])]
                  if archive.get("source_access_blocked_reason") else []),
            ],
            "detail_link": "https://www.stat.gov.tw/News.aspx?n=2668&sms=10980",
        }
    )
    cbc_archive = _read_json(base / "state/cbc_fx_reserve_release_vintages.json", {})
    cbc_archive = cbc_archive if isinstance(cbc_archive, Mapping) else {}
    cbc_path = base / "cbc_fx_reserve_release_vintages.parquet"
    cbc_total = _integer(cbc_archive.get("total_releases")) or _integer(
        cbc_archive.get("registered_releases")
    )
    cbc_completed = _integer(cbc_archive.get("completed_releases"))
    if cbc_completed is None:
        cbc_completed = _integer(cbc_archive.get("saved_releases")) or 0
    cbc_complete = bool(cbc_archive.get("complete")) and cbc_path.is_file()
    if cbc_archive.get("status") == "running":
        cbc_status = "updating"
        if cbc_archive.get("phase") == "discovering":
            done_pages = _integer(cbc_archive.get("completed_pages")) or 0
            total_pages = _integer(cbc_archive.get("total_pages"))
            cbc_label = (
                f"已清點 {done_pages:,}/{total_pages:,} 頁央行索引"
                if total_pages else "正在清點央行公告"
            )
        else:
            cbc_label = (
                f"已處理 {cbc_completed:,}/{cbc_total:,} 篇央行公告"
                if cbc_total else "正在清點央行公告"
            )
        remaining = _integer(cbc_archive.get("estimated_seconds_remaining"))
        cbc_eta = (
            {
                "state": "running_estimated",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=remaining)),
                "confidence": "low",
                "basis": "依本次央行公告實測速度估計；來源節流可改變剩餘時間。",
            }
            if remaining is not None else _unknown_eta(
                "running_unmeasured", "正在清點來源或尚無足夠吞吐率樣本。"
            )
        )
    elif cbc_complete:
        cbc_status = "complete"
        cbc_label = "本次央行公告清單抓取完成"
        cbc_eta = _complete_eta("公告已封存；不是所有公告都含可機讀數值。")
    elif cbc_archive:
        cbc_status = "degraded"
        cbc_label = "央行公告仍有失敗或缺期"
        cbc_eta = _unknown_eta("waiting_retry", "續跑下載器並檢查逐篇失敗與缺期收據。")
    else:
        cbc_status = "waiting"
        cbc_label = "尚未建立央行歷史公告收據"
        cbc_eta = _unknown_eta("waiting_source", "啟動央行外匯存底新聞稿封存下載器。")
    cbc_generated = _parse_time(cbc_archive.get("generated_at_utc")) or _parse_time(
        cbc_archive.get("started_at_utc")
    )
    rows.append(
        {
            "id": "tw-public:cbc_fx_reserve_release_vintages",
            "parent_id": "group:tw-public",
            "scope": "logical_source",
            "title": "cbc_fx_reserve_release_vintages",
            "provider": "中央銀行官方新聞稿",
            "category": "外匯存底, 歷史公告",
            "status": cbc_status,
            "status_label": cbc_label,
            "cadence": "交易日 07:00／16:30／19:30；週日稽核",
            "update_owner": "stockagent-tw-public-release-archives.timer",
            "latest_at_utc": _iso(cbc_generated),
            "data_through": cbc_archive.get("latest_period"),
            "freshness": _freshness(cbc_generated, now=now, window_seconds=45 * 86400),
            "coverage": (
                _coverage(_integer(cbc_archive.get("completed_pages")) or 0,
                          _integer(cbc_archive.get("total_pages")), unit="頁", label="來源索引進度")
                if cbc_archive.get("phase") == "discovering" and cbc_status == "updating"
                else _coverage(cbc_completed, cbc_total, unit="公告", label="逐篇下載進度")
            ),
            "eta": cbc_eta,
            "rows": _integer(cbc_archive.get("saved_releases")),
            "publishable": cbc_complete,
            "automation_eligible": True,
            "detail": "央行原始外匯存底新聞稿與文內當期數值；不以今日整包統計回填歷史版本。",
            "warnings": [
                "公告只有日期、無可靠逐篇時鐘；保守映射到該日期後的首個已證實交易日。",
                "找不到文內金額的公告保留原始頁，不產生模型數值。",
            ],
            "detail_link": "https://www.cbc.gov.tw/tw/lp-302-1-1-20.html",
        }
    )
    money_archive = _read_json(base / "state/cbc_money_release_vintages.json", {})
    money_archive = money_archive if isinstance(money_archive, Mapping) else {}
    money_path = base / "cbc_money_release_vintages.parquet"
    money_running = money_archive.get("status") == "running"
    money_archive_complete = bool(money_archive.get("complete")) and money_path.is_file()
    money_values_complete = money_archive.get("value_history_complete") is True
    money_complete = money_archive_complete and money_values_complete
    money_missing = money_archive.get("missing_periods")
    money_missing_count = len(money_missing) if isinstance(money_missing, list) else None
    money_total = _integer(money_archive.get("total_releases")) or _integer(
        money_archive.get("registered_releases")
    )
    money_done = _integer(money_archive.get("completed_releases"))
    if money_done is None:
        money_done = _integer(money_archive.get("saved_releases")) or 0
    money_remaining = _integer(money_archive.get("estimated_seconds_remaining"))
    money_generated = _parse_time(money_archive.get("generated_at_utc")) or _parse_time(
        money_archive.get("started_at_utc")
    )
    rows.append({
        "id": "tw-public:cbc_money_release_vintages",
        "parent_id": "group:tw-public",
        "scope": "logical_source",
        "title": "cbc_money_release_vintages",
        "provider": "中央銀行官方新聞稿",
        "category": "M1B, M2, 歷史公告",
        "status": "updating" if money_running else "complete" if money_complete else "degraded" if money_archive else "waiting",
        "status_label": (
            "正在清點央行公告索引" if money_archive.get("phase") == "discovering" and money_running
            else f"已處理 {money_done:,}/{money_total:,} 篇央行公告" if money_running and money_total
            else "原稿已封存，仍有歷史月份缺值" if money_archive_complete and not money_values_complete
            else "已核對原稿及連續月份當期數值" if money_complete
            else "公告仍有失敗或未解析的數值" if money_archive
            else "尚未建立央行貨幣新聞稿收據"
        ),
        "cadence": "交易日 07:00／16:30／19:30 增量；週日全索引",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(money_generated),
        "data_through": money_archive.get("latest_period"),
        "freshness": _freshness(money_generated, now=now, window_seconds=45 * 86400),
        "coverage": (
            _coverage(_integer(money_archive.get("completed_pages")) or 0,
                      _integer(money_archive.get("total_pages")), unit="頁", label="來源索引進度")
            if money_archive.get("phase") == "discovering" and money_running
            else _coverage(money_done, money_total, unit="公告", label="逐篇下載進度")
        ),
        "eta": (
            {"state": "running_estimated", "remaining_seconds": money_remaining,
             "estimated_complete_at_utc": _iso(now + timedelta(seconds=money_remaining)),
             "confidence": "low", "basis": "依已完成公告的實測速度推估。"}
            if money_running and money_remaining is not None
            else _unknown_eta("running_unmeasured", "清點來源中，尚無可靠速度樣本。")
            if money_running else _complete_eta("本次原稿清點完成；缺期另外標示。")
            if money_complete else _unknown_eta("waiting_retry", "尚有歷史缺期或來源失敗。")
        ),
        "rows": _integer(money_archive.get("saved_releases")),
        "publishable": money_complete,
        "automation_eligible": True,
        "detail": "只取逐期新聞稿原文 M1B／M2 年增率；不把現行整包貨幣水準倒填為歷史值。",
        "warnings": ["只記錄公告日期、非已證實逐篇時刻；映射到其後首個已驗證交易日。",
                     "找不到當期數值的原稿保留原文，不製造特徵。",
                     f"歷史數值缺期：{money_missing_count} 個月。" if money_missing_count is not None
                     else "歷史數值缺期尚待清點。"],
        "detail_link": "https://www.cbc.gov.tw/tw/lp-302-1-1-20.html",
    })
    overnight_state = _read_json(base / "state/cbc_overnight_official_pages.json", {})
    overnight_state = overnight_state if isinstance(overnight_state, Mapping) else {}
    overnight_path = base / "supplemental/cbc_overnight_official_pages.parquet"
    overnight_done = _integer(overnight_state.get("full_index_pages")) if overnight_state.get("full_history_scanned") else _integer(overnight_state.get("pages_read")) or 0
    overnight_total = _integer(overnight_state.get("full_index_pages"))
    overnight_good = overnight_path.is_file() and overnight_state.get("full_history_scanned") is True and not overnight_state.get("ambiguous_dates")
    overnight_observed = _parse_time(overnight_state.get("generated_at_utc"))
    rows.append({
        "id": "tw-public:cbc_overnight_official_pages",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "cbc_overnight_official_pages", "provider": "中央銀行官方日表",
        "category": "隔夜拆款利率, 日資料, 歷史數值",
        "status": "complete" if overnight_good else "degraded" if overnight_state else "waiting",
        "status_label": (
            f"官網日表已收齊至 {overnight_state.get('last_subject_date')}；首發版本另待驗證"
            if overnight_good else "日表未收齊或含衝突日期" if overnight_state
            else "尚未下載央行官方日表"
        ),
        "cadence": "交易日 07:00／16:30／19:30 增量；週日全索引",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(overnight_observed),
        "data_through": overnight_state.get("last_subject_date"),
        "freshness": _freshness(overnight_observed, now=now, window_seconds=3 * 86400),
        "coverage": _coverage(overnight_done or 0, overnight_total, unit="頁", label="官方日表歷史索引"),
        "eta": _complete_eta("日表已抓齊；此非首發數值版本驗證。") if overnight_good
        else _unknown_eta("waiting_retry", "先完成官網日表，再比對發布版本。"),
        "rows": _integer(overnight_state.get("distinct_dates")),
        "publishable": overnight_good, "automation_eligible": True,
        "detail": "央行官網每日歷史利率與原始 HTML；修補開放資料 CSV 的舊值衝突及近期延遲。",
        "warnings": ["官網現值未證明當年首發數值或首次發布時刻；不得直接宣稱嚴格 PIT。"],
        "detail_link": "https://www.cbc.gov.tw/tw/lp-641-1.html",
    })
    annual_state = _read_json(base / "state/cbc_usdtwd_annual_pages.json", {})
    annual_state = annual_state if isinstance(annual_state, Mapping) else {}
    annual_path = base / "supplemental/cbc_usdtwd_annual_pages.parquet"
    annual_years = annual_state.get("years") if isinstance(annual_state.get("years"), list) else []
    annual_good = annual_path.is_file() and annual_state.get("status") == "complete" and bool(annual_years)
    annual_observed = _parse_time(annual_state.get("generated_at_utc"))
    rows.append({
        "id": "tw-public:cbc_usdtwd_annual_pages",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "cbc_usdtwd_annual_pages", "provider": "中央銀行官方年表",
        "category": "美元／新臺幣, 日收盤, 歷史數值",
        "status": "complete" if annual_good else "degraded" if annual_state else "waiting",
        "status_label": (
            f"已收集 {len(annual_years)} 年官方日表；首發版本另待驗證"
            if annual_good else "官方年表尚未收齊" if annual_state else "尚未下載官方年表"
        ),
        "cadence": "首次擷取；週日重查年度索引",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(annual_observed),
        "data_through": annual_state.get("last_subject_date"),
        "freshness": _freshness(annual_observed, now=now, window_seconds=10 * 86400),
        "coverage": _coverage(len(annual_years), len(annual_years) if annual_good else None,
                              unit="年", label="已列出的官方年表"),
        "eta": _complete_eta("已列出的年度日表已保存；首發值仍待驗證。") if annual_good
        else _unknown_eta("waiting_retry", "需抓取官方年度索引與各年日表。"),
        "rows": _integer(annual_state.get("distinct_dates")),
        "publishable": annual_good, "automation_eligible": True,
        "detail": "官網年度日表補足開放資料 CSV 之前的匯率數值；保存原始 HTML。",
        "warnings": ["今日查得的年表不等於每個交易日當年的首發版本。"],
        "detail_link": "https://www.cbc.gov.tw/tw/lp-2151-1.html",
    })
    mof_state = _read_json(base / "state/mof_macro_release_dates.json", {})
    mof_state = mof_state if isinstance(mof_state, Mapping) else {}
    mof_path = base / "supplemental/mof_macro_release_dates.parquet"
    mof_done = mof_path.is_file() and mof_state.get("full_history_scanned") is True
    mof_fallback = mof_state.get("tax_pdf_fallback") if isinstance(mof_state.get("tax_pdf_fallback"), Mapping) else {}
    mof_unresolved = mof_fallback.get("not_found") if isinstance(mof_fallback.get("not_found"), list) else []
    mof_fallback_failures = mof_fallback.get("failures") if isinstance(mof_fallback.get("failures"), list) else []
    mof_dates_complete = mof_done and bool(mof_fallback) and not mof_unresolved and not mof_fallback_failures
    mof_observed = _parse_time(mof_state.get("generated_at_utc"))
    mof_categories = mof_state.get("categories") if isinstance(mof_state.get("categories"), Mapping) else {}
    mof_pages_scanned_now = sum(_integer(item.get("pages_read")) or 0 for item in mof_categories.values()
                                if isinstance(item, Mapping))
    mof_total = sum(_integer(item.get("pages")) or 0 for item in mof_categories.values()
                    if isinstance(item, Mapping))
    mof_pages = mof_total if mof_done else mof_pages_scanned_now
    rows.append({
        "id": "tw-public:mof_macro_release_dates",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "mof_macro_release_dates", "provider": "財政部新聞稿索引",
        "category": "進出口／賦稅, 月資料, 官方發布日",
        "status": "complete" if mof_dates_complete else "degraded" if mof_state else "waiting",
        "status_label": (
            "官方新聞稿日期與缺漏 PDF 已掃完；原始數值版本仍待核對"
            if mof_dates_complete else
            f"索引已掃完；{len(mof_unresolved)} 個無原稿、{len(mof_fallback_failures)} 個擷取失敗"
            if mof_done and mof_fallback else
            "財政部索引已掃完，待補查舊 PDF" if mof_done else
            "財政部索引尚未掃完" if mof_state else "尚未下載新聞稿日期索引"
        ),
        "cadence": "交易日 07:00／16:30／19:30 增量；週日全索引",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(mof_observed),
        "data_through": mof_state.get("latest_published_on"),
        "freshness": _freshness(mof_observed, now=now, window_seconds=3 * 86400),
        "coverage": _coverage(mof_pages, mof_total or None, unit="頁", label="財政部新聞稿索引"),
        "eta": _complete_eta("已掃完列出的新聞稿；歷史原始數值另驗證。") if mof_dates_complete
        else _unknown_eta("waiting_retry", "需完成貿易及稅收新聞稿索引。"),
        "rows": _integer(mof_state.get("distinct_release_periods")),
        "publishable": mof_dates_complete, "automation_eligible": True,
        "detail": "逐月保存官方新聞稿發布日與索引 HTML；若無公告才推測發布日。",
        "warnings": ["發布日期不等於發布時刻；今日整包統計值不等於當年首發版本。"] + (
            [f"仍無原始 PDF 的賦稅月份：{', '.join(mof_unresolved)}"] if mof_unresolved else []
        ) + ([f"原稿擷取失敗：{len(mof_fallback_failures)} 個期別。"] if mof_fallback_failures else []),
        "detail_link": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT",
    })
    trade_pdf_state = _read_json(base / "state/mof_trade_release_values.json", {})
    trade_pdf_state = trade_pdf_state if isinstance(trade_pdf_state, Mapping) else {}
    trade_pdf_path = base / "supplemental/mof_trade_release_values.parquet"
    trade_pdf_missing = trade_pdf_state.get("missing_bulk_periods")
    trade_pdf_missing = trade_pdf_missing if isinstance(trade_pdf_missing, list) else []
    trade_pdf_unresolved = trade_pdf_state.get("unresolved_periods")
    trade_pdf_unresolved = trade_pdf_unresolved if isinstance(trade_pdf_unresolved, list) else []
    trade_pdf_done = trade_pdf_state.get("status") == "complete" and not trade_pdf_unresolved
    trade_pdf_observed = _parse_time(trade_pdf_state.get("generated_at_utc"))
    rows.append({
        "id": "tw-public:mof_trade_release_values",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "mof_trade_release_values", "provider": "財政部初報 PDF",
        "category": "進出口, 月資料, 補齊整包 CSV 尾端",
        "status": "complete" if trade_pdf_done else "degraded" if trade_pdf_state else "waiting",
        "status_label": (
            "整包 CSV 缺月已由初報 PDF 補候選值；精度較低"
            if trade_pdf_done else f"仍有 {len(trade_pdf_unresolved)} 個月份未解析"
            if trade_pdf_state else "尚未檢查缺月的初報 PDF"
        ),
        "cadence": "交易日 07:00／16:30／19:30 缺月增量",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(trade_pdf_observed),
        "data_through": max(trade_pdf_missing) if trade_pdf_missing else trade_pdf_state.get("bulk_latest_period"),
        "freshness": _freshness(trade_pdf_observed, now=now, window_seconds=3 * 86400),
        "coverage": _coverage(len(trade_pdf_missing) - len(trade_pdf_unresolved),
                              len(trade_pdf_missing) or None, unit="月", label="整包 CSV 缺月 PDF"),
        "eta": _complete_eta("可解析缺月已補；原 PDF 僅發布到億元。") if trade_pdf_done
        else _unknown_eta("waiting_retry", "需取得並解析未解月份的官方初報 PDF。"),
        "rows": _integer(trade_pdf_state.get("pdf_value_periods")),
        "publishable": trade_pdf_done, "automation_eligible": True,
        "detail": "取得原始新聞稿 PDF；臺幣數值以億元四捨五入，另存為候選，不偽裝成千元精度。",
        "warnings": ["不得把四捨五入候選值當作海關 CSV 的精確千元值或嚴格訓練特徵。"],
        "detail_link": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT_EXP",
    })
    mof_original = _read_json(base / "state/mof_original_release_archive.json", {})
    mof_original = mof_original if isinstance(mof_original, Mapping) else {}
    mof_original_path = base / "supplemental/mof_original_release_archive.parquet"
    mof_indexed = _integer(mof_original.get("indexed_releases"))
    mof_archived = _integer(mof_original.get("archived_releases")) or 0
    mof_original_complete = (
        mof_original_path.is_file() and mof_original.get("status") == "complete"
        and mof_original.get("continuous_index_history") is True
    )
    mof_original_observed = _parse_time(mof_original.get("generated_at_utc"))
    mof_original_latest = mof_original.get("latest_period_by_series")
    mof_original_latest = mof_original_latest if isinstance(mof_original_latest, Mapping) else {}
    mof_original_gaps = mof_original.get("index_period_gaps")
    mof_original_gap_count = sum(len(gaps) for gaps in mof_original_gaps.values()
                                 if isinstance(gaps, list)) if isinstance(mof_original_gaps, Mapping) else 0
    mof_subject_unverified = mof_original.get("subject_unverified_releases")
    mof_subject_unverified = mof_subject_unverified if isinstance(mof_subject_unverified, list) else []
    mof_original_complete = mof_original_complete and not mof_subject_unverified
    rows.append({
        "id": "tw-public:mof_original_release_archive",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "mof_original_release_archive", "provider": "財政部初報原稿",
        "category": "進出口／賦稅, 月資料, 原始 PDF 封存",
        "status": "complete" if mof_original_complete else "degraded" if mof_original else "waiting",
        "status_label": (
            "已保存所列原稿；首發版本仍待核對" if mof_original_complete else
            f"已存 {mof_archived}/{mof_indexed or '?'} 份；索引缺 {mof_original_gap_count} 個月、內文期別未驗 {len(mof_subject_unverified)} 份"
            if mof_original else "尚未下載財政部逐期原稿"
        ),
        "cadence": "交易日近期原稿重驗；週日全量重驗",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": _iso(mof_original_observed),
        "data_through": max((str(value) for value in mof_original_latest.values()), default=None),
        "freshness": _freshness(mof_original_observed, now=now, window_seconds=3 * 86400),
        "coverage": _coverage(mof_archived, mof_indexed, unit="份", label="索引列出的財政部原稿"),
        "eta": _complete_eta("已保存索引原稿；首發版本未證明。") if mof_original_complete
        else _unknown_eta("waiting_retry", "需補齊或核對原始新聞稿 PDF。"),
        "rows": mof_archived, "publishable": mof_original_complete,
        "automation_eligible": True,
        "detail": "保留財政部貿易及稅收初報 PDF、原網址、位元組雜湊與逐期收據。",
        "warnings": ["今日可下載的舊 PDF 不必然是歷史首次發布的位元組版本。"]
        + ([f"原稿內文未能驗證期別：{', '.join(mof_subject_unverified)}"]
           if mof_subject_unverified else []),
        "detail_link": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT",
    })
    provisional_path = root / "artifacts/data_quality/tw_public_provisional_macro/events.parquet"
    provisional_summary = _read_json(provisional_path.with_suffix(".summary.json"), {})
    provisional_summary = provisional_summary if isinstance(provisional_summary, Mapping) else {}
    provisional_receipt_valid = False
    if provisional_path.is_file() and provisional_summary.get("output_sha256"):
        try:
            with provisional_path.open("rb") as handle:
                provisional_receipt_valid = (
                    hashlib.file_digest(handle, "sha256").hexdigest()
                    == provisional_summary["output_sha256"]
                )
        except OSError:
            pass
    provisional_coverage = provisional_summary.get("feature_coverage") if provisional_receipt_valid else None
    provisional_expected = len(provisional_coverage) if isinstance(provisional_coverage, Mapping) else None
    provisional_count = sum(
        _integer(item.get("rows")) not in (None, 0)
        for item in provisional_coverage.values() if isinstance(item, Mapping)
    ) if isinstance(provisional_coverage, Mapping) else 0
    rows.append({
        "id": "tw-public:provisional_macro_feature_events",
        "parent_id": "group:tw-public", "scope": "logical_source",
        "title": "provisional_macro_feature_events", "provider": "央行／主計總處／財政部",
        "category": "總體特徵原值與衍生候選, 暫定發布時刻, 待 PIT 驗證",
        "status": "degraded" if provisional_path.is_file() else "waiting",
        "status_label": f"{provisional_count}/{provisional_expected} 欄已有候選歷史值；全部仍需原始數值版本驗證"
        if provisional_receipt_valid else "候選歷史總帳收據缺失或雜湊不符",
        "cadence": "官方公告封存完成後重建",
        "update_owner": "stockagent-tw-public-release-archives.timer",
        "latest_at_utc": None, "data_through": None,
        "freshness": _freshness(None, now=now, window_seconds=86400),
        "coverage": _coverage(provisional_count, provisional_expected, unit="欄", label="候選歷史值欄位"),
        "eta": _unknown_eta("waiting_source", "原始數值版本與逐期發布時刻尚待補齊；不能估計嚴格 PIT 完成時間。"),
        "rows": _integer(provisional_summary.get("total_rows")) if provisional_receipt_valid else None,
        "publishable": False, "automation_eligible": True,
        "detail": "與嚴格訓練表分離；每筆保留數值來源、日期證據等級和推定時刻，strict_pit_eligible 一律為 false。",
        "warnings": ["現行整包歷史值可能含事後修訂；推定發布時刻不是精確發布證據。"]
        if provisional_receipt_valid else ["候選表與摘要未通過位元組雜湊核對；不展示逐欄筆數。"],
    })
    # The aggregate ledger is not a substitute for field-level coverage.  Its
    # build receipt already contains exact feature-period counts and bounds, so
    # the monitor can expose every candidate without rescanning Parquet on each
    # HTTP request or implying that any field is strict-PIT ready.
    if provisional_receipt_valid and isinstance(provisional_coverage, Mapping):
        for feature, stats in sorted(provisional_coverage.items()):
            if not isinstance(stats, Mapping):
                continue
            count = _integer(stats.get("rows")) or 0
            first = stats.get("first_subject_period")
            last = stats.get("last_subject_period")
            invalid = _integer(stats.get("raw_value_only_rows")) or 0
            estimated_dates = _integer(stats.get("estimated_publication_date_rows")) or 0
            rows.append({
                "id": f"tw-public:provisional-feature:{feature}",
                "parent_id": "group:tw-public", "scope": "logical_source",
                "title": str(feature), "provider": "台股公開總體特徵",
                "category": "候選歷史特徵, 非嚴格 PIT",
                "status": "degraded" if count else "waiting",
                "status_label": f"候選值 {count:,} 筆；推定發布日 {estimated_dates:,} 筆；僅可用原值 {invalid:,} 筆",
                "cadence": "官方來源更新後重建",
                "update_owner": "stockagent-tw-public-release-archives.timer",
                "latest_at_utc": None, "data_through": last,
                "freshness": _freshness(None, now=now, window_seconds=86400),
                "coverage": _coverage(count, None, unit="筆", label="候選歷史值"),
                "eta": _unknown_eta("waiting_source", "歷史首發數值與精確時刻未齊；不能估計嚴格 PIT 完成時間。"),
                "rows": count, "publishable": False, "automation_eligible": True,
                "detail": f"最早資料期 {first or '未知'}；最新資料期 {last or '未知'}。與總帳重複，不可加總。",
                "warnings": ["推定發布時刻及目前可下載的舊數值，不等於歷史首發版本。"],
                "_provisional_feature_stats": {"count": count, "first": first, "last": last},
            })
    xbrl = _read_json(base / "mops_xbrl/state.json", {})
    xbrl = xbrl if isinstance(xbrl, Mapping) else {}
    xbrl_total = _integer(xbrl.get("discovered_periods"))
    xbrl_done = _integer(xbrl.get("completed_periods")) or 0
    xbrl_local = _integer(xbrl.get("local_imported_periods"))
    if xbrl_local is None:
        xbrl_local = xbrl_done
    xbrl_observed = _parse_time(xbrl.get("generated_at_utc"))
    xbrl_covered = xbrl_total is not None and xbrl_done == xbrl_total
    xbrl_local_covered = xbrl_total is not None and xbrl_local == xbrl_total
    rows.append({
        "id": "tw-public:mops_xbrl_quarterly",
        "parent_id": "group:tw-public",
        "scope": "logical_source",
        "title": "mops_xbrl_quarterly",
        "provider": "MOPS / 公開資訊觀測站",
        "category": "台股, 季報, XBRL 原始事實",
        "status": (
            "updating" if xbrl.get("status") == "updating"
            else "degraded" if xbrl.get("status") == "failed" or xbrl_covered
            else "blocked" if not xbrl.get("automated_download_authorized") else "waiting"
        ),
        "status_label": (
            f"正在匯入季度 ZIP：{xbrl_local:,}/{xbrl_total:,} 季" if xbrl.get("status") == "updating" and xbrl_total is not None
            else "季度 ZIP 匯入失敗，等待診斷／重試" if xbrl.get("status") == "failed"
            else "季度 ZIP 已涵蓋；申報發布時刻尚未驗證" if xbrl_covered
            else f"本機已匯入 {xbrl_local:,}/{xbrl_total:,} 季；來源與申報時刻待驗證，自動更新待授權"
            if xbrl_local_covered and not xbrl.get("automated_download_authorized")
            else f"本機已匯入 {xbrl_local:,}/{xbrl_total:,} 季；缺自動下載授權"
            if not xbrl.get("automated_download_authorized") and xbrl_total is not None
            else "自動下載需證交所授權；尚無可驗證季度清單"
            if not xbrl.get("automated_download_authorized")
            else f"本機已匯入 {xbrl_local:,}/{xbrl_total:,} 季" if xbrl_total is not None
            else "尚未清點官方季度清單"
        ),
        "cadence": "取得自動下載授權後，每次排程重掃官方季度清單",
        "update_owner": "download_tw_mops_xbrl.py；timer 啟用狀態另見排程證據",
        "latest_at_utc": _iso(xbrl_observed),
        "data_through": xbrl.get("latest_period") if xbrl_covered else None,
        "freshness": _freshness(xbrl_observed, now=now, window_seconds=7 * 86400),
        "coverage": _coverage(xbrl_local, xbrl_total, unit="季", label="本機季度 ZIP 正規化匯入"),
        "eta": _unknown_eta("waiting_authorization" if not xbrl.get("automated_download_authorized")
                            else "waiting_schedule", "未有可據以估計完成時間的實測吞吐率與有效排程。"),
        "rows": _integer(xbrl.get("local_fact_rows")) if xbrl.get("local_fact_rows") is not None
                else _integer(xbrl.get("fact_rows")),
        "publishable": False,
        "automation_eligible": xbrl.get("automated_download_authorized") is True,
        "authorization_expires_on": xbrl.get("authorization_expires_on"),
        "detail": "本機匯入與官方來源驗證分開計數；按季原始 ZIP 保留版本與 SHA-256，正規化事實另存，季度結束日不能充當申報發布時刻。",
        "warnings": ["本機 ZIP 匯入不證明來源真實性、逐公司首次申報時刻與後續修訂；不可直接作為歷史回測特徵。",
                     "ToAlpha 依條款只供有限互動查詢，不做全市場自動缺口鏡像。"]
                    + ([f"最近一次失敗：{str(xbrl.get('last_error'))[:240]}"] if xbrl.get("last_error") else []),
        "detail_link": "https://mopsov.twse.com.tw/mops/web/t203sb02",
    })
    return rows


def _shioaji_sources(
    status: Mapping[str, Any], *, now: datetime
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    pipelines = status.get("pipelines")
    if not isinstance(pipelines, list):
        return output
    status_map = {
        "active": "updating",
        "ready": "current",
        "complete": "complete",
        "waiting": "waiting",
        "partial": "degraded",
        "attention": "degraded",
        "blocked": "blocked",
    }
    for pipeline in pipelines:
        if not isinstance(pipeline, Mapping):
            continue
        raw_status = str(pipeline.get("status") or "unavailable").lower()
        row_status = status_map.get(raw_status, "unavailable")
        latest = _parse_time(pipeline.get("latest_at_utc"))
        eta = pipeline.get("eta")
        coverage = pipeline.get("coverage")
        output.append(
            {
                "id": f"shioaji:{pipeline.get('id')}",
                "parent_id": (
                    "group:tw-futures"
                    if pipeline.get("id") == "futures_history"
                    else "group:tw-shioaji-history"
                    if pipeline.get("id") == "historical_market_data"
                    else "group:tw-microstructure-captures-cold"
                ),
                "scope": "logical_source",
                "title": str(pipeline.get("title") or pipeline.get("id") or "Shioaji"),
                "provider": "永豐 Shioaji",
                "category": str(pipeline.get("category") or "market-data"),
                "status": row_status,
                "status_label": str(pipeline.get("status_label") or raw_status),
                "cadence": "盤中連續"
                if pipeline.get("category") == "realtime"
                else "配額允許時持續回補",
                "update_owner": "Shioaji 資料服務",
                "latest_at_utc": _iso(latest),
                "data_through": str(pipeline.get("data_through") or "") or None,
                "freshness": _freshness(
                    latest,
                    now=now,
                    window_seconds=None
                    if pipeline.get("category") == "realtime"
                    else 72 * 3600,
                    continuous=pipeline.get("category") == "realtime",
                ),
                "coverage": dict(coverage) if isinstance(coverage, Mapping) else None,
                "eta": dict(eta)
                if isinstance(eta, Mapping)
                else _unknown_eta("unknown", "來源尚未提供 ETA 證據。"),
                "rows": None,
                "publishable": False,
                "automation_eligible": pipeline.get("category") != "on_demand",
                "detail": str(pipeline.get("detail") or "Shioaji 資料管線。"),
                "warnings": [str(value) for value in pipeline.get("warnings", [])],
                "detail_link": "../shioaji/",
            }
        )
    return output


def _openbb_sources(
    status: Mapping[str, Any], *, now: datetime
) -> list[dict[str, Any]]:
    providers = status.get("providers")
    if not isinstance(providers, list):
        providers = []
    latest = now
    source_age = _number(status.get("source_age_seconds"))
    if source_age is not None:
        latest = now - timedelta(seconds=source_age)
    output: list[dict[str, Any]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        accepted = _integer(provider.get("accepted_tasks")) or 0
        backlog = _integer(provider.get("exclusive_backlog_tasks"))
        if backlog is None:
            backlog = _integer(provider.get("eligible_backlog_tasks")) or 0
        rate = _number(provider.get("recent_tasks_per_minute")) or 0.0
        active = (_integer(provider.get("active")) or 0) > 0
        cooldown = provider.get("cooldown") is True
        if backlog == 0:
            row_status = "complete"
            label = "目前無待處理任務"
            eta = _complete_eta("此供應商目前沒有專屬待處理任務。")
        elif active or rate > 0:
            row_status = "updating"
            label = "正在回補"
            seconds = int(math.ceil(backlog / rate * 60)) if rate > 0 else None
            eta = {
                "state": "estimating" if seconds is not None else "unknown",
                "remaining_seconds": seconds,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds))
                if seconds is not None
                else None,
                "confidence": "low",
                "basis": "依此供應商最近接受任務速率與專屬 backlog 線性外推；配額會改變結果。",
            }
        elif cooldown:
            row_status = "waiting"
            label = "等待配額解除"
            eta = _unknown_eta(
                "waiting_quota",
                "配額冷卻中；在恢復有效吞吐率前不提供假完成時間。",
            )
        else:
            row_status = "waiting"
            label = "等待排程"
            eta = _unknown_eta("waiting_schedule", "目前速率為零，無法可靠外推。")
        output.append(
            {
                "id": f"openbb:{provider.get('provider')}",
                "parent_id": "group:openbb-compact",
                "scope": "logical_source",
                "title": str(provider.get("provider") or "OpenBB provider"),
                "provider": "OpenBB",
                "category": "archive-provider",
                "status": row_status,
                "status_label": label,
                "cadence": "持續回補",
                "update_owner": "OpenBB 供應商排程器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(latest, now=now, window_seconds=15 * 60),
                "coverage": _coverage(
                    accepted,
                    accepted + backlog,
                    unit="任務",
                    label="已接受／專屬待處理",
                ),
                "eta": eta,
                "rows": _integer(provider.get("success_rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": f"最近 {rate:,.1f} 任務/分鐘；專屬待處理 {backlog:,}。",
                "warnings": ["供應商 unavailable 與成功資料分開計數。"]
                if cooldown
                else [],
                "detail_link": "../openbb/",
            }
        )
    l1 = status.get("l1_compaction")
    if isinstance(l1, Mapping) and l1.get("generated_at_utc") is not None:
        success_files = _integer(l1.get("success_files")) or 0
        compacted_files = _integer(l1.get("compacted_files")) or 0
        pending_files = max(
            _integer(l1.get("pending_files")) or 0,
            success_files - compacted_files,
        )
        source_age = _number(l1.get("source_age_seconds"))
        latest = now - timedelta(seconds=source_age) if source_age is not None else None
        deferred_query_views = l1.get("deferred_query_views")
        deferred_query_views = (
            deferred_query_views if isinstance(deferred_query_views, Mapping) else {}
        )
        if pending_files == 0 and deferred_query_views:
            row_status = "partial"
            status_label = "壓實完成；部分查詢檢視待正規化"
            eta = _unknown_eta(
                "query_normalization_required",
                "超寬端點已保留 L1 Parquet，但需轉為 long-form 後才能發布查詢檢視。",
            )
        elif pending_files == 0:
            row_status = "complete"
            status_label = "目前成功 shard 已全部壓實"
            eta = _complete_eta("目前沒有待壓實的成功 shard。")
        elif source_age is None or source_age > 2 * 3600:
            row_status = "stale"
            status_label = "壓實狀態逾時"
            eta = _unknown_eta(
                "stale_status", "L1 狀態超過兩小時未更新，不能可靠估計完成時間。"
            )
        else:
            row_status = "waiting"
            status_label = "等待下一輪增量壓實"
            # Keep this capacity contract synchronized with the installed
            # systemd unit. It is a low-confidence no-new-arrivals projection,
            # not a completion promise.
            runs = math.ceil(pending_files / OPENBB_L1_MAX_SOURCE_FILES_PER_RUN)
            seconds = runs * OPENBB_L1_WORST_CASE_RUN_SECONDS
            eta = {
                "state": "estimating",
                "remaining_seconds": seconds,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds)),
                "confidence": "low",
                "basis": (
                    "依每輪最多 "
                    f"{OPENBB_L1_MAX_SOURCE_FILES_PER_RUN:,} shard 且沒有新資料的容量估計；"
                    f"小於 {OPENBB_L1_MIN_FILES_PER_SEGMENT} 檔的 endpoint tail 會等待累積。"
                ),
            }
        source_bytes = _integer(l1.get("source_bytes")) or 0
        output_bytes = _integer(l1.get("output_bytes")) or 0
        reduction = 100.0 * (1.0 - output_bytes / source_bytes) if source_bytes else 0.0
        output.append(
            {
                "id": "openbb:l1-compaction",
                "parent_id": "group:openbb-compact",
                "scope": "logical_source",
                "title": "OpenBB L1 Parquet 壓實",
                "provider": "本機 DuckDB／Polars／PyArrow",
                "category": "storage-compaction",
                "status": row_status,
                "status_label": status_label,
                "cadence": "每半小時",
                "update_owner": "OpenBB L1 壓實 timer",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(latest, now=now, window_seconds=2 * 3600),
                "coverage": _coverage(
                    compacted_files,
                    success_files,
                    unit="shard",
                    label="已壓實／成功 L0",
                ),
                "eta": eta,
                "rows": _integer(l1.get("compacted_rows")),
                "publishable": True,
                "automation_eligible": True,
                "detail": (
                    f"active segments {_integer(l1.get('active_segments')) or 0:,}；"
                    f"待壓實 {pending_files:,} shards；已壓實來源空間縮減 {reduction:.2f}%。"
                ),
                "warnings": [
                    "L1 是 shadow query layer；L0 原始 shard 保留且未刪除。",
                    *(
                        [
                            "待正規化查詢檢視："
                            + "、".join(
                                sorted(str(key) for key in deferred_query_views)
                            )
                        ]
                        if deferred_query_views
                        else []
                    ),
                ],
                "detail_link": "../openbb/",
            }
        )
    return output


def _crypto_feature_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Project exchange catalogs without pretending deferred archives are complete."""

    native_five_minute_prefixes = {
        "open_interest",
        "contract_taker_volume",
        "taker_buy_sell_volume",
        "contract_long_short_account_ratio",
        "global_long_short_account_ratio",
        "top_trader_account_ratio",
        "top_trader_position_ratio",
        "basis",
    }

    def migrate_legacy_catalog(source: Mapping[str, Any]) -> dict[str, Any]:
        # Legacy catalog files remain provenance for the still-running 15m job.
        # Reuse only their endpoint inventory, then relabel definitions according
        # to the new canonical 1m / native-5m contract.  Their mtimes are not used
        # as evidence that the new 1m dataset has run.
        text = json.dumps(source, ensure_ascii=False)
        migrated = json.loads(
            text.replace("15-minute", "one-minute").replace("15m", "1m")
        )
        migrated["bar"] = "1m"
        migrated["generated_at"] = None
        migrated["generated_at_utc"] = None
        for item in migrated.get("catalog", []):
            if not isinstance(item, dict):
                continue
            source_id = str(item.get("id") or "")
            if not source_id.endswith("_1m"):
                continue
            prefix = source_id.removesuffix("_1m")
            if prefix not in native_five_minute_prefixes:
                continue
            item["id"] = f"{prefix}_5m"
            item["grain"] = "instrument_native_5m_aligned_to_1m"
            item["history_contract"] = (
                "official native 5m observations within the provider retention "
                "boundary; causally aligned to completed 1m bars without interpolation"
            )
        return migrated

    specs = (
        (
            "okx",
            "OKX",
            root / "data_okx/1m/okx_historical_feature_catalog.json",
            root / "data_okx/okx_historical_feature_catalog.json",
            root / "data_okx/1m/historical_feature_report.csv",
        ),
        (
            "binance",
            "Binance",
            root / "data_binance/1m/binance_historical_feature_catalog.json",
            root / "data_binance/binance_historical_feature_catalog.json",
            root / "data_binance/1m/historical_feature_report.csv",
        ),
    )
    rows: list[dict[str, Any]] = []
    for dataset, provider, catalog_path, legacy_catalog_path, report_path in specs:
        payload = _read_json(catalog_path, {})
        if not payload:
            legacy = _read_json(legacy_catalog_path, {})
            payload = (
                migrate_legacy_catalog(legacy)
                if isinstance(legacy, Mapping) and legacy
                else {}
            )
        catalog = payload.get("catalog", []) if isinstance(payload, Mapping) else []
        if not isinstance(catalog, list):
            continue
        latest = _latest_time([catalog_path, report_path], [payload])
        fresh = _freshness(latest, now=now, window_seconds=6 * 3600)
        report_rows: list[dict[str, str]] = []
        try:
            with report_path.open(encoding="utf-8", newline="") as handle:
                report_rows = [dict(row) for row in csv.DictReader(handle)]
        except (FileNotFoundError, OSError, UnicodeError, csv.Error):
            pass
        stage_statuses: dict[str, list[str]] = {}
        stage_coverage: dict[str, int] = {}
        for report in report_rows:
            try:
                statuses = json.loads(report.get("stage_status_json") or "{}")
            except json.JSONDecodeError:
                statuses = {}
            try:
                coverage = json.loads(report.get("coverage_json") or "{}")
            except json.JSONDecodeError:
                coverage = {}
            if isinstance(statuses, Mapping):
                for stage, status in statuses.items():
                    stage_statuses.setdefault(str(stage), []).append(str(status))
            if isinstance(coverage, Mapping):
                for stage, item in coverage.items():
                    if isinstance(item, Mapping):
                        stage_coverage[str(stage)] = stage_coverage.get(
                            str(stage), 0
                        ) + (_integer(item.get("rows")) or 0)

        id_to_stage = {
            "mark_price_candles_1m": "mark_price",
            "index_price_candles_1m": "index_price",
            "premium_index_candles_1m": "premium_index",
            "funding_rate": "funding_rate",
            "funding_rate_history": "funding_rate",
            "open_interest_5m": "open_interest",
            "contract_taker_volume_5m": "taker_volume",
            "taker_buy_sell_volume_5m": "taker_buy_sell_volume",
            "contract_long_short_account_ratio_5m": "long_short_account_ratio",
            "global_long_short_account_ratio_5m": ("global_long_short_account_ratio"),
            "top_trader_account_ratio_5m": "top_trader_account_ratio",
            "top_trader_position_ratio_5m": "top_trader_position_ratio",
            "basis_5m": "basis",
        }
        for item in catalog:
            if not isinstance(item, Mapping):
                continue
            source_id = str(item.get("id") or "").strip()
            if not source_id:
                continue
            download_status = str(item.get("download_status") or "registered")
            stage = id_to_stage.get(source_id)
            statuses = stage_statuses.get(stage or "", [])
            completed = sum(status == "ok" for status in statuses)
            failures = sum(status == "failed" for status in statuses)
            total = len(statuses)
            scheduled = download_status.startswith("included")
            registry_alias = download_status in {
                "excluded_redundant",
                "excluded_duplicate",
            }
            deferred = not registry_alias and (
                download_status.startswith("separate")
                or download_status.startswith("excluded")
            )
            if failures:
                status = "degraded"
                label = f"{failures:,} 個商品階段失敗"
                eta = _unknown_eta("waiting_schedule", "等待下一輪端點重試。")
            elif scheduled and total and completed == total:
                status = "current" if fresh["state"] == "current" else "stale"
                label = (
                    "已納入更新且最近批次成功"
                    if status == "current"
                    else "歷史已抓但需要增量"
                )
                eta = _complete_eta("最近逐商品階段均成功。")
            elif source_id == "trade_candles_1m":
                status = "current" if fresh["state"] == "current" else "stale"
                label = "由主 OHLCV 更新器維護"
                eta = _complete_eta("主價格資料由同一交易所批次維護。")
            elif registry_alias:
                status = "current"
                label = "與專用端點重複；只保留清冊參照"
                eta = _not_applicable_eta(
                    "reference", "相同事實由既有專用端點唯一維護。"
                )
            elif deferred:
                status = "deferred"
                label = "已登錄，但不在目前 1m-only 取得範圍"
                eta = _not_applicable_eta(
                    "deferred",
                    "逐筆、委託簿、快照或非永續商品資料依使用者決策暫不排程。",
                )
            else:
                status = "waiting"
                label = "已登錄，等待首批回執"
                eta = _unknown_eta("waiting_schedule", "尚無逐商品吞吐率可估算。")
            rows.append(
                {
                    "id": f"{dataset}-feature:{source_id}",
                    "parent_id": f"group:{dataset}",
                    "scope": "logical_source",
                    "title": source_id,
                    "provider": provider,
                    "category": str(
                        item.get("category") or item.get("grain") or "feature"
                    ),
                    "status": status,
                    "status_label": label,
                    "cadence": "1 分鐘增量" if scheduled else "獨立排程",
                    "update_owner": f"{provider} 公開資料更新器",
                    "latest_at_utc": _iso(latest),
                    "data_through": None,
                    "freshness": dict(fresh),
                    "coverage": _coverage(
                        completed,
                        total,
                        unit="商品",
                        label="最近特徵階段",
                    ),
                    "eta": eta,
                    "rows": stage_coverage.get(stage or ""),
                    "publishable": True,
                    "automation_eligible": (
                        bool(scheduled) or source_id == "trade_candles_1m"
                    )
                    and not deferred
                    and not registry_alias,
                    "acquisition_enabled": not deferred and not registry_alias,
                    "registry_alias": registry_alias,
                    "detail": str(
                        item.get("model_role")
                        or item.get("reason")
                        or item.get("history_contract")
                        or "交易所公開資料。"
                    ),
                    "warnings": (
                        ["目前快照不得倒填歷史；只有重新啟用後的觀測值可使用。"]
                        if "snapshot" in download_status
                        else ["此端點與既有資料重複，不建立第二份下載進度。"]
                        if registry_alias
                        else []
                    ),
                    "detail_link": None,
                }
            )
    return rows


def _credential_states(root: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json(root / "artifacts/data_credentials/status.json", {})
    providers = payload.get("providers", []) if isinstance(payload, Mapping) else []
    return {
        str(item.get("id")): dict(item)
        for item in providers
        if isinstance(item, Mapping) and item.get("id")
    }


def _credential_registry_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Expose credential readiness without ever exposing credential values."""

    payload = _read_json(root / "artifacts/data_credentials/status.json", {})
    providers = payload.get("providers", []) if isinstance(payload, Mapping) else []
    generated_at = (
        _parse_time(payload.get("generated_at_utc"))
        if isinstance(payload, Mapping)
        else None
    )
    freshness = _freshness(generated_at, now=now, window_seconds=26 * 3600)
    rows: list[dict[str, Any]] = []
    for item in providers if isinstance(providers, list) else []:
        if not isinstance(item, Mapping) or not item.get("id"):
            continue
        credential_id = str(item["id"])
        provider = str(item.get("provider") or credential_id)
        state = str(item.get("state") or "missing")
        configured = _integer(item.get("configured_count")) or 0
        required = max(1, _integer(item.get("required_count")) or 1)
        is_openbb = credential_id.startswith("openbb:")
        if state == "configured":
            status = "current"
            label = "所需憑證已安全設定"
            eta = _complete_eta("憑證存在性稽核已通過；未讀出或發布金鑰值。")
        else:
            status = "unavailable"
            label = f"憑證狀態：{state}（{configured}/{required}）"
            eta = _unknown_eta("blocked", "缺少必要憑證，相關端點不會啟動。")
        rows.append(
            {
                "id": f"credential:{credential_id}",
                "parent_id": (
                    "group:openbb-compact"
                    if is_openbb
                    else "group:tw-minute-source-cold"
                    if credential_id == "shioaji"
                    else "group:free-public-context"
                ),
                "scope": "credential_gate",
                "title": f"{provider} · API 憑證",
                "provider": provider,
                "category": "credential_readiness",
                "status": status,
                "status_label": label,
                "cadence": "每次資料更新前重新稽核",
                "update_owner": "非機密憑證存在性稽核",
                "latest_at_utc": _iso(generated_at),
                "data_through": None,
                "freshness": freshness,
                "coverage": _coverage(
                    configured,
                    required,
                    unit="必要欄位",
                    label="憑證完整度",
                ),
                "eta": eta,
                "rows": None,
                "publishable": True,
                "automation_eligible": False,
                "credential_state": state,
                "detail": (
                    "此列只呈現 configured/partial/missing 與欄位計數；"
                    "API key、secret、token 值永不進入公開 payload。"
                ),
                "warnings": ["憑證就緒不等於免費方案具有資料權限或足夠配額。"],
                "detail_link": None,
            }
        )
    return rows


def _product_granularity_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Flatten the canonical daily/1m/tick contract into auditable rows."""

    registry = _read_json(root / "configs/data_product_granularities.json", {})
    products = registry.get("products", []) if isinstance(registry, Mapping) else []
    credentials = _credential_states(root)
    rows: list[dict[str, Any]] = []
    windows = {"daily": 72 * 3600, "1m": 6 * 3600, "tick": 10 * 60}
    for product in products:
        if not isinstance(product, Mapping):
            continue
        product_id = str(product.get("id") or "").strip()
        product_title = str(product.get("title") or product_id)
        provider = str(product.get("provider") or "其他")
        granularities = product.get("granularities", [])
        if not product_id or not isinstance(granularities, list):
            continue
        for spec in granularities:
            if not isinstance(spec, Mapping):
                continue
            grain = str(spec.get("granularity") or "").strip()
            if grain not in {"daily", "1m", "tick"}:
                continue
            implementation = str(spec.get("implementation") or "registered")
            availability = str(spec.get("availability") or "unspecified")
            parent = str(spec.get("parent_dataset") or "")
            storage_relative = str(spec.get("storage_path") or "")
            summary_relative = str(spec.get("summary_path") or "")
            progress_relative = str(spec.get("progress_path") or "")
            storage_path = root / storage_relative if storage_relative else None
            summary_path = root / summary_relative if summary_relative else None
            progress_path = root / progress_relative if progress_relative else None
            summary = _read_json(summary_path, {}) if summary_path else {}
            progress = _read_json(progress_path, {}) if progress_path else {}
            latest = _latest_time(
                [
                    path
                    for path in (summary_path, progress_path)
                    if path is not None and path.exists()
                ],
                [
                    payload
                    for payload in (summary, progress)
                    if isinstance(payload, Mapping) and payload
                ],
            )
            data_through = (
                _extract_data_through([summary])
                if isinstance(summary, Mapping)
                else None
            )
            data_through_time = _parse_time(data_through)
            if data_through_time is not None:
                latest = data_through_time
            freshness = _freshness(
                latest,
                now=now,
                window_seconds=windows[grain],
            )
            counts = _status_counts([summary]) if isinstance(summary, Mapping) else {}
            total = sum(counts.values())
            failures = sum(
                count
                for status_name, count in counts.items()
                if any(
                    token in status_name.lower()
                    for token in ("fail", "error", "partial", "mismatch")
                )
            )
            completed = max(0, total - failures)
            storage_present = storage_path is not None and storage_path.exists()
            # A directory proves only that storage exists.  It says nothing
            # about date/symbol completeness, so it must never create a 1/1
            # progress denominator.
            coverage = (
                _coverage(completed, total, unit="項", label="最近批次")
                if total
                else None
            )
            credential_id = str(spec.get("credential_id") or "")
            credential = credentials.get(credential_id) if credential_id else None
            credential_state = (
                str(credential.get("state") or "missing")
                if isinstance(credential, Mapping)
                else "unknown"
                if credential_id
                else "not_required"
            )
            implemented = implementation.startswith(
                "implemented"
            ) or implementation.startswith("reused_existing")
            registered_only = implementation.startswith("registered")
            unsupported = implementation == "not_available"
            deferred = (
                implementation.startswith("deferred")
                or spec.get("acquisition_enabled") is False
            )
            warnings: list[str] = []
            if deferred:
                status = "deferred"
                exchange_scope = "exchange_scope" in implementation
                status_label = (
                    "依目前交易所範圍延後"
                    if exchange_scope
                    else "依目前 1m-only 範圍延後"
                )
                eta = _not_applicable_eta(
                    "deferred",
                    "目前只啟用 Binance、OKX、Bybit；此交易所不會消耗流量或容量。"
                    if exchange_scope
                    else "逐筆／委託簿資料未排程；不會消耗流量或容量。",
                )
                automation_eligible = False
                coverage = None
                warnings.append(
                    "既有資料保留；只有使用者明確擴大交易所範圍後才可續抓。"
                    if exchange_scope
                    else "既有逐筆資料保留；只有使用者明確重新啟用後才可續抓。"
                )
            elif credential_id and credential_state != "configured":
                status = "unavailable"
                status_label = f"憑證狀態：{credential_state}"
                eta = _unknown_eta("blocked", "所需 API 憑證尚未完整設定。")
                automation_eligible = False
                warnings.append("面板只顯示憑證是否存在，不公開任何 API key 值。")
            elif unsupported:
                status = "unavailable"
                status_label = "來源沒有可驗證的逐筆歷史契約"
                eta = _unknown_eta("blocked", "此供應商不提供該資料粒度。")
                automation_eligible = False
            elif registered_only:
                status = "waiting"
                status_label = "已註冊但尚未接入可執行管線"
                eta = _unknown_eta(
                    "waiting_schedule",
                    "需要來源權限、容量或 materializer 完成後才有可量測 ETA。",
                )
                automation_eligible = False
            elif failures:
                status = "degraded"
                status_label = f"最近批次有 {failures:,} 個失敗／部分完成項"
                eta = _unknown_eta("waiting_schedule", "等待下一輪精確重試。")
                automation_eligible = implemented
            elif implemented and latest is not None:
                status = "current" if freshness["state"] == "current" else "stale"
                status_label = "已到最新" if status == "current" else "需要補到最新"
                eta = (
                    _complete_eta("最近資料與摘要通過新鮮度檢查。")
                    if status == "current"
                    else _unknown_eta("waiting_schedule", "等待下一輪增量或回補。")
                )
                automation_eligible = True
            elif implemented:
                status = "waiting"
                status_label = "管線已實作，等待第一份可驗證回執"
                eta = _unknown_eta("waiting_schedule", "首次批次尚未產生吞吐率。")
                automation_eligible = True
            else:
                status = "unavailable"
                status_label = "未知的實作契約"
                eta = _unknown_eta("blocked", "註冊資料不完整。")
                automation_eligible = False

            if not deferred and isinstance(progress, Mapping) and progress:
                progress_updated = _parse_time(progress.get("updated_at_utc"))
                progress_age = (
                    max(0.0, (now - progress_updated).total_seconds())
                    if progress_updated is not None
                    else None
                )
                if (
                    str(progress.get("state") or "").lower() == "running"
                    and progress_age is not None
                    and progress_age <= 15 * 60
                ):
                    status = "updating"
                    status_label = str(progress.get("label") or "資料更新正在執行")
                    coverage = _coverage(
                        progress.get("current"),
                        progress.get("total"),
                        unit=str(progress.get("unit") or "項"),
                        label="目前批次",
                    )
                    remaining = _integer(progress.get("remaining_seconds"))
                    eta = {
                        "state": "estimating"
                        if remaining is not None
                        else "warming_up",
                        "remaining_seconds": remaining,
                        "estimated_complete_at_utc": progress.get(
                            "estimated_complete_at_utc"
                        ),
                        "confidence": "low"
                        if remaining is not None
                        else "not_available",
                        "basis": str(
                            progress.get("basis") or "依完整批次的實測吞吐率線性外推。"
                        ),
                    }
                    latest = progress_updated
            if grain == "tick":
                warnings.append("Tick 必須是真實成交／報價事件；不以 1 分鐘 K 線冒充。")
            if "rolling" in availability or "recent" in availability:
                warnings.append("來源歷史範圍有限；完整度只在官方可取得邊界內成立。")
            rows.append(
                {
                    "id": f"product:{product_id}:{grain}",
                    "parent_id": f"group:{parent}" if parent else None,
                    "scope": "product_granularity",
                    "title": f"{product_title} · {grain}",
                    "provider": provider,
                    "category": "market_product_granularity",
                    "granularity": grain,
                    "native_granularity": grain,
                    "availability": availability,
                    "implementation": implementation,
                    "credential_state": credential_state,
                    "status": status,
                    "status_label": status_label,
                    "cadence": str(spec.get("cadence") or "依來源排程"),
                    "update_owner": f"{provider} {grain} 管線",
                    "latest_at_utc": _iso(latest),
                    "data_through": data_through,
                    "freshness": freshness,
                    "coverage": coverage,
                    "eta": eta,
                    "rows": _extract_rows([summary])
                    if isinstance(summary, Mapping)
                    else None,
                    "publishable": True,
                    "automation_eligible": automation_eligible,
                    "acquisition_enabled": not deferred,
                    "stream_contract": bool(spec.get("stream")),
                    "storage_present": storage_present,
                    "completion_receipt_present": bool(
                        summary_path is not None
                        and summary_path.exists()
                        and isinstance(summary, Mapping)
                        and summary
                    ),
                    "detail": (
                        f"availability={availability}; implementation={implementation}; "
                        f"storage={storage_relative or 'none'}"
                    ),
                    "warnings": warnings,
                    "detail_link": None,
                }
            )
    return rows


def _crypto_acquisition_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Expose the first-principles crypto fact registry and its unique owner."""

    registry = _read_json(root / "configs/crypto_data_acquisition.json", {})
    facts = registry.get("datasets", []) if isinstance(registry, Mapping) else []
    source_status = _read_json(root / "data_crypto_reference/source_status.json", {})
    provider_rows = (
        source_status.get("providers", []) if isinstance(source_status, Mapping) else []
    )
    dataset_rows = (
        source_status.get("datasets", []) if isinstance(source_status, Mapping) else []
    )
    free_manifest = _read_json(root / "data_free_public/download_manifest.json", {})
    free_dataset_rows = (
        free_manifest.get("results", []) if isinstance(free_manifest, Mapping) else []
    )
    providers = {
        str(item.get("credential_id")): item
        for item in provider_rows
        if isinstance(item, Mapping) and item.get("credential_id")
    }
    evidence = {
        str(item.get("dataset")): item
        for item in [*dataset_rows, *free_dataset_rows]
        if isinstance(item, Mapping) and item.get("dataset")
    }
    fact_evidence_ids = {
        "aggregate_asset_identity": ["coingecko_asset_catalog"],
        "aggregate_market_cap_supply": ["coingecko_market_snapshot"],
        "ethereum_realtime_gas": [
            "blockscout_ethereum_gas",
            "blockscout_ethereum_latest_block",
        ],
        "stablecoin_supply_and_peg": ["defillama_stablecoins"],
        "defi_tvl": ["defillama_chains"],
        "dex_derivatives_options_volume": [
            "defillama_dex_volume",
            "defillama_options_notional_volume",
            "defillama_open_interest",
        ],
        "protocol_fees_revenue": [
            "defillama_protocol_fees",
            "defillama_protocol_revenue",
        ],
        "defi_yields_and_borrow_rates": ["defillama_yields"],
        "bitcoin_mempool_and_fees": [
            "bitcoin_mempool_fees",
            "bitcoin_mempool_state",
            "bitcoin_difficulty_adjustment",
            "bitcoin_hashrate_history",
        ],
        "sentiment_indices": ["alternative_me_fear_greed"],
        "options_chain_greeks_iv": [
            "deribit_btc_options",
            "deribit_eth_options",
        ],
    }
    fact_progress_paths = {
        "venue_instrument_lifecycle": root / "data_binance_archive/progress.json",
        "venue_spot_ohlcv_1m": root / "data_binance_archive/progress.json",
        "venue_dated_futures_ohlcv_1m": root / "data_binance_archive/progress.json",
    }
    provider_paths = {
        "Binance": [
            root / "data_binance/1m/download_summary.json",
            root / "data_binance/download_summary.json",
            root / "data_binance/progress.json",
            root / "data_binance_archive/download_summary.json",
            root / "data_binance_archive/plan_summary.json",
            root / "data_binance_archive/capacity_receipt.json",
            root / "data_binance_archive/progress.json",
        ],
        "OKX": [
            root / "data_okx/1m/download_summary.json",
            root / "data_okx/download_summary.json",
            root / "data_okx/progress.json",
        ],
        "Bybit": [
            root / "data_bybit/1m/download_summary.json",
            root / "data_bybit/download_summary.json",
            root / "data_bybit/progress.json",
        ],
        "DefiLlama": [
            root / "data_free_public/download_summary.json",
            root / "data_free_public/download_manifest.json",
        ],
        "Coin Metrics": [root / "data_coinmetrics_community/download_summary.json"],
        "mempool.space": [root / "data_free_public/download_manifest.json"],
        "Deribit": [root / "data_free_public/download_manifest.json"],
        "Alternative.me": [root / "data_free_public/download_manifest.json"],
        "Blockscout": [root / "data_free_public/download_manifest.json"],
        "OpenBB": [root / "data_openBB/_state/monitor_latest.json"],
    }
    output: list[dict[str, Any]] = []
    for fact in facts if isinstance(facts, list) else []:
        if not isinstance(fact, Mapping) or not fact.get("id"):
            continue
        fact_id = str(fact["id"])
        title = str(fact.get("title") or fact_id)
        implementation = str(fact.get("implementation") or "registered_pending")
        priority = str(fact.get("priority") or "P2")
        score = _integer(fact.get("score")) or 0
        owners = [
            str(item) for item in fact.get("canonical_owners", []) if str(item).strip()
        ]
        fallbacks = [
            str(item) for item in fact.get("fallbacks", []) if str(item).strip()
        ]
        credential_id = str(fact.get("credential_id") or "")
        provider_state = providers.get(credential_id) if credential_id else None
        expected_evidence_ids = fact_evidence_ids.get(fact_id, [])
        direct_items = [
            evidence[dataset_id]
            for dataset_id in expected_evidence_ids
            if dataset_id in evidence
        ]
        evidence_current = sum(
            str(item.get("status")) in {"updated", "current_cached"}
            for item in direct_items
        )
        evidence_paths = [
            path
            for owner in owners
            for token, paths in provider_paths.items()
            if token.lower() in owner.lower()
            for path in paths
        ]
        latest = _latest_time(
            evidence_paths,
            [],
        )
        direct_times = [
            _parse_time(item.get("observed_at_utc"))
            for item in direct_items
            if isinstance(item, Mapping)
        ]
        direct_times = [value for value in direct_times if value is not None]
        if direct_times:
            latest = max(direct_times)
        window = (
            26 * 3600
            if "snapshot" in str(fact.get("native_granularity"))
            else 72 * 3600
        )
        fresh = _freshness(latest, now=now, window_seconds=window)
        coverage = (
            _coverage(
                evidence_current,
                len(expected_evidence_ids),
                unit="資料集",
                label="唯一主來源落盤",
            )
            if expected_evidence_ids
            else None
        )
        rows = (
            sum(
                _integer(item.get("rows") or item.get("observations_added")) or 0
                for item in direct_items
            )
            if direct_items
            else None
        )
        status: str
        status_label: str
        eta: dict[str, Any]
        automation_eligible = False
        operational = (
            str(provider_state.get("operational_state") or "")
            if isinstance(provider_state, Mapping)
            else ""
        )
        blocked_provider = operational in {
            "invalid_credential",
            "not_entitled",
            "quota_exhausted",
            "unavailable",
        }
        evidence_complete = bool(expected_evidence_ids) and evidence_current == len(
            expected_evidence_ids
        )
        deferred = (
            implementation.startswith("deferred")
            or fact.get("acquisition_enabled") is False
        )
        if deferred:
            status = "deferred"
            status_label = "依目前 1m-only 範圍延後"
            eta = _complete_eta("不啟動逐筆、報價簿、L2/L3 或強平事件取得。")
            automation_eligible = False
        elif (
            evidence_complete
            and implementation.startswith("implemented")
            and not any(
                token in implementation
                for token in ("partial", "only", "pending", "requires", "blocked")
            )
        ):
            status = "current" if fresh["state"] == "current" else "stale"
            status_label = (
                "唯一主來源已落盤" if status == "current" else "唯一主來源需要更新"
            )
            eta = (
                _complete_eta("最近唯一主來源回執已成功。")
                if status == "current"
                else _unknown_eta(
                    "waiting_schedule", "等待 cadence receipt 到期後補到最新。"
                )
            )
            automation_eligible = True
        elif (
            blocked_provider
            and fact_id != "venue_liquidations"
            and not (implementation.startswith("implemented") and latest is not None)
        ):
            status = "unavailable"
            status_label = f"設定 API 不可用：{operational}"
            eta = _unknown_eta("blocked", "憑證存在不代表有效或具有端點權限。")
        elif implementation.startswith("blocked"):
            status = "unavailable"
            status_label = "已驗證阻擋條件"
            eta = _unknown_eta("blocked", "必須先修復金鑰或取得免費可用替代來源。")
        elif implementation.startswith("reused_existing"):
            status = "current" if latest is not None else "waiting"
            status_label = (
                "沿用既有可稽核管線" if latest is not None else "等待既有管線回執"
            )
            eta = (
                _complete_eta("既有專用資料管線負責更新。")
                if latest is not None
                else _unknown_eta("waiting_schedule", "尚未找到對應回執。")
            )
            automation_eligible = True
        elif implementation.startswith("implemented"):
            partial = any(
                token in implementation
                for token in ("partial", "only", "pending", "requires", "blocked")
            )
            if latest is None:
                status = "waiting"
                status_label = "已實作，等待第一份可驗證回執"
                eta = _unknown_eta("waiting_schedule", "首批吞吐率尚不可量測。")
            elif partial:
                status = "degraded"
                status_label = "部分場館／歷史已實作，剩餘缺口仍在清冊"
                eta = _unknown_eta("waiting_schedule", "不同場館與歷史邊界需分別完成。")
            else:
                status = "current" if fresh["state"] == "current" else "stale"
                status_label = (
                    "已由唯一主來源維護" if status == "current" else "需要補到最新"
                )
                eta = (
                    _complete_eta("最近來源回執在允許的新鮮度內。")
                    if status == "current"
                    else _unknown_eta("waiting_schedule", "等待下一輪更新。")
                )
            automation_eligible = True
        else:
            status = "waiting"
            status_label = "價值與主來源已確定，下載器尚待完成"
            eta = _unknown_eta(
                "waiting_schedule",
                "未執行的工作不以零吞吐率捏造 ETA。",
            )
        fact_progress_path = fact_progress_paths.get(fact_id)
        fact_progress = (
            _read_json(fact_progress_path, {}) if fact_progress_path is not None else {}
        )
        if (
            not deferred
            and isinstance(fact_progress, Mapping)
            and str(fact_progress.get("state") or "").lower() == "running"
        ):
            progress_updated = _parse_time(fact_progress.get("updated_at_utc"))
            progress_age = (
                max(0.0, (now - progress_updated).total_seconds())
                if progress_updated is not None
                else None
            )
            if progress_age is not None and progress_age <= 15 * 60:
                remaining = _integer(fact_progress.get("remaining_seconds"))
                progress_phase = str(fact_progress.get("phase") or "")
                status = "updating"
                status_label = (
                    "唯一主來源正在建立精確容量計畫"
                    if progress_phase == "discover"
                    else "唯一主來源正在回補"
                )
                eta = {
                    "state": "estimating" if remaining is not None else "warming_up",
                    "remaining_seconds": remaining,
                    "estimated_complete_at_utc": fact_progress.get(
                        "estimated_complete_at_utc"
                    ),
                    "confidence": "low" if remaining is not None else "not_available",
                    "basis": (
                        "僅為官方目錄規劃階段 ETA；完整下載 ETA 尚未可量測。"
                        if progress_phase == "discover"
                        else str(fact_progress.get("basis") or "依目前完成吞吐率估計。")
                    ),
                    "phase": progress_phase or None,
                }
                automation_eligible = True
        owner_text = " / ".join(owners) if owners else "待指定"
        detail = (
            f"{fact.get('mechanism') or ''} primary={owner_text}; "
            f"fallback={' / '.join(fallbacks) if fallbacks else 'none'}"
        )
        warnings = [
            f"去重鍵：{' + '.join(str(item) for item in fact.get('dedup_key', []))}",
            "不同交易所的同一幣種是不同市場，不互相去重。",
        ]
        if deferred:
            warnings.append(
                str(fact.get("deferred_reason") or "依使用者決策延後；既有資料不刪除。")
            )
        if blocked_provider:
            warnings.append(
                str(provider_state.get("message") or operational)
                if isinstance(provider_state, Mapping)
                else operational
            )
        output.append(
            {
                "id": f"crypto-fact:{fact_id}",
                "parent_id": "group:crypto-reference",
                "scope": "crypto_fact_family",
                "title": f"{priority} · {title}",
                "provider": owner_text,
                "category": "crypto_canonical_fact",
                "availability": f"priority={priority}; value_score={score}/10; implementation={implementation}",
                "status": status,
                "status_label": status_label,
                "cadence": str(fact.get("native_granularity") or "依來源事件"),
                "update_owner": f"唯一主來源：{owner_text}",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": coverage,
                "eta": eta,
                "rows": rows,
                "publishable": True,
                "automation_eligible": automation_eligible,
                "acquisition_enabled": not deferred,
                "priority": priority,
                "value_score": score,
                "dedup_key": list(fact.get("dedup_key", [])),
                "credential_state": (
                    str(provider_state.get("credential_state"))
                    if isinstance(provider_state, Mapping)
                    else "not_required"
                ),
                "credential_operational_state": operational or "not_required",
                "detail": detail,
                "warnings": warnings,
                "detail_link": None,
            }
        )
    return output


def _finlab_receipt_file_exists(finlab_root: Path, data_path: str) -> bool:
    """Check the ordinary flat dataset grain without repeated path resolution.

    The directory and leaf must both reject symlinks on the fast path. Older
    nested paths or links within the enrolled root retain the existing resolved
    containment check; a link outside that root is never accepted.
    """

    relative = Path(data_path)
    if relative.is_absolute() or relative.parts[:1] != ("datasets",):
        return False
    if (
        len(relative.parts) == 2
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
    ):
        try:
            directory_fd = os.open(
                finlab_root / "datasets",
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
        except OSError:
            pass
        else:
            try:
                try:
                    mode = os.stat(
                        relative.parts[1], dir_fd=directory_fd,
                        follow_symlinks=False,
                    ).st_mode
                except FileNotFoundError:
                    return False
                opened_directory = os.fstat(directory_fd)
                current_directory = os.stat(
                    finlab_root / "datasets", follow_symlinks=False,
                )
                if (
                    opened_directory.st_dev == current_directory.st_dev
                    and opened_directory.st_ino == current_directory.st_ino
                ):
                    if stat.S_ISREG(mode):
                        return True
                    if not stat.S_ISLNK(mode):
                        return False
            except (OSError, ValueError):
                pass
            finally:
                os.close(directory_fd)
    resolved = (finlab_root / relative).resolve()
    return resolved.is_relative_to(finlab_root) and resolved.is_file()


def _finlab_candidate_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Expose every discovered key without treating catalog membership as data."""

    catalog = _read_json(root / "configs/finlab_history_candidates.json", {})
    candidates = catalog.get("datasets", []) if isinstance(catalog, Mapping) else []
    discovered = _read_json(root / "data_finlab/catalog/discovery.json", {})
    discovered_keys = discovered.get("keys", []) if isinstance(discovered, Mapping) else []
    receipts_root = root / "data_finlab/receipts"
    attempts_root = root / "data_finlab/attempts"
    intraday_status = _read_json(root / "data_finlab/intraday/status.json", {})
    intraday_by_key = intraday_status.get("by_key", {}) if isinstance(intraday_status, Mapping) else {}
    if not isinstance(intraday_by_key, Mapping):
        intraday_by_key = {}
    by_key: dict[str, Mapping[str, Any]] = {}
    attempts_by_key: dict[str, Mapping[str, Any]] = {}
    if receipts_root.is_dir():
        for path in receipts_root.glob("*.json"):
            receipt = _read_json(path, {})
            if isinstance(receipt, Mapping) and receipt.get("dataset"):
                by_key[str(receipt["dataset"])] = receipt
    if attempts_root.is_dir():
        for path in attempts_root.glob("*.json"):
            attempt = _read_json(path, {})
            if isinstance(attempt, Mapping) and attempt.get("dataset"):
                attempts_by_key[str(attempt["dataset"])] = attempt
    finlab_root = (root / "data_finlab").resolve()
    local_timer_enabled = (
        (root / "deploy/systemd/stockagent-finlab-local-refresh.timer.in").is_file()
        and Path("/etc/systemd/system/timers.target.wants/stockagent-finlab-local-refresh.timer").exists()
    )
    listed = [item for item in candidates if isinstance(item, Mapping)] if isinstance(candidates, list) else []
    listed_keys = {str(item.get("key")) for item in listed if item.get("key")}
    # Discovery is a key inventory, not proof of entitlement or download.
    all_keys = set(by_key) | set(attempts_by_key)
    if isinstance(discovered_keys, list):
        all_keys.update(str(key) for key in discovered_keys if isinstance(key, str) and key)
    for key in sorted(all_keys - listed_keys):
        listed.append({"key": key, "group": key.split(":", 1)[0],
                       "gap": "FinLab SDK 目錄發現項；需另行核對本機語義重複與歷史發布時點"})
    output: list[dict[str, Any]] = []
    for item in listed:
        if not isinstance(item, Mapping) or not item.get("key"):
            continue
        key = str(item["key"])
        receipt = by_key.get(key, {})
        attempt = attempts_by_key.get(key, {})
        data_path = receipt.get("parquet_path") if isinstance(receipt, Mapping) else None
        stored = (
            isinstance(data_path, str)
            and receipt.get("dataset") == key
            and receipt.get("status") == "downloaded_unverified_for_pit"
            and _finlab_receipt_file_exists(finlab_root, data_path)
        )
        count = _integer(receipt.get("rows_with_values")) if stored else None
        first = (receipt.get("first_event_at") or receipt.get("first_non_null_source_index")) if stored else None
        last = (receipt.get("last_event_at") or receipt.get("last_non_null_source_index")) if stored else None
        bounds_basis = (
            f"來源欄 {receipt.get('event_time_column')} 的事件時刻"
            if stored and receipt.get("first_event_at") else "來源索引"
        )
        attempt_status = str(attempt.get("status") or "") if attempt.get("dataset") == key else ""
        configured_deferred_reason = AUTOMATICALLY_DEFERRED_REASONS.get(key)
        deferred_reason = configured_deferred_reason if not stored else None
        deferred = deferred_reason is not None
        partition = intraday_by_key.get(key, {}) if deferred_reason == "requires_date_window" else {}
        if not isinstance(partition, Mapping):
            partition = {}
        partition_count = _integer(partition.get("receipted_partitions")) or 0
        partition_total = _integer(partition.get("requested_weekday_partitions")) or 0
        partition_rows = _integer(partition.get("rows")) or 0
        vip_only = not stored and not deferred and attempt_status == "vip_only"
        failed = not stored and not deferred and attempt_status in {
            "provider_error", "provider_empty", "timed_out",
            "authentication_failed", "quota_exhausted",
        }
        failure_state = {
            "provider_empty": "provider_empty",
            "timed_out": "resource_timeout",
            "authentication_failed": "authentication_failed",
            "quota_exhausted": "quota_wait",
        }.get(attempt_status, "provider_error")
        acquisition_state = (
            "downloaded" if stored else
            "partial_windowed" if deferred_reason == "requires_date_window" and partition_count else
            "deferred_windowed" if deferred_reason == "requires_date_window" else
            "deferred_resource" if deferred else failure_state if failed else
            "vip_only" if vip_only else "pending"
        )
        retry_at = (
            attempt_retry_at(dict(attempt), downloaded=stored)
            if attempt_status and not deferred else None
        )
        status_labels = {
            "oversized_metadata": "來源標籤寬表超出單鍵記憶體預算；待有界擷取方案",
            "oversized_wide_refresh": "來源寬表強制追新超出單鍵記憶體預算；舊版仍保留",
            "oversized_table": "券商整表超出單鍵記憶體與額度預算；待供應商分區介面",
            "requires_date_window": "此鍵必須指定起訖日期；一般整表下載器不適用",
            "resource_timeout": "最近一次 SDK 查詢達單鍵逾時上限；保留冷卻時間",
            "provider_error": "最近一次 SDK 查詢失敗；僅保留錯誤類別，根因未證實",
            "provider_empty": "SDK 有回傳資料框，但沒有任何非空來源值",
            "authentication_failed": "最近一次登入驗證失敗；需檢查本機 session",
            "quota_wait": "最近一次帳號額度不足；重置後再試",
            "vip_only": "此鍵的 SDK 回覆要求 VIP；需核對實際帳號與此鍵授權",
        }
        status_label = (
            status_labels[configured_deferred_reason]
            if stored and configured_deferred_reason else
            "已下載；歷史 PIT 與跨主機使用權限仍待查證" if stored else
            f"已收 {partition_count:,}/{partition_total:,} 個工作日分區；所列期間之前的歷史仍未知"
            if partition_count else
            status_labels.get(deferred_reason or acquisition_state, "尚未下載；列入帳號配額排程")
        )
        output.append({
            "id": f"finlab:{key}",
            "parent_id": "group:finlab-research",
            "scope": "source_registry",
            "title": key,
            "provider": "FinLab",
            "category": str(item.get("group") or "historical_research"),
            "status": (
                "stale" if stored and configured_deferred_reason else
                "legacy" if stored else "partial" if partition_count else "deferred" if deferred
                else "degraded" if failed else "blocked" if vip_only else "waiting"
            ),
            "status_label": status_label,
            "cadence": "每日 08:00 台北時間配額重置後（含休市日）；交易日 08:20–09:10 保護開盤資源",
            "update_owner": "FinLab 歷史下載器",
            "latest_at_utc": receipt.get("fetched_at_utc") if stored else None,
            "data_through": None,
            "freshness": {"state": "unknown", "age_seconds": None},
            "coverage": _coverage(partition_count, partition_total, unit="日分區",
                                  label="已存／所列期間工作日；不代表 FinLab 最早歷史")
                        if partition_count and partition_total else None,
            "eta": _not_applicable_eta("reference", "逐鍵下載狀態由 FinLab 群組計算；沒有逐鍵可信 ETA。"),
            "rows": count if stored else partition_rows if partition_count else None,
            "publishable": False,
            "automation_eligible": True,
            "acquisition_enabled": local_timer_enabled,
            "registry_alias": True,
            "finlab_acquisition_state": acquisition_state,
            "finlab_deferred_reason": configured_deferred_reason,
            "finlab_attempt_status": attempt_status or None,
            "finlab_provider_rows": _integer(attempt.get("provider_rows")) if attempt_status == "provider_empty" else None,
            "finlab_provider_fields": _integer(attempt.get("provider_fields")) if attempt_status == "provider_empty" else None,
            "finlab_intraday_partitions": dict(partition) if partition_count else None,
            "finlab_last_attempt_at_utc": attempt.get("attempted_at_utc") if attempt_status else None,
            "finlab_next_retry_at_utc": _iso(retry_at),
            "detail": str(item.get("gap") or "FinLab 歷史候選資料集"),
            "warnings": [
                "FinLab 目錄與 FAQ 不等於帳號授權；舊 Free 快取曾只到 2018 年底，升級後以逐項雲端刷新回執為準。",
                "收據索引是來源期別或時間，不等於盤前可用日；原始值與修訂版本尚未完成 PIT 驗證。",
                *(["最近一次官方 SDK 回覆 VIP only；目錄鍵名不代表目前可下載。"] if vip_only else []),
                *(["舊版仍在；高記憶體強制追新已暫緩，不能視為目前最新。"] if stored and configured_deferred_reason else []),
                *(["FinLab 官方 intraday 歷史仍在陸續上架；日期分區完成不等於全史完整。"] if partition_count else []),
            ],
            "detail_link": item.get("url"),
            "record_stats": {
                "count": count if stored else partition_rows if partition_count else None,
                "first": first if stored else partition.get("first_data_date"),
                "last": last if stored else partition.get("last_data_date"),
                "files_inspected": 1 if stored else partition_count,
                "files_total": 1 if stored else partition_total if partition_count else 0,
                "state": "receipt_only_not_hash_reverified" if stored else
                         "partition_receipts_not_full_history" if partition_count else
                         "vip_only" if vip_only else "not_downloaded",
                "basis": (
                    "FinLab 日期分區收據：僅所列期間與已上架日；不是全部歷史或 PIT 驗證。"
                    if partition_count else
                    f"FinLab 下載收據：{bounds_basis}首末與非空列數；不是交易可用日或當前檔案雜湊重驗。"
                ),
            },
        })
    market_status = _read_json(root / "data_finlab/intraday/market_status.json", {})
    market_kinds = market_status.get("by_kind", {}) if isinstance(market_status, Mapping) else {}
    if isinstance(market_kinds, Mapping):
        for kind in ("tw_minute", "tw_tick"):
            facts = market_kinds.get(kind)
            if not isinstance(facts, Mapping):
                continue
            total = _integer(facts.get("target_partitions")) or 0
            received = _integer(facts.get("receipted_partitions")) or 0
            output.append({
                "id": f"finlab-market:{kind}",
                "parent_id": "group:finlab-research",
                "scope": "source_registry",
                "title": ("FinLab 全市場 Tick 合成分鐘" if kind == "tw_minute"
                          else "FinLab 全市場原始 Tick"),
                "provider": "FinLab",
                "category": "tw_stock_intraday_market",
                "status": "partial" if received else "waiting",
                "status_label": f"已收 {received:,}/{total:,} 個歷史候選股日分區；來源可供性未證實",
                "cadence": "帳號額度重置後持續補；交易日開盤保護窗暫停",
                "update_owner": "FinLab 全市場逐檔下載器",
                "latest_at_utc": market_status.get("observed_at_utc"),
                "data_through": facts.get("last_data_date"),
                "freshness": {"state": "unknown", "age_seconds": None},
                "coverage": _coverage(received, total, unit="股日分區",
                                      label="歷史候選分區；不代表 FinLab 已上架") if total else None,
                "eta": _not_applicable_eta("reference", "FinLab 上架範圍與帳號流量成本未知，無可信全市場 ETA。"),
                "rows": _integer(facts.get("rows")),
                "publishable": False,
                "automation_eligible": True,
                "acquisition_enabled": local_timer_enabled,
                "registry_alias": True,
                "detail": ("只從已存 FinLab Tick 在本機合成，不另呼叫分鐘 API；逐檔表見 FinLab 面板。"
                           if kind == "tw_minute" else
                           "官方現有＋歷史下市股票名單，依觀測交易日與個股生命週期列候選；逐檔表見 FinLab 面板。"),
                "warnings": [
                    "清冊分母是待查詢候選，不是來源已提供的完整歷史。",
                    "下市前資料、未上架分區和 2004 年以前交易日仍需個別驗證。",
                ],
                "detail_link": "/finlab/#market-title",
                "record_stats": {
                    "count": _integer(facts.get("rows")),
                    "first": facts.get("first_data_date"),
                    "last": facts.get("last_data_date"),
                    "files_inspected": received,
                    "files_total": total,
                    "state": "partition_receipts_not_full_history",
                    "basis": "FinLab 個股交易日收據索引；尚非全面檔案雜湊重驗或訓練可用性證明。",
                },
            })
    daily = market_status.get("daily_price_coverage", {}) if isinstance(market_status, Mapping) else {}
    if isinstance(daily, Mapping) and daily.get("state") == "parquet_footer_nonnull_counts":
        total = _integer(market_status.get("universe_symbols")) or 0
        received = _integer(daily.get("symbols_with_values")) or 0
        output.append({
            "id": "finlab-market:daily-close-symbols",
            "parent_id": "group:finlab-research",
            "scope": "source_registry",
            "title": "FinLab 日收盤價逐股覆蓋",
            "provider": "FinLab",
            "category": "tw_stock_daily_market",
            "status": "partial" if received < total else "legacy",
            "status_label": f"非空股票欄 {received:,}/{total:,}；缺 {max(0, total - received):,} 檔",
            "cadence": "FinLab 日價表依帳號額度重置後追新；逐股欄位由本機收據核對",
            "update_owner": "FinLab 歷史下載器",
            "latest_at_utc": market_status.get("observed_at_utc"),
            "data_through": daily.get("source_last_index"),
            "freshness": {"state": "unknown", "age_seconds": None},
            "coverage": _coverage(received, total, unit="股票代號",
                                  label="已下載價表的非空股票欄，不代表全史") if total else None,
            "eta": _not_applicable_eta("reference", "FinLab 缺的歷史下市股不保證來源有資料。"),
            "rows": None,
            "publishable": False,
            "automation_eligible": True,
            "acquisition_enabled": local_timer_enabled,
            "registry_alias": True,
            "detail": "FinLab 寬表按 Parquet 頁尾非空列數逐股核對；另有本機官方日線的缺項不算 FinLab 已取得。",
            "warnings": [
                f"FinLab 缺的 {daily.get('symbols_missing')} 檔中，本機另有日線 {daily.get('missing_but_local_daily')} 檔；兩邊都缺 {daily.get('missing_both_sources')} 檔。",
                "欄位有值不代表每個交易日完整、發布時點正確或適合歷史 PIT 訓練。",
            ],
            "detail_link": "/finlab/#market-title",
            "record_stats": {
                "count": received,
                "first": daily.get("source_first_index"),
                "last": daily.get("source_last_index"),
                "files_inspected": 1,
                "files_total": 1,
                "state": "parquet_footer_nonnull_symbol_count_not_full_history",
                "basis": "FinLab price:收盤價當前 Parquet 逐股非空列數，不是完整交易日審計。",
            },
        })
    return output


def _finlab_acquisition_status(
    root: Path,
    sources: list[Mapping[str, Any]],
    *,
    service: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Measure catalog acquisition, not historical/PIT completeness."""

    discovery = _read_json(root / "data_finlab/catalog/discovery.json", {})
    keys = discovery.get("keys") if isinstance(discovery, Mapping) else None
    catalog_keys = (
        {key for key in keys if isinstance(key, str) and key}
        if isinstance(keys, list) else None
    )
    selected = [
        row for row in sources
        if catalog_keys is None or str(row.get("id") or "").removeprefix("finlab:") in catalog_keys
    ]
    downloaded = sum(row.get("finlab_acquisition_state") == "downloaded" for row in selected)
    states = (
        "pending", "deferred_resource", "deferred_windowed", "partial_windowed",
        "provider_error", "provider_empty",
        "resource_timeout", "vip_only", "authentication_failed", "quota_wait",
    )
    reason_counts = {
        state: sum(row.get("finlab_acquisition_state") == state for row in selected)
        for state in states
    }
    deferred = reason_counts["deferred_resource"]
    failed = sum(reason_counts[state] for state in (
        "provider_error", "provider_empty", "resource_timeout", "vip_only",
        "authentication_failed", "quota_wait",
    ))
    total = len(catalog_keys) if catalog_keys is not None else None
    observed_receipts = [
        parsed for row in selected
        if row.get("finlab_acquisition_state") == "downloaded"
        if (parsed := _parse_time(row.get("latest_at_utc"))) is not None
    ]
    last_receipt = max(observed_receipts) if observed_receipts else None
    recent_downloads = sum(
        timestamp >= now - timedelta(minutes=15) for timestamp in observed_receipts
    )
    run = _read_json(root / "data_finlab/runs/latest.json", {})
    run_finished = _parse_time(run.get("finished_at_utc")) if isinstance(run, Mapping) else None
    quota_fresh = (
        run_finished is not None
        and run_finished.astimezone(TAIPEI).date() == now.astimezone(TAIPEI).date()
    )
    quota_remaining = _number(run.get("quota_remaining_mb")) if quota_fresh else None
    quota_limit = _number(run.get("quota_limit_mb")) if quota_fresh else None
    recorded_margin = _number(run.get("quota_reserve_mb"))
    quota_margin = recorded_margin if recorded_margin is not None and recorded_margin >= 0 else 50.0
    packed_catalog = _read_json(root / "configs/data_sync/packed_datasets.json", {})
    packed_datasets = packed_catalog.get("datasets", []) if isinstance(packed_catalog, Mapping) else []
    finlab_pack = next(
        (entry for entry in packed_datasets
         if isinstance(entry, Mapping) and entry.get("dataset") == "finlab-research"),
        {},
    )
    cold_publish_configured = finlab_pack.get("publish") is True
    running = service.get("active") is True
    timer_active = service.get("timer_active") is True
    if total is None:
        state = "catalog_unknown"
    elif total > 0 and downloaded >= total:
        state = "catalog_downloaded_not_pit_validated"
    elif running:
        state = "running"
    elif quota_remaining is not None and quota_remaining <= quota_margin:
        state = "waiting_quota"
    elif service.get("result") not in {None, "success"}:
        state = "service_failed"
    elif timer_active:
        state = "scheduled"
    else:
        state = "timer_disabled"
    return {
        "state": state,
        "catalog_observed_at_utc": discovery.get("observed_at_utc") if isinstance(discovery, Mapping) else None,
        "catalog_total": total,
        "downloaded": downloaded,
        "not_downloaded": max(0, total - downloaded) if total is not None else None,
        "deferred_resource": deferred,
        "not_downloaded_by_reason": reason_counts,
        "failed_or_entitlement": failed,
        "recent_downloaded_15m": recent_downloads,
        "last_receipt_at_utc": _iso(last_receipt),
        "last_receipt_age_seconds": max(0, int((now - last_receipt).total_seconds())) if last_receipt else None,
        "run_finished_at_utc": _iso(run_finished),
        "quota_observed_at_utc": _iso(run_finished) if quota_fresh else None,
        "quota_remaining_mb": quota_remaining,
        "quota_limit_mb": quota_limit,
        "quota_reserve_mb": quota_margin,
        "cold_publish_configured": cold_publish_configured,
        "service_active": running,
        "timer_active": timer_active,
        "next_run_at_utc": service.get("next_run_at_utc"),
        "ratio": downloaded / total if total else None,
        "eta": _unknown_eta(
            "waiting_quota" if state == "waiting_quota" else "running_unmeasured" if running else "waiting_schedule",
            "目錄各鍵大小差異很大且共享每日帳號配額；目前沒有可驗證的全目錄完工 ETA。",
        ),
        "basis": "FinLab SDK 目錄為分母；檔案存在且有下載收據才算已取得。不是歷史完整度、PIT 或冷庫同步率。",
    }


def _finmind_free_sources(
    root: Path, *, now: datetime, service: Mapping[str, Any],
    complement_service: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Project receipt-backed FinMind tasks without scanning daily Parquet."""

    storage = root / "data_finmind"
    summary = _read_json(storage / "status.json", {})
    series = summary.get("series") if isinstance(summary.get("series"), Mapping) else {}
    active = service.get("active") is True
    state = str(summary.get("state") or "not_started")
    rows: list[dict[str, Any]] = []
    labels = {
        "TaiwanStockStatisticsOfOrderBookAndTrade": "全市場委託／成交統計（實測 1 分或 5 秒）",
        "TaiwanVariousIndicators5Seconds": "盤中加權指數（實測 1 分或 5 秒）",
    }
    for dataset, title in labels.items():
        item = series.get(dataset) if isinstance(series.get(dataset), Mapping) else {}
        total = _integer(item.get("total")) or 0
        complete = _integer(item.get("complete")) or 0
        latest = str(item.get("last_complete_date") or "") or None
        date_freshness = _freshness(_parse_time(latest), now=now, window_seconds=72 * 3600)
        if not summary:
            row_state = "waiting"
        elif state.startswith("calendar_") or state == "not_entitled":
            row_state = "degraded"
        elif complete >= total and total:
            row_state = "current" if date_freshness["state"] == "current" else "stale"
        elif active and state in {"running", "backfilling"}:
            row_state = "updating"
        else:
            row_state = "waiting"
        record_count = _integer(item.get("rows"))
        rows.append({
            "id": f"finmind:{dataset}",
            "parent_id": "group:finmind-free",
            "scope": "source_registry",
            "title": title,
            "provider": "FinMind",
            "category": "taiwan_market_intraday",
            "status": row_state,
            "status_label": f"已驗證 {complete:,}/{total:,} 個交易日分區；{state}",
            "cadence": "交易日 14:00 後嘗試追新；歷史按官方速率續補",
            "update_owner": "FinMind 免費資料下載器",
            "latest_at_utc": None,
            "data_through": latest,
            "freshness": date_freshness,
            "coverage": _coverage(complete, total, unit="交易日分區", label="逐日完整格點") if total else None,
            "eta": _unknown_eta(
                "running_unmeasured" if row_state == "updating" else "waiting_schedule",
                "總網路時間至少受 FinMind 每小時配額限制；重試及來源缺口使精確完工時間未知。",
            ),
            "rows": record_count,
            "publishable": False,
            "automation_eligible": active,
            "registry_alias": False,
            "detail": "只有交易所盤中全市場彙總，不是逐檔 L2 或逐筆；原始來源尚非 PIT 訓練特徵。",
            "warnings": ["早期來源實測為 1 分鐘；逐日收據記錄實際格點，不將 1 分鐘偽標為 5 秒。"],
            "record_stats": {
                "count": record_count,
                "first": item.get("first_complete_date"),
                "last": latest,
                "files_inspected": 0,
                "files_total": complete,
                "state": "source_receipts_not_hash_reverified" if complete else "not_downloaded",
                "basis": "逐日下載收據和檔案大小檢查；完整 SHA-256 與 PIT 可用性須另行稽核。",
            },
        })
    calendar = _read_json(storage / "calendar.json", {})
    calendar_dates = calendar.get("dates") if isinstance(calendar.get("dates"), list) else []
    rows.append({
        "id": "finmind:TaiwanStockTradingDate",
        "parent_id": "group:finmind-free", "scope": "source_registry",
        "title": "FinMind 台股交易日曆（含未來預排日）",
        "provider": "FinMind", "category": "calendar",
        "status": "current" if calendar_dates else "waiting",
        "status_label": f"已存 {len(calendar_dates):,} 個日曆日期" if calendar_dates else "等待首次查詢",
        "cadence": "約每 20 小時重新查詢", "update_owner": "FinMind 免費資料下載器",
        "latest_at_utc": calendar.get("observed_at_utc"), "data_through": None,
        "freshness": _freshness(_parse_time(calendar.get("observed_at_utc")), now=now, window_seconds=48 * 3600),
        "coverage": None, "eta": _not_applicable_eta("reference", "日曆不是逐交易日歷史回補工作。"),
        "rows": len(calendar_dates) if calendar_dates else None,
        "publishable": False, "automation_eligible": active, "registry_alias": False,
        "detail": "可能包含未來預排開市日；不是已發生的市場觀測。",
        "warnings": [],
        "record_stats": {
            "count": len(calendar_dates) if calendar_dates else None,
            "first": calendar_dates[0] if calendar_dates else None,
            "last": calendar_dates[-1] if calendar_dates else None,
            "files_inspected": 1 if calendar_dates else 0, "files_total": 1,
            "state": "calendar_with_future_dates" if calendar_dates else "not_downloaded",
            "basis": "FinMind 交易日曆來源；末日可能尚未發生，不代表行情資料最新日。",
        },
    })
    today = now.astimezone(TAIPEI).date()
    master_root = storage / "receipts" / "TaiwanStockInfoWithWarrant"
    try:
        receipt_paths = sorted(master_root.glob("*.json"))
    except OSError:
        receipt_paths = []
    latest_master = _read_json(receipt_paths[-1], {}) if receipt_paths else {}
    master_count = _integer(latest_master.get("rows")) if latest_master.get("status") == "complete" else None
    rows.append({
        "id": "finmind:TaiwanStockInfoWithWarrant",
        "parent_id": "group:finmind-free", "scope": "source_registry",
        "title": "台股及權證主檔每日觀測快照",
        "provider": "FinMind", "category": "security_master",
        "status": "current" if latest_master.get("snapshot_date_taipei") == today.isoformat() and master_count else "stale" if master_count else "waiting",
        "status_label": "已取得最近快照；非歷史上市狀態" if master_count else "等待首次快照",
        "cadence": "每日 14:00 後", "update_owner": "FinMind 免費資料下載器",
        "latest_at_utc": latest_master.get("fetched_at_utc"),
        "data_through": latest_master.get("snapshot_date_taipei"),
        "freshness": _freshness(_parse_time(latest_master.get("fetched_at_utc")), now=now, window_seconds=48 * 3600),
        "coverage": None, "eta": _not_applicable_eta("reference", "主檔每日觀測快照，不是完整歷史日分區。"),
        "rows": master_count, "publishable": False, "automation_eligible": active,
        "registry_alias": False,
        "detail": "當日查詢得到的主檔快照；不得回填成歷史當時已知名單。",
        "warnings": [],
        "record_stats": {
            "count": master_count, "first": latest_master.get("source_first_date"),
            "last": latest_master.get("source_last_date"),
            "files_inspected": 0, "files_total": len(receipt_paths),
            "state": "snapshot_receipt_not_hash_reverified" if master_count else "not_downloaded",
            "basis": "最新每日主檔收據；非歷史 PIT 主檔。",
        },
    })
    return (rows + _finmind_complement_sources(storage, now=now, service=complement_service or {})
            + _finmind_sponsor_sources(storage, now=now))


def _finmind_complement_sources(storage: Path, *, now: datetime,
                                service: Mapping[str, Any]) -> list[dict[str, Any]]:
    complement = _read_json(storage / "complement" / "status.json", {})
    companion_series = complement.get("series") if isinstance(complement.get("series"), Mapping) else {}
    companion_state = str(complement.get("state") or "not_started")
    delegated = set(complement.get("delegated_to_sponsor", [])) if isinstance(complement.get("delegated_to_sponsor"), list) else set()
    companion_time = _parse_time(complement.get("observed_at_utc"))
    companion_fresh = companion_time is not None and (now - companion_time).total_seconds() < 180
    active = service.get("active") is True
    rows: list[dict[str, Any]] = []
    for dataset in FINMIND_COMPLEMENT_DATASETS:
        item = companion_series.get(dataset) if isinstance(companion_series.get(dataset), Mapping) else {}
        total = _integer(item.get("target")) or 0
        complete = _integer(item.get("complete")) or 0
        empty = _integer(item.get("observed_empty")) or 0
        checked = complete + empty
        failed = _integer(item.get("failed")) or 0
        blocked = _integer(item.get("not_entitled")) or 0
        invalid = _integer(item.get("invalid_request")) or 0
        latest = item.get("last_data_date")
        if dataset in delegated:
            row_state = "deferred"
        elif blocked + invalid == total and total:
            row_state = "unavailable"
        elif checked == total and total:
            if dataset in FINMIND_COMPLEMENT_SNAPSHOTS:
                snapshot_time = _parse_time(item.get("last_attempt_at_utc"))
                row_state = "current" if snapshot_time and (now - snapshot_time).total_seconds() < 36 * 3600 else "stale"
            else:
                row_state = "complete"
        elif active and companion_fresh and companion_state == "running":
            row_state = "updating"
        elif failed or invalid or companion_state in {"rate_limited", "not_entitled", "disk_guard", "ip_banned", "invalid_token"}:
            row_state = "degraded"
        else:
            row_state = "waiting"
        record_count = _integer(item.get("rows"))
        rows.append({
            "id": f"finmind:{dataset}", "parent_id": "group:finmind-free",
            "scope": "source_registry", "title": dataset,
            "provider": "FinMind", "category": "cross_market_source",
            "status": row_state,
            "status_label": ("已讓渡給 Sponsor 全市場批量管線；保留既有收據" if dataset in delegated else
                             f"已查驗 {checked:,}/{total:,} 分區（非空 {complete:,}、來源空回 {empty:,}）；失敗 {failed:,}；權限 {blocked:,}；無效請求 {invalid:,}"),
            "cadence": ("本機由法人長表衍生，不呼叫 FinMind API" if dataset == FINMIND_DERIVED_WIDE
                        else "共用每小時額度；依標的／年份分區增量"),
            "update_owner": "FinMind 補充資料下載器",
            "latest_at_utc": item.get("last_attempt_at_utc"),
            "data_through": latest,
            "freshness": _freshness(_parse_time(item.get("last_attempt_at_utc")), now=now, window_seconds=30 * 86400),
            "coverage": _coverage(checked, total, unit="來源分區", label="已查驗來源分區；空回不代表有數值") if total else None,
            "eta": _unknown_eta("running_unmeasured" if row_state == "updating" else "waiting_schedule",
                                "與 FinMind 盤中工作共用額度；空回與來源修訂使完工時間不可精確預測。"),
            "rows": record_count, "publishable": False,
            "automation_eligible": active and dataset not in delegated, "registry_alias": False,
            "detail": ("從 FinMind 法人長表於本機轉成寬表，缺席的歷史類別填 0；非獨立 API 來源，不代表歷史 PIT 或訓練可用。"
                       if dataset == FINMIND_DERIVED_WIDE else
                       "FinMind 原始研究資料；與 FinLab／官方來源分開，不代表歷史 PIT 或訓練可用。"),
            "warnings": (["空回分區未算取得；需逐資料集驗證最早可用日及完整性。"] if empty else [])
                        + (["指定代碼仍遭權限拒絕；已停止自動重試，需核對帳號後手動重排。"] if blocked else [])
                        + (["請求參數遭來源拒絕；已停止自動重試以避免 IP 封鎖。"] if invalid else []),
            "record_stats": {
                "count": record_count, "first": item.get("first_data_date"),
                "last": latest, "files_inspected": 0, "files_total": complete,
                "state": "source_receipts_not_hash_reverified" if complete else "not_downloaded",
                "basis": ("法人長表衍生 Parquet 與收據；公開面板未重算全部檔案雜湊。"
                          if dataset == FINMIND_DERIVED_WIDE else
                          "逐請求 Parquet 與收據；公開面板未重算全部檔案雜湊。"),
            },
        })
    return rows


def _finmind_sponsor_sources(storage: Path, *, now: datetime) -> list[dict[str, Any]]:
    sponsor = _read_json(storage / "sponsor" / "status.json", {})
    series = sponsor.get("series") if isinstance(sponsor.get("series"), Mapping) else {}
    fresh = _parse_time(sponsor.get("observed_at_utc"))
    running = sponsor.get("state") == "running" and fresh is not None and (now - fresh).total_seconds() < 180
    rows: list[dict[str, Any]] = []
    for spec in FINMIND_SPONSOR_SOURCES:
        item = series.get(spec.dataset) if isinstance(series.get(spec.dataset), Mapping) else {}
        total = _integer(item.get("target")) or 0
        complete = _integer(item.get("complete")) or 0
        empty = _integer(item.get("observed_empty")) or 0
        failed = _integer(item.get("failed")) or 0
        blocked = _integer(item.get("blocked")) or 0
        checked = complete + empty
        latest = item.get("last_data_date")
        state = ("unavailable" if total and blocked == total else
                 "complete" if total and checked == total else
                 "updating" if running else
                 "degraded" if blocked or failed else "waiting")
        count = _integer(item.get("rows"))
        rows.append({
            "id": f"finmind:sponsor:{spec.dataset}", "parent_id": "group:finmind-free",
            "scope": "source_registry", "title": f"{spec.dataset}（Sponsor 全市場）",
            "provider": "FinMind", "category": "taiwan_market_sponsor",
            "status": state,
            "status_label": f"已查驗 {checked:,}/{total:,} 分區；非空 {complete:,}、空回 {empty:,}、失敗 {failed:,}、受阻 {blocked:,}",
            "cadence": "按官方實際帳號額度共用節流；全市場日期分區增量",
            "update_owner": "FinMind Sponsor 資料下載器",
            "latest_at_utc": item.get("last_attempt_at_utc"), "data_through": latest,
            "freshness": _freshness(_parse_time(item.get("last_attempt_at_utc")), now=now, window_seconds=30 * 86400),
            "coverage": _coverage(checked, total, unit="來源分區", label="已查驗；空回非資料") if total else None,
            "eta": _unknown_eta("running_unmeasured" if running else "waiting_schedule",
                                "官方額度只是請求下界，回應大小、服務時間及缺口未知。"),
            "rows": count, "publishable": False, "automation_eligible": running,
            "registry_alias": False,
            "detail": "獨立原始收據；與 Free 逐檔任務重疊時先由 Sponsor 批量查詢。尚未驗證 PIT 或可訓練性。",
            "warnings": ["來源空回不算有數值的歷史。"] if empty else [],
            "record_stats": {"count": count, "first": item.get("first_data_date"), "last": latest,
                             "files_inspected": 0, "files_total": complete,
                             "state": "source_receipts_not_hash_reverified" if complete else "not_downloaded",
                             "basis": "不可變 Parquet 與原子收據；公開頁未重算 SHA-256。"},
        })
    return rows


def _free_public_registry_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    registry = _read_json(root / "configs/free_public_data_sources.json", {})
    sources = registry.get("sources", []) if isinstance(registry, Mapping) else []
    manifest = _read_json(root / "data_free_public/download_manifest.json", {})
    manifest_results = (
        manifest.get("results", []) if isinstance(manifest, Mapping) else []
    )
    by_dataset = {
        str(item.get("dataset")): item
        for item in manifest_results
        if isinstance(item, Mapping) and item.get("dataset")
    }
    grouped_result_ids = {
        "hyperliquid_public_context": tuple(
            key for key in by_dataset if key.startswith("hyperliquid_")
        ),
        "deribit_public_context": tuple(
            key for key in by_dataset if key.startswith("deribit_")
        ),
    }
    output: list[dict[str, Any]] = []
    for source in sources if isinstance(sources, list) else []:
        if not isinstance(source, Mapping):
            continue
        source_id = str(source.get("id") or "").strip()
        if not source_id:
            continue
        implementation = str(source.get("implementation_status") or "registered")
        configured_result_ids = source.get("dataset_ids")
        if isinstance(configured_result_ids, list) and configured_result_ids:
            result_ids = tuple(
                str(item).strip() for item in configured_result_ids if str(item).strip()
            )
        else:
            result_ids = grouped_result_ids.get(source_id, (source_id,))
        evidence = [by_dataset[key] for key in result_ids if key in by_dataset]
        summary_path = source.get("summary_path")
        if not evidence and isinstance(summary_path, str) and summary_path.strip():
            external_summary = _read_json(root / summary_path, {})
            external_status_counts = (
                external_summary.get("status_counts", {})
                if isinstance(external_summary, Mapping)
                else {}
            )
            external_rows = _extract_rows([external_summary])
            if external_rows is not None and external_rows > 0:
                external_failed = sum(
                    _integer(external_status_counts.get(key)) or 0
                    for key in ("failed", "error")
                )
                evidence = [
                    {
                        "dataset": source_id,
                        "status": "failed" if external_failed else "updated",
                        "observations_added": external_rows,
                        "observed_at_utc": (
                            external_summary.get("ended_at_utc")
                            or external_summary.get("completed_at_utc")
                        ),
                    }
                ]
                result_ids = (source_id,)
        failures = sum(str(item.get("status")) == "failed" for item in evidence)
        completed = sum(str(item.get("status")) == "updated" for item in evidence)
        expected = len(result_ids) if result_ids else 1
        latest = max(
            (
                parsed
                for item in evidence
                if (parsed := _parse_time(item.get("observed_at_utc"))) is not None
            ),
            default=None,
        )
        fresh = _freshness(latest, now=now, window_seconds=26 * 3600)
        explicitly_deferred = implementation.startswith("deferred_by_user")
        registry_alias = (
            source.get("registry_alias") is True
            or implementation.startswith("reused_existing")
        )
        if explicitly_deferred:
            status = "deferred"
            label = "依目前交易所範圍暫停"
            eta = _unknown_eta(
                "waiting_schedule",
                "目前只啟用 Binance、OKX、Bybit；既有檔案保留但不再自動抓取。",
            )
        elif registry_alias:
            status = "current"
            label = "清冊參照；由既有專用端點維護"
            eta = _not_applicable_eta(
                "reference",
                "此列只保留來源清冊與責任映射，不建立第二份下載工作或進度。",
            )
        elif failures:
            status = "degraded"
            label = f"最近捕捉有 {failures:,} 個資料集失敗"
            eta = _unknown_eta("waiting_schedule", "等待下一輪匿名端點重試。")
        elif evidence and completed == len(evidence):
            status = "current" if fresh["state"] == "current" else "stale"
            label = (
                "已取得且保留觀測版本" if status == "current" else "已取得但需要新快照"
            )
            eta = _complete_eta("最近已登錄的緊湊資料集皆成功落盤。")
        elif any(
            token in implementation for token in ("pending", "gate", "next_backfill")
        ):
            status = "waiting"
            label = "已登錄，等待下載／容量／憑證條件"
            eta = _unknown_eta(
                "waiting_schedule",
                "尚未有執行中批次，不能以零吞吐率捏造 ETA。",
            )
        else:
            status = "waiting"
            label = "已登錄，等待首批可驗證回執"
            eta = _unknown_eta("waiting_schedule", "尚無批次速率可估算。")
        dataset_group = str(source.get("dataset_group") or "free-public-context")
        parent_id = (
            f"group:{dataset_group}"
            if dataset_group in _GROUP_META
            else "group:free-public-context"
        )
        output.append(
            {
                "id": f"free-source:{source_id}",
                "parent_id": parent_id,
                "scope": "source_registry",
                "title": source_id,
                "provider": str(source.get("provider") or "公開來源"),
                "category": str(source.get("category") or "market-context"),
                "status": status,
                "status_label": label,
                "cadence": str(source.get("cadence") or "依來源排程"),
                "update_owner": "來源清冊／對應專用下載器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": (
                    None
                    if registry_alias
                    else _coverage(
                        completed,
                        expected,
                        unit="資料集",
                        label="最近匿名捕捉",
                    )
                ),
                "eta": eta,
                "rows": sum(
                    _integer(item.get("observations_added")) or 0 for item in evidence
                )
                or None,
                "publishable": True,
                "automation_eligible": not registry_alias
                and not explicitly_deferred
                and not any(
                    token in implementation
                    for token in ("pending", "gate", "next_backfill")
                ),
                "acquisition_enabled": not registry_alias and not explicitly_deferred,
                "registry_alias": registry_alias,
                "detail": str(
                    source.get("history_contract")
                    or source.get("feature_status")
                    or "已登錄免費公開來源。"
                ),
                "warnings": (
                    [str(source.get("feature_status"))]
                    if source.get("feature_status")
                    else []
                ),
                "detail_link": None,
            }
        )
    return output


def _calendar_partition_count(start_text: str, end: date, chunk_months: int) -> int:
    try:
        cursor = date.fromisoformat(start_text)
    except ValueError:
        return 0
    count = 0
    while cursor < end:
        absolute = cursor.year * 12 + cursor.month - 1 + max(1, chunk_months)
        year, month0 = divmod(absolute, 12)
        cursor = min(date(year, month0 + 1, 1), end)
        count += 1
    return count


def _crypto_history_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    """Expose every registered Dune query and issuer endpoint with receipt evidence."""

    output: list[dict[str, Any]] = []
    dune_config = _read_json(root / "configs/dune_crypto_queries.json", {})
    dune_summary = _read_json(root / "data_dune_crypto/download_summary.json", {})
    dune_progress = _read_json(root / "data_dune_crypto/progress.json", {})
    dune_results: dict[str, list[Mapping[str, Any]]] = {}
    if isinstance(dune_summary, Mapping):
        for item in dune_summary.get("results", []):
            if not isinstance(item, Mapping):
                continue
            dune_results.setdefault(str(item.get("query_id")), []).append(item)
    dune_run_blocked = isinstance(dune_summary, Mapping) and (
        str(dune_summary.get("state") or "") == "blocked"
        or (_integer(dune_summary.get("blocked_credit_partitions")) or 0) > 0
    )
    dune_subscription_blocked = isinstance(dune_summary, Mapping) and (
        (_integer(dune_summary.get("blocked_subscription_partitions")) or 0) > 0
    )
    progress_updated = (
        _parse_time(dune_progress.get("updated_at_utc"))
        if isinstance(dune_progress, Mapping)
        else None
    )
    progress_live = (
        isinstance(dune_progress, Mapping)
        and str(dune_progress.get("state") or "") == "running"
        and progress_updated is not None
        and (now - progress_updated).total_seconds() <= 15 * 60
    )
    for query in (
        dune_config.get("queries", []) if isinstance(dune_config, Mapping) else []
    ):
        if not isinstance(query, Mapping) or not query.get("enabled"):
            continue
        query_id = str(query.get("id") or "")
        receipts = sorted(
            (root / "data_dune_crypto/receipts" / query_id).glob("*.json")
        )
        receipt_payloads = [_read_json(path, {}) for path in receipts]
        completed = sum(
            isinstance(item, Mapping) and item.get("status") == "complete"
            for item in receipt_payloads
        )
        expected = _calendar_partition_count(
            str(query.get("history_start") or ""),
            now.date(),
            int(query.get("chunk_months") or 3),
        )
        latest = _latest_time(receipts, receipt_payloads)
        query_results = dune_results.get(query_id, [])
        query_statuses = {str(item.get("status") or "") for item in query_results}
        result_status = next(
            (
                status
                for status in (
                    "blocked_subscription",
                    "not_started_subscription",
                    "blocked_credits",
                    "failed",
                    "not_started",
                    "complete",
                )
                if status in query_statuses
            ),
            "",
        )
        if dune_subscription_blocked and query_statuses & {
            "blocked_subscription",
            "not_started_subscription",
        }:
            result_status = "blocked_subscription"
        elif dune_run_blocked and query_statuses & {
            "blocked_credits",
            "not_started",
        }:
            result_status = "blocked_credits"
        query_progress_live = progress_live and query_id in str(
            dune_progress.get("phase") or ""
        )
        fresh = _freshness(latest, now=now, window_seconds=72 * 3600)
        if result_status == "blocked_subscription":
            status, label = "blocked", "Dune 訂閱不支援 API SQL 執行"
            eta = _unknown_eta(
                "external_input",
                "等待可執行 API SQL 的 Dune 訂閱；其餘分區未送出。",
            )
        elif result_status == "blocked_credits":
            status, label = "blocked", "Dune credits 不足，已停止新增執行"
            eta = _unknown_eta(
                "waiting_quota", "HTTP 402 後 fail-closed；等待 credits 恢復。"
            )
        elif result_status in {"failed", "not_started"}:
            status, label = "degraded", "最近分區失敗／尚未啟動"
            eta = _unknown_eta("waiting_schedule", "完成分區保留，下一輪只重試缺口。")
        elif query_progress_live:
            status, label = "updating", "Dune 歷史分區正在回補"
            remaining = _integer(dune_progress.get("remaining_seconds"))
            eta = {
                "state": "estimating" if remaining is not None else "warming_up",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": dune_progress.get(
                    "estimated_complete_at_utc"
                ),
                "confidence": "low" if remaining is not None else "not_available",
                "basis": str(dune_progress.get("basis") or "依已完成分區吞吐外推。"),
            }
        elif expected and completed >= expected and fresh["state"] == "current":
            status, label = "current", "歷史完整且已到最新分區"
            eta = _complete_eta("每個非重疊日曆分區都有完整回執。")
        elif completed:
            status, label = "waiting", f"已完成 {completed:,}/{expected:,} 個分區"
            eta = _unknown_eta(
                "waiting_schedule", "等待每日回補器續跑；未執行時不捏造 ETA。"
            )
        else:
            status, label = "waiting", "已註冊，等待第一個完整分區"
            eta = _unknown_eta("waiting_schedule", "尚無完整分區吞吐率。")
        output.append(
            {
                "id": f"dune-query:{query_id}",
                "parent_id": "group:dune-crypto",
                "scope": "registered_endpoint",
                "title": query_id,
                "provider": "Dune",
                "category": str(query.get("fact_family") or "onchain"),
                "status": status,
                "status_label": label,
                "cadence": "每日；每批 "
                + str(query.get("chunk_months") or 3)
                + " 個月",
                "update_owner": "Dune 版本化 SQL 回補器",
                "latest_at_utc": _iso(latest),
                "data_through": max(
                    (
                        str(item.get("window_end_exclusive"))
                        for item in receipt_payloads
                        if isinstance(item, Mapping)
                    ),
                    default=None,
                ),
                "freshness": fresh,
                "coverage": _coverage(
                    completed, expected, unit="分區", label="歷史分區"
                )
                if expected
                else None,
                "eta": eta,
                "rows": sum(
                    _integer(item.get("rows")) or 0
                    for item in receipt_payloads
                    if isinstance(item, Mapping)
                ),
                "publishable": False,
                "automation_eligible": True,
                "acquisition_enabled": True,
                "detail": str(query.get("description") or "已註冊 Dune SQL 查詢。"),
                "warnings": [
                    "原始 SQL SHA、execution_id、查詢視窗與可用時間均會落盤。",
                    "鏈上地址標籤流量不等於交易所儲備或償付能力證明。",
                ],
                "detail_link": None,
            }
        )

    etf_config = _read_json(root / "configs/crypto_etf_sources.json", {})
    etf_summary = _read_json(root / "data_crypto_etf/download_summary.json", {})
    etf_results = (
        {
            str(item.get("source_id")): item
            for item in etf_summary.get("results", [])
            if isinstance(item, Mapping)
        }
        if isinstance(etf_summary, Mapping)
        else {}
    )
    etf_latest = (
        _parse_time(etf_summary.get("generated_at_utc"))
        if isinstance(etf_summary, Mapping)
        else None
    )
    etf_fresh = _freshness(etf_latest, now=now, window_seconds=72 * 3600)
    sec_results = [
        item for key, item in etf_results.items() if key.startswith("sec_cik_")
    ]
    sec_missing = [
        item
        for key, item in etf_results.items()
        if key.startswith("sec_ticker_")
        and str(item.get("status") or "") == "unavailable_mapping"
    ]
    blocked_sec = etf_results.get("sec_edgar")
    sec_total = (
        (_integer(etf_summary.get("sec_entities")) or len(sec_results))
        + len(sec_missing)
        if isinstance(etf_summary, Mapping)
        else len(sec_results) + len(sec_missing)
    )
    sec_complete = sum(str(item.get("status")) == "complete" for item in sec_results)
    if isinstance(blocked_sec, Mapping):
        sec_status, sec_label = "blocked", "SEC_USER_AGENT 尚未設定"
        sec_eta = _unknown_eta(
            "blocked",
            str(
                blocked_sec.get("message")
                or "SEC fair-access identification is required."
            ),
        )
    elif sec_missing:
        sec_status, sec_label = (
            "degraded",
            f"{len(sec_missing):,} 個 ticker 缺少目前 SEC CIK 映射",
        )
        sec_eta = _unknown_eta(
            "waiting_schedule", "保留缺口，不以錯誤 CIK 或同名公司替代。"
        )
    elif any(str(item.get("status")) in {"failed", "degraded"} for item in sec_results):
        sec_status, sec_label = "degraded", "部分 SEC 實體或原始申報文件失敗"
        sec_eta = _unknown_eta("waiting_schedule", "下一輪會沿用已完成檔案並只補缺口。")
    elif sec_results and sec_complete >= sec_total:
        sec_status = "current" if etf_fresh["state"] == "current" else "stale"
        sec_label = (
            "所有已解析 CIK 已完成"
            if sec_status == "current"
            else "歷史完整但需要增量更新"
        )
        sec_eta = _complete_eta(
            "SEC submissions、companyfacts 與選定 primary documents 已落盤。"
        )
    else:
        sec_status, sec_label = "waiting", "已註冊，等待 SEC 回填"
        sec_eta = _unknown_eta("waiting_schedule", "尚無完整 SEC 實體吞吐率。")
    output.append(
        {
            "id": "crypto-etf:sec-edgar",
            "parent_id": "group:crypto-etf-history",
            "scope": "registered_endpoint",
            "title": "SEC EDGAR submissions / companyfacts / primary filings",
            "provider": "U.S. SEC",
            "category": "regulatory_filings",
            "status": sec_status,
            "status_label": sec_label,
            "cadence": "每日與 filing event",
            "update_owner": "Crypto ETF SEC 回補器",
            "latest_at_utc": _iso(etf_latest),
            "data_through": None,
            "freshness": etf_fresh,
            "coverage": _coverage(sec_complete, sec_total, unit="CIK", label="SEC 實體")
            if sec_total
            else None,
            "eta": sec_eta,
            "rows": sum(_integer(item.get("rows")) or 0 for item in sec_results),
            "publishable": False,
            "automation_eligible": not isinstance(blocked_sec, Mapping),
            "acquisition_enabled": True,
            "detail": "每次以 SEC company_tickers.json 重新解析 ticker 到 CIK，再抓 submissions 全歷史分片、companyfacts 與選定原始申報文件。",
            "warnings": [
                "SEC 不需 API key，但公平存取政策要求可識別的 SEC_USER_AGENT。"
            ],
            "detail_link": None,
        }
    )
    for spec in (
        etf_config.get("issuer_sources", []) if isinstance(etf_config, Mapping) else []
    ):
        if not isinstance(spec, Mapping):
            continue
        source_id = str(spec.get("id") or "")
        result = etf_results.get(source_id)
        receipt = _read_json(
            root / "data_crypto_etf/receipts/issuers" / f"{source_id}.json", {}
        )
        latest = (
            _parse_time(receipt.get("observed_at_utc"))
            if isinstance(receipt, Mapping)
            else None
        )
        fresh = _freshness(
            latest, now=now, window_seconds=None if spec.get("immutable") else 72 * 3600
        )
        raw_status = (
            str(result.get("status") or "") if isinstance(result, Mapping) else ""
        )
        if raw_status == "failed":
            status, label = "degraded", "最近擷取或正規化失敗"
            eta = _unknown_eta("waiting_schedule", "等待每日排程重試。")
        elif receipt:
            status = (
                "current"
                if spec.get("immutable") or fresh["state"] == "current"
                else "stale"
            )
            label = (
                "官方歷史檔已版本化"
                if spec.get("immutable")
                else ("官方日檔已更新" if status == "current" else "需要新日檔")
            )
            eta = _complete_eta("原始 bytes、SHA-256 與正規化結果均已落盤。")
        else:
            status, label = "waiting", "已註冊，等待第一份官方檔案"
            eta = _unknown_eta("waiting_schedule", "尚無第一次成功回執。")
        output.append(
            {
                "id": f"crypto-etf:issuer:{source_id}",
                "parent_id": "group:crypto-etf-history",
                "scope": "registered_endpoint",
                "title": f"{spec.get('ticker')} · {source_id}",
                "provider": str(spec.get("provider") or "ETF issuer"),
                "category": str(spec.get("adapter") or "issuer_history"),
                "status": status,
                "status_label": label,
                "cadence": "不可變歷史檔" if spec.get("immutable") else "每日官方更新",
                "update_owner": "ETF 發行商歷史回補器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": fresh,
                "coverage": _coverage(
                    1 if receipt else 0, 1, unit="端點", label="官方來源回執"
                ),
                "eta": eta,
                "rows": _integer(result.get("rows"))
                if isinstance(result, Mapping)
                else None,
                "publishable": False,
                "automation_eligible": True,
                "acquisition_enabled": True,
                "detail": str(spec.get("url") or "官方發行商資料來源"),
                "warnings": ["歷史列在本機首次觀測前不會被回填成 point-in-time 可用。"],
                "detail_link": None,
            }
        )
    return output


def _rollup_storage_groups(
    groups: list[dict[str, Any]], logical: Iterable[Mapping[str, Any]]
) -> None:
    """Reconcile every group against active child endpoint operations.

    Physical receipts and logical endpoints answer different questions.  The
    group keeps its own row count and coverage, but cannot claim current when a
    required active child is catching up or unable to complete.
    """

    by_parent: dict[str, list[Mapping[str, Any]]] = {}
    for child in logical:
        by_parent.setdefault(str(child.get("parent_id") or ""), []).append(child)
    for group in groups:
        group_id = str(group.get("id") or "")
        children = by_parent.get(group_id, [])
        if not children:
            continue
        active_children = [
            child
            for child in children
            if str(child.get("operation_state") or "")
            not in {"deferred", "control", "reference"}
        ]
        counts = {state: 0 for state in _OPERATION_ORDER}
        for child in children:
            state = str(child.get("operation_state") or "unable")
            counts[state] = counts.get(state, 0) + 1
        group["child_operation_counts"] = counts
        group["child_endpoint_count"] = len(children)
        group["active_child_endpoint_count"] = len(active_children)
        if group_id in {"group:okx", "group:bybit", "group:binance"}:
            canonical_1m = next(
                (
                    child
                    for child in children
                    if child.get("scope") == "product_granularity"
                    and child.get("granularity") == "1m"
                    and isinstance(child.get("coverage"), Mapping)
                ),
                None,
            )
            if canonical_1m is not None:
                group["coverage"] = dict(canonical_1m["coverage"])
                group["coverage_source_endpoint_id"] = canonical_1m.get("id")
        if not active_children:
            continue

        active_counts = {state: 0 for state in _OPERATION_ORDER}
        for child in active_children:
            state = str(child.get("operation_state") or "unable")
            active_counts[state] = active_counts.get(state, 0) + 1
        group["active_child_operation_counts"] = active_counts
        warning = (
            "子端點狀態："
            + "、".join(
                f"{_OPERATION_LABELS[state]} {count}"
                for state, count in active_counts.items()
                if count
            )
            + "。群組狀態採最保守必要端點，不以目錄存在或部分成功冒充完成。"
        )
        warnings = list(group.get("warnings") or [])
        if warning not in warnings:
            warnings.append(warning)
        group["warnings"] = warnings

        if active_counts["unable"]:
            group["status"] = "blocked"
            group["status_label"] = f"{active_counts['unable']:,} 個必要子端點無法完成"
            group["eta"] = _unknown_eta(
                "blocked", "至少一個必要子端點受阻；其餘端點完成不能使群組完成。"
            )
        elif active_counts["catching_up"]:
            running = any(
                child.get("execution_state") == "running"
                for child in active_children
                if child.get("operation_state") == "catching_up"
            )
            group["status"] = "updating" if running else "waiting"
            group["status_label"] = (
                f"{active_counts['catching_up']:,} 個必要子端點尚未到最新"
            )
            active_etas = [
                child.get("eta")
                for child in active_children
                if child.get("operation_state") == "catching_up"
                and isinstance(child.get("eta"), Mapping)
            ]
            measured = [
                eta
                for eta in active_etas
                if _integer(eta.get("remaining_seconds")) is not None
            ]
            group["eta"] = (
                dict(
                    max(
                        measured,
                        key=lambda eta: _integer(eta.get("remaining_seconds")) or 0,
                    )
                )
                if measured
                else _unknown_eta(
                    "warming_up" if running else "waiting_schedule",
                    "子端點尚未到最新，但目前沒有可合併的有效吞吐 ETA。",
                )
            )
        elif active_counts["streaming"]:
            group["status"] = "updating"
            group["status_label"] = (
                f"{active_counts['streaming']:,} 個子端點正在有效串流"
            )
            group["eta"] = _unknown_eta(
                "continuous", "串流沒有總完工日；以交易時窗與落盤心跳驗證。"
            )


def _specialize_groups(
    groups: list[dict[str, Any]],
    *,
    shioaji: Mapping[str, Any],
    openbb: Mapping[str, Any],
    root: Path,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
    runtime_progress: list[dict[str, Any]],
) -> None:
    by_id = {row["id"]: row for row in groups}
    tw_summary = _read_json(root / "data_tw_public/download_summary.json", {})
    tw_receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    tw_events = _read_json(
        root / "artifacts/data_refresh/tw_public/events/latest.json", {}
    )
    tw = by_id.get("group:tw-public")
    if tw is not None and isinstance(tw_summary, Mapping):
        total = _integer(tw_summary.get("dataset_count")) or 0
        completed = (_integer(tw_summary.get("ok_count")) or 0) + (
            _integer(tw_summary.get("up_to_date_count")) or 0
        )
        coverage_complete = tw_summary.get("coverage_complete") is True
        receipt_ok = (
            isinstance(tw_receipt, Mapping) and tw_receipt.get("status") == "ok"
        )
        tw["coverage"] = _coverage(completed, total, unit="資料集", label="完整稽核")
        tw["data_through"] = str(tw_summary.get("end_date") or "") or None
        tw["rows"] = _integer(tw_summary.get("rows_total"))
        if coverage_complete and receipt_ok and tw["freshness"]["state"] == "current":
            tw["status"] = "current"
            tw["status_label"] = "不可變快照完整且最新"
            tw["eta"] = _complete_eta("全部公開資料集缺口稽核通過並完成快照切換。")
        elif coverage_complete:
            tw["status"] = "stale"
            tw["status_label"] = "完整快照需要更新"
        event_registered = (
            _integer(tw_events.get("registered_dataset_count"))
            if isinstance(tw_events, Mapping)
            else None
        )
        event_observed = (
            _integer(tw_events.get("observed_dataset_count"))
            if isinstance(tw_events, Mapping)
            else None
        )
        event_blocking_unapplied = (
            _integer(tw_events.get("blocking_unapplied_event_count"))
            if isinstance(tw_events, Mapping)
            and "blocking_unapplied_event_count" in tw_events
            else _integer(tw_events.get("unapplied_event_count"))
            if isinstance(tw_events, Mapping)
            else None
        )
        event_healthy = bool(
            isinstance(tw_events, Mapping)
            and tw_events.get("status") == "ok"
            and tw_events.get("coverage_complete") is True
            and event_registered is not None
            and event_registered > 0
            and event_observed == event_registered
            and _integer(tw_events.get("failed_probe_count")) == 0
            and event_blocking_unapplied == 0
        )
        tw["event_monitor"] = {
            "healthy": event_healthy,
            "status": tw_events.get("status")
            if isinstance(tw_events, Mapping)
            else None,
            "registered": event_registered,
            "observed": event_observed,
            "failed_probes": _integer(tw_events.get("failed_probe_count"))
            if isinstance(tw_events, Mapping)
            else None,
            "unapplied_events": _integer(tw_events.get("unapplied_event_count"))
            if isinstance(tw_events, Mapping)
            else None,
            "blocking_unapplied_events": event_blocking_unapplied,
            "opening_apply_deferred": tw_events.get("opening_apply_deferred")
            if isinstance(tw_events, Mapping)
            else None,
            "opening_apply_deferred_count": _integer(
                tw_events.get("opening_apply_deferred_count")
            )
            if isinstance(tw_events, Mapping)
            else None,
            "updated_at": tw_events.get("updated_at_taipei")
            if isinstance(tw_events, Mapping)
            else None,
        }
        tw["cadence"] = "來源事件每 60–300 秒；08:00/08:15/08:24 live root 驗收；23:50 背景冷備份"
        tw["update_owner"] = "來源事件監測器＋不可變快照更新器"
        if event_healthy and tw.get("status") == "current":
            tw["status_label"] = "156/156 來源事件健康，快照完整最新"
        elif not event_healthy:
            tw["status"] = "degraded"
            tw["status_label"] = "來源版本事件監測尚未全數健康"
            tw["eta"] = _unknown_eta(
                "running_unmeasured", "等待 156 項來源完成探測與事件套用。"
            )

    pipeline_by_id = {
        str(row.get("id")): row
        for row in shioaji.get("pipelines", [])
        if isinstance(row, Mapping)
    }
    for group_id, pipeline_id in {
        "group:tw-minute-train": "minute_research",
        "group:tw-minute-source-cold": "stock_minute",
        "group:tw-microstructure-train": "hft_dataset",
        "group:tw-microstructure-captures-cold": "fop_stream",
        "group:tw-futures": "futures_history",
        "group:tw-shioaji-history": "historical_market_data",
    }.items():
        group = by_id.get(group_id)
        pipeline = pipeline_by_id.get(pipeline_id)
        if group is None or pipeline is None:
            continue
        raw_status = str(pipeline.get("status") or "unavailable")
        group["status"] = {
            "active": "updating",
            "ready": "current",
            "complete": "complete",
            "waiting": "waiting",
            "partial": "degraded",
            "attention": "degraded",
        }.get(raw_status, "unavailable")
        group["status_label"] = str(pipeline.get("status_label") or raw_status)
        group["coverage"] = (
            dict(pipeline["coverage"])
            if isinstance(pipeline.get("coverage"), Mapping)
            else None
        )
        group["eta"] = (
            dict(pipeline["eta"])
            if isinstance(pipeline.get("eta"), Mapping)
            else group["eta"]
        )
        group["latest_at_utc"] = pipeline.get("latest_at_utc")
        group["data_through"] = pipeline.get("data_through")
        group["warnings"] = [str(value) for value in pipeline.get("warnings", [])]
        group["detail_link"] = "../shioaji/"

    archive = openbb.get("archive")
    process = openbb.get("process")
    if isinstance(archive, Mapping):
        for group_id in ("group:openbb-compact", "group:openbb-task-shards-local"):
            group = by_id.get(group_id)
            if group is None:
                continue
            resolved = _integer(archive.get("resolved_tasks")) or 0
            total = _integer(archive.get("total_tasks")) or 0
            actionable = _integer(archive.get("actionable_unresolved_tasks")) or 0
            rates = [
                _number(row.get("recent_tasks_per_minute")) or 0.0
                for row in openbb.get("providers", [])
                if isinstance(row, Mapping)
            ]
            rate = sum(rates)
            group["coverage"] = _coverage(resolved, total, unit="任務", label="已判定")
            group["rows"] = (
                _integer(archive.get("success_rows"))
                if group_id == "group:openbb-compact"
                else None
            )
            group["data_through"] = str(archive.get("end_date") or "") or None
            source_age = _number(openbb.get("source_age_seconds"))
            latest = (
                now - timedelta(seconds=source_age) if source_age is not None else None
            )
            group["latest_at_utc"] = _iso(latest)
            group["freshness"] = _freshness(
                latest,
                now=now,
                window_seconds=15 * 60,
            )
            alive = (
                isinstance(process, Mapping) and process.get("downloader_alive") is True
            )
            audit_health = str(openbb.get("audit_health") or "unknown").lower()
            if audit_health in {"critical", "degraded"}:
                group["status"] = "degraded"
                group["status_label"] = (
                    "正在回補，但完整性稽核仍為 " + audit_health
                    if alive
                    else "完整性稽核失敗且下載程序未執行"
                )
            else:
                group["status"] = "updating" if alive else "blocked"
                group["status_label"] = "正在持續回補" if alive else "下載程序未執行"
            if actionable == 0:
                group["eta"] = _complete_eta("沒有可執行的未解任務。")
            elif rate > 0:
                seconds = int(math.ceil(actionable / rate * 60))
                group["eta"] = {
                    "state": "estimating",
                    "remaining_seconds": seconds,
                    "estimated_complete_at_utc": _iso(now + timedelta(seconds=seconds)),
                    "confidence": "low",
                    "basis": "依近期供應商總接受速率外推可執行缺口；配額與權限結果會改變 ETA。",
                }
            else:
                group["eta"] = _unknown_eta(
                    "waiting_quota",
                    "目前有效速率為零；等待配額或下一輪排程。",
                )
            group["detail"] = (
                f"已接受 {_integer(archive.get('accepted_tasks')) or 0:,}；"
                f"權威不可用 {_integer(archive.get('unavailable_tasks')) or 0:,}；"
                f"可執行缺口 {actionable:,}。"
            )
            alerts = openbb.get("alerts")
            if isinstance(alerts, list):
                group["warnings"] = [
                    str(alert.get("message"))
                    for alert in alerts
                    if isinstance(alert, Mapping) and alert.get("message")
                ][:3]
            group["detail_link"] = "../openbb/"

    active_group_services = {
        "group:tw-index-futures": ("taifex_futures",),
        "group:tw-index-derivatives-ticks": ("taifex_auxiliary",),
        "group:tw-index-options-daily": ("taifex_auxiliary",),
    }
    for group_id, service_names in active_group_services.items():
        group = by_id.get(group_id)
        if group is None:
            continue
        if not any(
            refresh_services.get(name, {}).get("active") is True
            for name in service_names
        ):
            continue
        group["status"] = "updating"
        group["status_label"] = "完整缺口更新正在執行"
        group["eta"] = _unknown_eta(
            "running_unmeasured",
            "更新正在執行；目前下載器尚未寫出足夠的批內速率，完成後會以新回執校正。",
        )

    progress_tokens = {
        "group:yahoo-market": (":us_stocks", ":crypto"),
        "group:okx": ("download:okx",),
        "group:bybit": ("download:bybit",),
        "group:binance": ("download:binance", "download:binance-history"),
        "group:forex-frankfurter": ("download:forex:frankfurter",),
    }
    progress_services = {
        "group:yahoo-market": ("registered_daily", "registered_intraday"),
        "group:okx": ("registered_intraday",),
        "group:bybit": ("registered_intraday",),
        "group:binance": ("registered_intraday",),
        "group:forex-frankfurter": ("registered_daily",),
    }
    for group_id, tokens in progress_tokens.items():
        group = by_id.get(group_id)
        if group is None or not any(
            refresh_services.get(name, {}).get("active") is True
            for name in progress_services[group_id]
        ):
            continue
        structured_progress_at = _parse_time(group.get("latest_at_utc"))
        structured_progress_age = (
            max(0.0, (now - structured_progress_at).total_seconds())
            if structured_progress_at is not None
            else None
        )
        structured_eta_state = str((group.get("eta") or {}).get("state") or "")
        if (
            group.get("status") == "updating"
            and structured_progress_age is not None
            and structured_progress_age <= 15 * 60
            and structured_eta_state
            in {"estimating", "warming_up", "running_unmeasured"}
        ):
            # Structured progress.json is the canonical logical-unit receipt.
            # Tqdm log scraping observes only one transient phase and must be a
            # fallback, never an override of source-backed progress.
            continue
        progress = _select_runtime_progress(runtime_progress, tokens=tokens)
        if progress is None:
            continue
        group["status"] = "updating"
        group["status_label"] = "完整缺口更新正在執行"
        group["coverage"] = _coverage(
            progress["current"],
            progress["total"],
            unit="項",
            label=f"執行階段 {progress['label']}",
        )
        remaining = _integer(progress.get("remaining_seconds"))
        if remaining is not None:
            group["eta"] = {
                "state": "phase_estimate",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=remaining)),
                "confidence": "low",
                "basis": "下載器目前執行階段的 tqdm 吞吐率 ETA；進入下一個掃描或修復階段後會重新估算。",
            }


def _stock_stream_window(now: datetime) -> dict[str, Any]:
    local = now.astimezone(TAIPEI)
    candidates: list[tuple[datetime, datetime]] = []
    for offset in range(0, 10):
        session_date = local.date() + timedelta(days=offset)
        if session_date.weekday() >= 5:
            continue
        starts = datetime.combine(session_date, datetime_time(8, 45), tzinfo=TAIPEI)
        ends = datetime.combine(session_date, datetime_time(13, 30), tzinfo=TAIPEI)
        if ends > local:
            candidates.append((starts, ends))
    if not candidates:
        raise RuntimeError("could not resolve next stock capture window")
    starts, ends = min(candidates, key=lambda value: value[0])
    return {
        "kind": "tw_stock",
        "timezone": "Asia/Taipei",
        "schedule_label": "交易日 08:45–13:30（以實際訂閱與落盤心跳為準）",
        "state": "open" if starts <= local < ends else "waiting",
        "starts_at_utc": _iso(starts.astimezone(UTC)),
        "ends_at_utc": _iso(ends.astimezone(UTC)),
    }


def _taifex_stream_window(now: datetime) -> dict[str, Any]:
    window = next_taifex_capture_window(now.astimezone(TAIPEI))
    local = now.astimezone(TAIPEI)
    return {
        "kind": "taifex",
        "timezone": "Asia/Taipei",
        "schedule_label": (
            "交易日 08:30–13:45、14:50–次日 05:00"
            "（正常週曆；休市以實際訂閱與落盤心跳為準）"
        ),
        "session": window.session,
        "trading_date": window.trading_date.isoformat(),
        "state": "open" if window.starts_at <= local < window.stops_at else "waiting",
        "starts_at_utc": _iso(window.starts_at.astimezone(UTC)),
        "ends_at_utc": _iso(window.stops_at.astimezone(UTC)),
    }


def _next_declared_calendar(
    profile: Mapping[str, Any], now: datetime
) -> datetime | None:
    raw_clock = str(profile.get("calendar_time") or "")
    if not raw_clock:
        return None
    try:
        hour_text, minute_text = raw_clock.split(":", maxsplit=1)
        clock = datetime_time(int(hour_text), int(minute_text))
    except (TypeError, ValueError):
        return None
    local = now.astimezone(TAIPEI)
    candidate = datetime.combine(local.date(), clock, tzinfo=TAIPEI)
    if candidate <= local:
        candidate += timedelta(days=1)
    if profile.get("calendar_weekdays") is True:
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


def _profile_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    row_id = str(row.get("id") or "")
    if row_id == "tw-public:mops_xbrl_quarterly":
        return {
            "mode": "timer", "service_keys": ("tw_mops_xbrl",),
            "schedule_label": "取得核准後每四小時檢查官方季度 ZIP",
            "active_means_running": True, "requires_timer_active": True,
        }
    if row_id in {
        "tw-public:dgbas_release_vintages",
        "tw-public:cbc_fx_reserve_release_vintages",
        "tw-public:cbc_money_release_vintages",
        "tw-public:cbc_overnight_official_pages",
        "tw-public:cbc_usdtwd_annual_pages",
        "tw-public:mof_macro_release_dates",
        "tw-public:mof_trade_release_values",
        "tw-public:mof_original_release_archive",
        "tw-public:provisional_macro_feature_events",
    }:
        return {
            "mode": "timer",
            "service_keys": ("tw_public_release_archives",),
            "schedule_label": "交易日 07:00／16:30／19:30 增量；週日全索引稽核",
            "active_means_running": True,
            "requires_timer_active": True,
        }
    shioaji_profiles: dict[str, dict[str, Any]] = {
        "shioaji:fop_stream": {
            **_AUTOMATION_PROFILES["group:tw-microstructure-captures-cold"],
            "service_keys": ("shioaji_fop_stream",),
            "stream_kind": "taifex",
        },
        "shioaji:top200_stream": {
            **_AUTOMATION_PROFILES["group:tw-microstructure-captures-cold"],
            "service_keys": ("shioaji_stock_stream",),
            "stream_kind": "tw_stock",
        },
        "shioaji:hft_dataset": _AUTOMATION_PROFILES["group:tw-microstructure-train"],
        "shioaji:stock_minute": _AUTOMATION_PROFILES["group:tw-minute-source-cold"],
        "shioaji:minute_research": _AUTOMATION_PROFILES["group:tw-minute-train"],
        "shioaji:stock_daily": _AUTOMATION_PROFILES["group:tw-minute-train"],
        "shioaji:futures_history": _AUTOMATION_PROFILES["group:tw-futures"],
        "shioaji:contract_catalog": _AUTOMATION_PROFILES["group:tw-futures"],
        "shioaji:historical_market_data": _AUTOMATION_PROFILES[
            "group:tw-shioaji-history"
        ],
        "shioaji:on_demand_snapshots": {
            "mode": "on_demand",
            "service_keys": (),
            "schedule_label": "策略需要報價時逐次查詢；不持續輪詢",
            "active_means_running": False,
        },
    }
    if row_id in shioaji_profiles:
        return dict(shioaji_profiles[row_id])
    parent_id = str(row.get("parent_id") or row_id)
    profile = _AUTOMATION_PROFILES.get(parent_id)
    if profile is None:
        return {
            "mode": "not_configured",
            "service_keys": (),
            "schedule_label": "尚未註冊自動更新排程",
            "active_means_running": False,
        }
    return dict(profile)


def _automation_for_row(
    row: Mapping[str, Any],
    *,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    profile = _profile_for_row(row)
    keys = tuple(str(value) for value in profile.get("service_keys", ()))
    states = [
        refresh_services[key]
        for key in keys
        if isinstance(refresh_services.get(key), Mapping)
    ]
    unarmed_service_keys = [
        key
        for key in keys
        if isinstance(state := refresh_services.get(key), Mapping)
        and state.get("timer_active") is True
        and state.get("timer_state") == "elapsed"
        and state.get("active") is not True
        and not state.get("next_run_at_utc")
    ]
    next_runs = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("next_run_at_utc"))) is not None
    ]
    last_triggers = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("last_trigger_at_utc"))) is not None
    ]
    starts = [
        parsed
        for state in states
        if (parsed := _parse_time(state.get("started_at_utc"))) is not None
    ]
    declared_next = _next_declared_calendar(profile, now)
    if not next_runs and declared_next is not None:
        next_runs.append(declared_next)
    next_run_basis = (
        "systemd_timer"
        if any(state.get("next_run_at_utc") for state in states)
        else "declared_calendar"
        if declared_next is not None
        else "contract_only"
    )
    active = any(state.get("active") is True for state in states)
    active_means_running = profile.get("active_means_running", True) is True
    eta_state = str((row.get("eta") or {}).get("state") or "unknown")
    row_has_live_work = str(row.get("status") or "") == "updating" or eta_state in {
        "estimating",
        "warming_up",
        "running_unmeasured",
        "phase_estimate",
    }
    # An umbrella service can be active while a different provider stage runs.
    # Require endpoint-level progress/ETA evidence before calling this row active.
    job_running = active and active_means_running and row_has_live_work
    stream_kind = str(profile.get("stream_kind") or "")
    stream_window = None
    if stream_kind == "taifex":
        stream_window = _taifex_stream_window(now)
    elif stream_kind == "tw_stock":
        stream_window = _stock_stream_window(now)
    elif stream_kind == "mixed_tw":
        taifex = _taifex_stream_window(now)
        stock = _stock_stream_window(now)
        candidates = [taifex, stock]
        open_windows = [item for item in candidates if item["state"] == "open"]
        selected = min(
            open_windows or candidates,
            key=lambda item: str(item.get("starts_at_utc") or ""),
        )
        stream_window = {
            **selected,
            "kind": "mixed_tw",
            "schedule_label": (
                "台股 08:45–13:30；TAIFEX 08:30–13:45、14:50–次日 05:00（正常週曆）"
            ),
        }
    eligible = row.get("automation_eligible", True) is True
    mode = str(profile.get("mode") or "not_configured")
    automatic = eligible and mode not in {"frozen", "not_configured", "on_demand"}
    if profile.get("requires_timer_active") is True:
        automatic = automatic and any(state.get("timer_active") is True for state in states)
    if unarmed_service_keys:
        automatic = False
    schedule_label = str(
        profile.get("schedule_label") or row.get("cadence") or "未指定"
    )
    if not eligible and mode not in {"frozen", "on_demand"}:
        schedule_label = "未接入可執行自動更新；父群組排程不代表此端點"
    if not eligible and mode not in {"frozen", "on_demand"}:
        schedule_state = "not_configured"
    elif profile.get("requires_timer_active") is True and not automatic:
        schedule_state = "not_configured"
    elif unarmed_service_keys:
        schedule_state = "timer_unarmed"
    elif mode == "stream":
        schedule_state = (
            "stream_window_open"
            if stream_window and stream_window.get("state") == "open"
            else "waiting_stream_window"
        )
    elif job_running:
        schedule_state = "running"
    elif next_runs:
        schedule_state = "scheduled"
    elif mode in {"continuous_backfill", "quota_backfill"} and active:
        schedule_state = "running_or_waiting_quota"
    elif mode == "interval_after_completion":
        schedule_state = "after_previous_completion"
    elif mode == "on_demand":
        schedule_state = "on_demand"
    elif mode == "frozen":
        schedule_state = "not_applicable"
    elif mode == "not_configured":
        schedule_state = "not_configured"
    else:
        schedule_state = "schedule_declared"
    evidence = []
    if states:
        evidence.append("systemd_service")
    if any(state.get("timer_active") is True for state in states):
        evidence.append("systemd_timer")
    if row.get("latest_at_utc"):
        evidence.append("receipt_or_manifest")
    if not evidence:
        evidence.append("registry_only")
    if next_run_basis == "declared_calendar":
        evidence.append("declared_calendar")
    return {
        "mode": mode,
        "automatic_update": automatic,
        "schedule_state": schedule_state,
        "schedule_label": schedule_label,
        "service_keys": list(keys),
        "unarmed_service_keys": unarmed_service_keys,
        "service_active": active,
        "job_running": job_running,
        "next_run_at_utc": _iso(min(next_runs)) if next_runs else None,
        "next_run_basis": next_run_basis,
        "last_trigger_at_utc": _iso(max(last_triggers)) if last_triggers else None,
        "last_started_at_utc": _iso(max(starts)) if starts else None,
        "stream_window": stream_window,
        "evidence": evidence,
    }


def _operation_state(
    row: Mapping[str, Any], automation: Mapping[str, Any]
) -> tuple[str, str, str]:
    raw_status = str(row.get("status") or "unavailable")
    scope = str(row.get("scope") or "")
    eta_state = str((row.get("eta") or {}).get("state") or "unknown")
    mode = str(automation.get("mode") or "not_configured")
    stream_window = automation.get("stream_window")
    freshness_age = _number((row.get("freshness") or {}).get("age_seconds"))
    freshness_state = str((row.get("freshness") or {}).get("state") or "unknown")
    coverage_ratio = _number((row.get("coverage") or {}).get("ratio"))
    recent_stream_heartbeat = freshness_age is not None and freshness_age <= 10 * 60

    if scope == "credential_gate":
        return (
            "control",
            "control",
            str(row.get("status_label") or "API 憑證存在性稽核"),
        )

    # Product inventory rows describe both executable acquisitions and explicit
    # capability boundaries.  A provider that does not offer a grain is a
    # reference fact, not a failed download.  Likewise, a product held behind
    # an unconfigured credential is deferred by that control gate; the separate
    # credential row remains visible.  Counting either case as an active
    # ``unable`` endpoint made the overall monitor critical even though there
    # was no runnable job that could succeed.
    if scope == "product_granularity":
        implementation = str(row.get("implementation") or "")
        if implementation == "not_available":
            return (
                "reference",
                "not_applicable",
                str(row.get("status_label") or "供應商不提供此資料粒度"),
            )
        credential_state = str(row.get("credential_state") or "not_required")
        if (
            "credential_gate" in implementation
            and credential_state not in {"configured", "not_required"}
        ):
            return (
                "deferred",
                "waiting_credential",
                str(row.get("status_label") or "等待必要憑證設定"),
            )

    if row.get("registry_alias") is True:
        return (
            "reference",
            "registry_alias",
            str(row.get("status_label") or "由既有專用端點維護"),
        )

    if raw_status == "deferred":
        return (
            "deferred",
            "deferred",
            str(row.get("status_label") or "依目前資料取得範圍延後"),
        )

    if mode == "stream":
        window_open = (
            isinstance(stream_window, Mapping) and stream_window.get("state") == "open"
        )
        if raw_status in {"blocked", "unavailable", "degraded"}:
            return "unable", "blocked", "串流來源缺少可用的服務或落盤證據"
        if (
            window_open
            and raw_status in {"updating", "current"}
            and recent_stream_heartbeat
        ):
            return "streaming", "streaming", "交易時窗內且最近十分鐘有落盤心跳"
        return (
            "catching_up",
            "waiting_stream_window",
            "目前未觀測到有效串流；等待下一個時窗或新心跳",
        )

    actively_working = automation.get("job_running") is True or eta_state in {
        "estimating",
        "warming_up",
        "running_unmeasured",
        "phase_estimate",
    }
    schedule_state = str(automation.get("schedule_state") or "")
    scheduled_execution = (
        "scheduled"
        if schedule_state
        in {"scheduled", "after_previous_completion", "schedule_declared"}
        else "waiting_quota"
        if eta_state == "waiting_quota"
        else "waiting"
    )
    if schedule_state == "timer_unarmed" and not actively_working:
        return "unable", "failed", "自動 timer 已啟用，但沒有下一次觸發"
    if raw_status in {"blocked", "unavailable"}:
        return "unable", "blocked", str(row.get("status_label") or "來源不可用")
    if raw_status == "degraded":
        if actively_working:
            return "catching_up", "running", "正在修復已知缺口"
        return "unable", "failed", str(row.get("status_label") or "完整性稽核未通過")
    if mode == "on_demand":
        return "complete", "on_demand", "端點按需逐次完成，沒有常駐下載佇列"
    if (
        raw_status in {"current", "complete"}
        and mode != "frozen"
        and freshness_state == "stale"
    ):
        if row.get("automation_eligible", True) is not True:
            return "unable", "not_configured", "最近批次已過時，且尚無自動更新管線"
        return (
            "catching_up",
            "running" if actively_working else scheduled_execution,
            "最近批次已過新鮮度時窗；歷史完成不等於目前已到最新",
        )
    if (
        raw_status in {"current", "complete"}
        and coverage_ratio is not None
        and coverage_ratio < 1.0
    ):
        if row.get("automation_eligible", True) is not True:
            return "unable", "not_configured", "完整度尚有缺口，且尚無自動補齊管線"
        return (
            "catching_up",
            "running" if actively_working else scheduled_execution,
            "完整度分子小於分母；不得以成功狀態覆蓋尚存缺口",
        )
    if row.get("automation_eligible", True) is not True:
        if raw_status in {"current", "complete", "legacy"}:
            return "complete", "not_applicable", "封存或按契約不需要持續更新"
        return "unable", "not_configured", "來源已登錄，但尚未具備可執行的自動更新管線"

    if raw_status == "updating" or actively_working:
        return "catching_up", "running", "更新工作執行中，尚未到最新"
    if raw_status in {"waiting", "stale"} or eta_state in {
        "waiting_quota",
        "waiting_schedule",
    }:
        return (
            "catching_up",
            scheduled_execution,
            str(row.get("status_label") or "等待更新"),
        )
    if raw_status in {"current", "complete", "legacy"}:
        return "complete", "idle_current", "最新可驗證批次已完成"
    return "unable", "unknown", "缺少足夠狀態證據，無法判定會自動完成"


def _next_data_date(value: Any) -> str | None:
    """Return the next calendar data date for a source-backed date value."""

    text = str(value or "").strip()
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:$|T)", text)
    if match is None:
        return None
    try:
        parsed = date.fromisoformat(match.group(1))
    except ValueError:
        return None
    return (parsed + timedelta(days=1)).isoformat()


def _publication_for_row(
    row: Mapping[str, Any],
    *,
    automation: Mapping[str, Any],
    hint: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Describe upstream publication separately from our acquisition timer."""

    explicit = dict(hint) if isinstance(hint, Mapping) else {}
    mode = str(automation.get("mode") or "not_configured")
    cadence = str(row.get("cadence") or "依來源更新")
    scope = str(row.get("scope") or "")
    if scope == "credential_gate":
        schedule_kind = "not_applicable"
        schedule_label = "憑證狀態，不是公開資料發布端點"
        basis = "此列只驗證憑證是否已設定，不代表任何上游資料發布。"
    elif row.get("registry_alias") is True:
        schedule_kind = "reference"
        schedule_label = "清冊參照；發布與取得時間請見對應專用端點"
        basis = "此列只保留來源責任映射，不建立第二份發布或排程事實。"
    elif explicit:
        schedule_kind = str(explicit.get("schedule_kind") or "source_contract")
        schedule_label = str(
            explicit.get("schedule_label") or "來源未承諾固定發布時刻；以實際偵測為準"
        )
        basis = str(explicit.get("basis") or "發布時間來自來源契約或版本偵測收據。")
    elif mode == "stream":
        schedule_kind = "continuous"
        schedule_label = f"連續發布；{cadence}"
        basis = "串流資料沒有單一發布時刻；最近落盤心跳代表實際觀測。"
    elif mode == "on_demand":
        schedule_kind = "on_demand"
        schedule_label = "按需產生，沒有固定發布時刻"
        basis = "只有呼叫端點時才產生資料。"
    elif mode == "frozen":
        schedule_kind = "not_applicable"
        schedule_label = "凍結封存，不再發布"
        basis = "封存資料不再追蹤下一次發布。"
    else:
        schedule_kind = "cadence_only"
        schedule_label = f"來源未承諾固定時刻；發布頻率：{cadence}"
        basis = (
            "目前只有來源 cadence 或我方取得契約；最近觀測時間不冒充上游官方發布時間。"
        )
    return {
        "schedule_kind": schedule_kind,
        "schedule_label": schedule_label,
        "exact_time_declared": explicit.get("exact_time_declared") is True,
        "probe_boundaries_taipei": list(explicit.get("probe_boundaries_taipei") or []),
        "detected_at_utc": explicit.get("detected_at_utc"),
        "last_checked_at_utc": explicit.get("last_checked_at_utc"),
        "applied_at_utc": explicit.get("applied_at_utc"),
        "next_check_at_utc": explicit.get("next_check_at_utc"),
        "observed_at_utc": row.get("latest_at_utc"),
        "basis": basis,
        "receipt_phases": list(explicit.get("receipt_phases") or []),
        "acquisition_schedule_label": automation.get("schedule_label"),
        "next_acquisition_at_utc": automation.get("next_run_at_utc"),
    }


def _acquisition_progress(
    row: Mapping[str, Any],
    *,
    operation: str,
    execution: str,
    publication: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize per-source progress while preserving unknown denominators."""

    coverage = row.get("coverage")
    coverage = coverage if isinstance(coverage, Mapping) else {}
    ratio = _number(coverage.get("ratio"))
    if ratio is not None:
        ratio = min(1.0, max(0.0, ratio))
    current = _integer(coverage.get("current"))
    total = _integer(coverage.get("total"))
    if operation in {"deferred", "control", "reference"}:
        state = {
            "deferred": "deferred",
            "control": "not_applicable",
            "reference": "reference",
        }[operation]
        label = {
            "deferred": "已延後；不列入主動取得範圍",
            "control": "設定／憑證就緒狀態，不是資料下載進度",
            "reference": "清冊參照；進度由既有專用端點唯一計算",
        }[operation]
        basis = {
            "deferred": "使用者範圍決策，不是完整度分子／分母",
            "control": "控制面狀態與資料面完整度分開計算",
            "reference": "只保留來源與專用端點責任映射，避免同一資料重複進入分母",
        }[operation]
        return {
            "state": state,
            "label": label,
            "current": None,
            "total": None,
            "ratio": None,
            "unit": None,
            "basis": basis,
            "first_data_observed": False,
            "first_data_at_utc": None,
            "data_through": None,
            "preparing_for_date": None,
            "coverage_complete": False,
            "batch_complete": False,
            "up_to_date": False,
            "evidence_coverage": None,
        }
    evidence_coverage = None
    stale_full_receipt = (
        operation in {"catching_up", "unable"} and ratio is not None and ratio >= 1.0
    )
    if stale_full_receipt:
        # A historical or phase-local 100% receipt is evidence, not proof that
        # an endpoint which is stale or blocked has presently completed.
        evidence_coverage = {
            "current": current,
            "total": total,
            "ratio": ratio,
            "unit": str(coverage.get("unit") or "").strip() or None,
            "label": str(coverage.get("label") or "").strip() or "既有完整度收據",
        }
        ratio = None
        current = None
        total = None
    data_through = str(row.get("data_through") or "").strip() or None
    preparing_for_date = _next_data_date(data_through)
    first_data_observed = bool(
        data_through
        or (current is not None and current > 0)
        or ((_integer(row.get("rows")) or 0) > 0)
        or operation == "streaming"
    )
    up_to_date = operation in {"complete", "streaming"}
    coverage_complete = ratio >= 1.0 if ratio is not None else False
    batch_complete = operation == "complete" or (
        coverage_complete and execution not in {"running", "streaming"}
    )
    if operation == "streaming":
        state = "streaming"
        label = "持續取得中；串流沒有總完工日"
    elif up_to_date:
        state = "complete"
        label = "取得完成且已到最新"
    elif operation == "unable":
        state = "blocked"
        label = (
            "目前受阻；既有完整度收據不代表現在可完成"
            if stale_full_receipt
            else "取得未完成；目前受阻"
        )
    elif stale_full_receipt:
        state = "recalibrating" if execution == "running" else "stale_complete_receipt"
        label = (
            "舊批次或目前階段已完成；仍在重新量測全域進度"
            if execution == "running"
            else "舊批次已完成但資料已過時；等待新一輪取得"
        )
    elif batch_complete and preparing_for_date:
        state = "preparing_next_date"
        label = f"本批完成；準備下一資料日 {preparing_for_date}"
    elif first_data_observed and preparing_for_date:
        state = "acquiring"
        label = f"首筆已到；取得中並準備下一資料日 {preparing_for_date}"
    elif first_data_observed:
        state = "acquiring"
        label = "已收到資料；取得中"
    else:
        state = "waiting"
        label = "尚未收到首筆資料"

    progress_basis = str(coverage.get("label") or "").strip()
    eta = row.get("eta")
    eta = eta if isinstance(eta, Mapping) else {}
    completed_receipt = (
        row.get("completion_receipt_present", True) is True
        and bool(row.get("latest_at_utc"))
        and str(eta.get("state") or "") == "complete"
    )
    if ratio is None and up_to_date and completed_receipt:
        # A timestamped, high-confidence completed receipt is a valid binary
        # denominator.  An on-demand contract alone is not.
        ratio = 1.0
        current = 1
        total = 1
        unit = "完成收據"
        progress_basis = "最新完成收據"
    else:
        unit = str(coverage.get("unit") or "").strip() or None
        if ratio is None and not stale_full_receipt:
            progress_basis = (
                "已收到首筆，但來源未提供可靠總量"
                if first_data_observed
                else "來源未提供可靠分子／分母"
            )
        elif stale_full_receipt:
            progress_basis = "既有收據另列為證據，不作目前工作完成率"
    first_data_at = publication.get("applied_at_utc") or None
    return {
        "state": state,
        "label": label,
        "current": current,
        "total": total,
        "ratio": ratio,
        "unit": unit,
        "basis": progress_basis,
        "first_data_observed": first_data_observed,
        "first_data_at_utc": first_data_at,
        "data_through": data_through,
        "preparing_for_date": preparing_for_date,
        "coverage_complete": coverage_complete,
        "batch_complete": batch_complete,
        "up_to_date": up_to_date,
        "evidence_coverage": evidence_coverage,
    }


def _row_sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    operation = str(row.get("operation_state") or "unable")
    execution_order = {
        "running": 0,
        "streaming": 0,
        "waiting_stream_window": 1,
        "scheduled": 2,
        "waiting_quota": 3,
        "waiting": 4,
        "idle_current": 5,
        "on_demand": 6,
        "waiting_credential": 7,
        "deferred": 8,
        "control": 9,
        "registry_alias": 10,
        "not_applicable": 11,
        "not_configured": 12,
        "failed": 13,
        "blocked": 14,
        "unknown": 15,
    }
    eta = _number((row.get("eta") or {}).get("remaining_seconds"))
    next_run = _parse_time((row.get("automation") or {}).get("next_run_at_utc"))
    return (
        _OPERATION_ORDER.get(operation, 99),
        execution_order.get(str(row.get("execution_state") or "unknown"), 99),
        eta if eta is not None else math.inf,
        next_run.timestamp() if next_run is not None else math.inf,
        str(row.get("provider") or "").casefold(),
        str(row.get("title") or "").casefold(),
        str(row.get("id") or ""),
    )


def _enrich_and_sort_rows(
    rows: Iterable[dict[str, Any]],
    *,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for original in rows:
        row = dict(original)
        if row.get("id") == "tw-public:mops_xbrl_quarterly":
            timer_state = refresh_services.get("tw_mops_xbrl", {})
            try:
                expires_on = date.fromisoformat(str(row.get("authorization_expires_on")))
            except (TypeError, ValueError):
                expires_on = None
            row["automation_eligible"] = bool(
                row.get("automation_eligible") is True
                and expires_on is not None and expires_on >= now.astimezone(TAIPEI).date()
                and isinstance(timer_state, Mapping)
                and timer_state.get("timer_active") is True
            )
        publication_hint = row.pop("_publication_hint", None)
        automation = _automation_for_row(
            row, now=now, refresh_services=refresh_services
        )
        operation, execution, reason = _operation_state(row, automation)
        publication = _publication_for_row(
            row,
            automation=automation,
            hint=(publication_hint if isinstance(publication_hint, Mapping) else None),
        )
        row["endpoint_id"] = str(row.get("id") or "")
        row["operation_state"] = operation
        row["operation_label"] = _OPERATION_LABELS[operation]
        row["operation_rank"] = _OPERATION_ORDER[operation]
        row["execution_state"] = execution
        row["operation_reason"] = reason
        row["in_active_scope"] = operation not in {
            "deferred",
            "control",
            "reference",
        }
        row["is_latest"] = operation == "streaming" or (operation == "complete")
        row["last_verified_at_utc"] = row.get("latest_at_utc")
        row["automation"] = automation
        row["publication"] = publication
        _normalize_eta(
            row,
            operation=operation,
            execution=execution,
            now=now,
        )
        row["acquisition_progress"] = _acquisition_progress(
            row,
            operation=operation,
            execution=execution,
            publication=publication,
        )
        enriched.append(row)
    enriched.sort(key=_row_sort_key)
    for index, row in enumerate(enriched, start=1):
        row["sort_index"] = index
    return enriched


def _provider_summaries(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get("provider") or "其他"), []).append(row)
    output = []
    for provider, items in buckets.items():
        counts: dict[str, int] = {}
        operation_counts: dict[str, int] = {}
        for item in items:
            status = str(item.get("status") or "unavailable")
            counts[status] = counts.get(status, 0) + 1
            operation = str(item.get("operation_state") or "unable")
            operation_counts[operation] = operation_counts.get(operation, 0) + 1
        worst = max(counts, key=lambda value: _STATUS_PRIORITY.get(value, 99))
        output.append(
            {
                "provider": provider,
                "status": worst,
                "registered": len(items),
                "status_counts": counts,
                "operation_state_counts": operation_counts,
            }
        )
    return sorted(output, key=lambda row: (-row["registered"], row["provider"].lower()))


def _monitor_integrity_checks(
    rows: list[Mapping[str, Any]],
    *,
    active_data_endpoints: int,
) -> dict[str, Any]:
    """Publish executable cross-field invariants for the monitor itself."""

    endpoint_ids = [str(row.get("id") or "") for row in rows]
    group_ids = {
        str(row.get("id") or "") for row in rows if row.get("scope") == "storage_group"
    }
    logical = [row for row in rows if row.get("scope") != "storage_group"]

    def ratio(row: Mapping[str, Any], key: str) -> float | None:
        payload = row.get(key)
        return _number(payload.get("ratio")) if isinstance(payload, Mapping) else None

    counts = {
        "duplicate_endpoint_ids": len(endpoint_ids) - len(set(endpoint_ids)),
        "orphan_parent_ids": sum(
            bool(parent := str(row.get("parent_id") or "")) and parent not in group_ids
            for row in logical
        ),
        "complete_with_stale_freshness": sum(
            row.get("operation_state") == "complete"
            and (row.get("freshness") or {}).get("state") == "stale"
            for row in rows
        ),
        "complete_with_incomplete_coverage": sum(
            row.get("operation_state") == "complete"
            and ratio(row, "coverage") is not None
            and (ratio(row, "coverage") or 0.0) < 1.0
            for row in rows
        ),
        "noncomplete_with_complete_eta": sum(
            row.get("operation_state") != "complete"
            and (row.get("eta") or {}).get("state") == "complete"
            for row in rows
        ),
        "blocked_or_catching_with_full_progress_bar": sum(
            row.get("operation_state") in {"unable", "catching_up"}
            and ratio(row, "acquisition_progress") is not None
            and (ratio(row, "acquisition_progress") or 0.0) >= 1.0
            for row in rows
        ),
        "registry_alias_not_reference": sum(
            row.get("registry_alias") is True
            and row.get("operation_state") != "reference"
            for row in rows
        ),
        "inactive_state_marked_active": sum(
            row.get("operation_state") in {"deferred", "control", "reference"}
            and row.get("in_active_scope") is not False
            for row in rows
        ),
        "active_denominator_mismatch": abs(
            sum(row.get("in_active_scope") is True for row in logical)
            - active_data_endpoints
        ),
    }
    violations = sum(counts.values())
    return {
        "state": "pass" if violations == 0 else "fail",
        "violations": violations,
        "checks": counts,
        "basis": "由最終公開 DTO 逐列重算；零代表狀態、時效、完整度、ETA 與分母契約沒有已知矛盾。",
    }


def _market_category(row: Mapping[str, Any]) -> str:
    row_id = str(row.get("id") or "")
    parent = str(row.get("parent_id") or "")
    if row_id.startswith("inventory:pepperstone:"):
        return {
            "forex": "forex", "crypto": "crypto",
            "commodities": "macro", "other": "cross_market",
        }.get(row_id.rsplit(":", 1)[-1], "cross_market")
    if row_id.startswith("inventory:yahoo:"):
        return {
            "tw_stocks": "taiwan_equity", "us_stocks": "global_equity",
            "crypto": "crypto", "forex": "forex",
        }.get(row_id.rsplit(":", 1)[-1], "cross_market")
    if row_id.startswith("inventory:tw-public:"):
        return "taiwan_equity"
    if row_id == "inventory:legacy:stock-features":
        return "taiwan_equity"
    if row.get("scope") == "credential_gate":
        return "configuration"
    if row_id.startswith("tw-public:"):
        source = row_id.partition(":")[2]
        if source.startswith(("taifex_",)):
            return "taiwan_derivatives"
        if source.startswith(("cbc_", "dgbas_", "mof_")):
            return "macro"
        if source.startswith(("twse_", "tpex_", "mops_", "tdcc_", "sitca_", "tw_", "data_gov_tdcc_")):
            return "taiwan_equity"
        return "taiwan_public"
    if row_id.startswith("product:"):
        product = row_id.split(":", 2)[1]
        if product.startswith("tw_index"):
            return "taiwan_derivatives"
        if product.startswith("tw_listed"):
            return "taiwan_equity"
        if product.startswith("yahoo_crypto") or product in {
            "okx_perpetual_swaps", "bybit_perpetuals", "binance_usdm_perpetuals",
            "coinbase_spot", "kraken_spot", "bitfinex_spot_derivatives",
            "hyperliquid_perpetuals", "deribit_derivatives",
        }:
            return "crypto"
        if product.startswith("yahoo_global") or product.startswith("alpaca_"):
            return "global_equity"
        if product.startswith(("fx_", "pepperstone_")):
            return "forex"
    group = row_id.removeprefix("group:") if row_id.startswith("group:") else parent.removeprefix("group:")
    if group in _GROUP_MARKET_CATEGORY:
        return _GROUP_MARKET_CATEGORY[group]
    source_text = " ".join(str(row.get(key) or "") for key in ("id", "provider", "title")).lower()
    if any(token in source_text for token in ("crypto", "binance", "bybit", "okx", "dune", "coingecko", "bitcoin", "ethereum", "defi")):
        return "crypto"
    if any(token in source_text for token in ("forex", "frankfurter", "pepperstone", "匯率")):
        return "forex"
    return "cross_market"


def _record_stats_for_row(
    row: Mapping[str, Any], inventory: Mapping[str, Any]
) -> dict[str, Any]:
    if str(row.get("id") or "").startswith("finlab:") and isinstance(row.get("record_stats"), Mapping):
        return dict(row["record_stats"])
    row_id = str(row.get("id") or "")
    provisional_stats = row.get("_provisional_feature_stats")
    if row_id.startswith("tw-public:provisional-feature:") and isinstance(provisional_stats, Mapping):
        return {
            "count": _integer(provisional_stats.get("count")),
            "first": provisional_stats.get("first"),
            "last": provisional_stats.get("last"),
            "files_inspected": 1, "files_total": 1, "state": "verified",
            "basis": "候選事件總帳的建置收據：首末為資料所屬期間，不是實際發布時刻；各欄與總帳重複，不可加總。",
        }
    if row.get("scope") == "credential_gate" or row.get("implementation") == "not_available":
        return {
            "count": None, "first": None, "last": None,
            "files_inspected": 0, "files_total": None, "state": "not_applicable",
            "basis": "此列是設定或來源能力契約，不是已儲存資料集。",
        }
    key = str(row.get("record_inventory_key") or row_id)
    if row_id.startswith("product:") and row.get("granularity") == "1m":
        product = row_id.split(":", 2)[1]
        key = {
            "okx_perpetual_swaps": "group:okx",
            "bybit_perpetuals": "group:bybit",
            "binance_usdm_perpetuals": "group:binance",
        }.get(product, row_id)
    elif row_id == "product:yahoo_crypto_spot:daily":
        key = "yahoo:crypto"
    elif row_id == "product:yahoo_global_equities:daily":
        key = "yahoo:equities"
    key = {
        "product:tw_index_futures:daily": "group:tw-index-futures",
        "product:tw_index_options:daily": "group:tw-index-options-daily",
        "free-source:taifex_public_history": "group:taifex-public-history",
        "dune-query:dune_cex_labeled_flows_daily_v1": "physical:dune:cex-flows",
        "dune-query:dune_dex_asset_activity_daily_v1": "physical:dune:dex-activity",
        "dune-query:dune_stablecoin_issuance_daily_v1": "physical:dune:stablecoin",
    }.get(row_id, key)
    stats = inventory.get(key)
    if isinstance(stats, Mapping):
        result = dict(stats)
        result["basis"] = (
            str(result.get("basis") or "")
            + (f" 時間欄位：{result['time_column']}。" if result.get("time_column") else "")
            + (" 此列沿用實體檔案統計，不可與母群組重複加總。" if key != row_id else "")
        )
        return result
    return {
        "count": None, "first": None, "last": None,
        "files_inspected": 0, "files_total": None, "state": "unverified",
        "basis": {
            "group:tw-minute-source-cold": "原始 Shioaji 回補 chunks 存在重疊；未建立去重後清冊前不能加總。",
            "group:tw-microstructure-captures-cold": "即時原始落盤與研究分區分屬不同層，尚未建立不重疊的 capture 主表清冊。",
            "group:tw-shioaji-history": "契約原始查詢、補洞與衍生表可能重疊；尚未建立逐契約主表清冊。",
            "group:tw-futures": "期貨原始 KBar／Tick、補洞和衍生視圖並存；尚未建立不重疊的主表清冊。",
            "group:openbb-task-shards-local": "可續傳任務分片會被端點封存壓實；與 compact 重複，不加總原始分片。",
            "group:crypto-reference": "同一來源有連續快照；快照列數不是唯一資產事實，尚未定義去重口徑。",
            "group:legacy-parquet": "舊版封存可能與現行來源重疊；不將兩份副本相加。",
        }.get(
            row_id,
            "尚無對應的實體檔案清冊；批次下載列數或要求日期不能冒充庫存總筆數與實際首末筆。",
        ),
    }


def _yahoo_inventory_rows() -> list[dict[str, Any]]:
    labels = {
        "tw_stocks": "台股日資料", "us_stocks": "海外股票／ETF 日資料",
        "crypto": "加密貨幣現貨日資料", "forex": "外匯日資料",
    }
    return [
        {
            "id": f"inventory:yahoo:{asset_class}",
            "parent_id": "group:yahoo-market",
            "scope": "inventory_partition",
            "title": f"Yahoo · {label}",
            "provider": "yfinance / Yahoo Finance",
            "category": "physical_inventory",
            "status": "legacy",
            "status_label": "實存 Parquet 分類清冊；下載狀態請看母群組",
            "cadence": "隨檔案清冊更新",
            "update_owner": "Yahoo OHLCV 更新器",
            "latest_at_utc": None,
            "data_through": None,
            "freshness": {"state": "unknown", "age_seconds": None},
            "coverage": None,
            "eta": _not_applicable_eta("reference", "此列僅按資產分類展示庫存，不另起下載任務。"),
            "rows": None,
            "publishable": False,
            "automation_eligible": False,
            "registry_alias": True,
            "record_inventory_key": f"yahoo:{asset_class}",
            "detail": "這是 Yahoo 物理檔案分類，不重複計入下載完成率。",
            "warnings": [],
            "detail_link": None,
        }
        for asset_class, label in labels.items()
    ]


def _physical_inventory_rows() -> list[dict[str, Any]]:
    """Expose every selected physical table without adding a downloader task."""

    return [
        {
            "id": f"inventory:{family}",
            "parent_id": f"group:{group}",
            "scope": "physical_inventory",
            "title": label,
            "provider": _GROUP_META.get(group, {}).get("provider", "公開資料"),
            "category": "physical_inventory",
            "status": "legacy",
            "status_label": "實存表清冊；下載狀態請看母群組",
            "cadence": "隨檔案清冊更新",
            "update_owner": "實體 Parquet 清冊",
            "latest_at_utc": None,
            "data_through": None,
            "freshness": {"state": "unknown", "age_seconds": None},
            "coverage": None,
            "eta": _not_applicable_eta("reference", "此列只展示實體表，不另起下載任務。"),
            "rows": None,
            "publishable": False,
            "automation_eligible": False,
            "registry_alias": True,
            "record_inventory_key": f"physical:{family}",
            "detail": (
                "群組主表；納入群組實存列數，與衍生表分開顯示。"
                if primary else "衍生或輔助視圖；不重複納入群組實存列數。"
            ),
            "warnings": [],
            "detail_link": None,
        }
        for family, (group, label, _, primary) in PHYSICAL_FAMILIES.items()
    ]


def _apply_verified_storage_freshness(
    rows: Iterable[dict[str, Any]],
    inventory: Mapping[str, Any],
    *,
    now: datetime,
) -> None:
    """Prevent a requested 1m end date from passing as observed storage."""

    for row in rows:
        row_id = str(row.get("id") or "")
        group = row_id if row_id in {"group:okx", "group:bybit", "group:binance"} else {
            "product:okx_perpetual_swaps:1m": "group:okx",
            "product:bybit_perpetuals:1m": "group:bybit",
            "product:binance_usdm_perpetuals:1m": "group:binance",
        }.get(row_id)
        if group is None:
            continue
        stats = inventory.get(group)
        if not isinstance(stats, Mapping) or stats.get("state") != "verified":
            continue
        actual = str(stats.get("last") or "")
        actual_time = _parse_time(actual)
        target = str(row.get("data_through") or "")
        target_time = _parse_time(target)
        if actual_time is None or target_time is None:
            continue
        row["requested_data_through"] = target
        row["data_through"] = actual
        row["freshness"] = _freshness(actual_time, now=now, window_seconds=6 * 3600)
        behind_target = actual_time.date() < target_time.date()
        behind_clock = row["freshness"]["state"] == "stale"
        if not behind_target and not behind_clock:
            continue
        warning = (
            f"下載摘要截止日 {target} 不等於實存最新 1 分 K {actual}；"
            "尚未到最新，不能以摘要日期宣告完成。"
        )
        row["warnings"] = [*row.get("warnings", []), warning]
        if row.get("status") in {"current", "complete"}:
            row["status"] = "stale"
            row["status_label"] = "實存 1 分 K 落後於摘要／時鐘"
            row["eta"] = _unknown_eta(
                "waiting_schedule", "等待原更新器續抓；實存尾端沒有可量測吞吐率。"
            )


def build_data_monitor_public_status(
    repo_root: Path,
    *,
    now: datetime | None = None,
    refresh_services: Mapping[str, Mapping[str, Any]] | None = None,
    shioaji_status: Mapping[str, Any] | None = None,
    openbb_status: Mapping[str, Any] | None = None,
    refresh_inventory: bool = False,
    inventory_max_refresh_files: int = 4_096,
    record_inventory: Mapping[str, Any] | None = None,
    timing_ms: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Build the complete public registry and current monitor projection."""

    stage_started = clock.perf_counter()

    def mark_stage(name: str) -> None:
        nonlocal stage_started
        if timing_ms is not None:
            completed = clock.perf_counter()
            timing_ms[name] = round((completed - stage_started) * 1_000, 3)
            stage_started = completed

    root = Path(repo_root)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    if record_inventory is not None and refresh_inventory:
        raise ValueError("cannot refresh an already observed record inventory")
    if record_inventory is None:
        record_inventory = build_record_inventory(
            root,
            refresh=refresh_inventory,
            max_refresh_files=inventory_max_refresh_files,
        )
    inventory_datasets = record_inventory["datasets"]
    registry = _read_json(root / "configs/data_sync/packed_datasets.json", {})
    configs = registry.get("datasets", []) if isinstance(registry, Mapping) else []
    groups = [
        _generic_group(root, config, now=observed)
        for config in configs
        if isinstance(config, Mapping)
    ]
    mark_stage("inventory_and_groups")
    shioaji_timing_ms: dict[str, float] = {}
    shioaji = (
        dict(shioaji_status)
        if isinstance(shioaji_status, Mapping)
        else build_shioaji_public_status(
            root, timing_ms=shioaji_timing_ms if timing_ms is not None else None
        )
    )
    mark_stage("shioaji_status")
    if timing_ms is not None:
        timing_ms.update({f"shioaji.{key}": value for key, value in shioaji_timing_ms.items()})
    openbb = (
        dict(openbb_status)
        if isinstance(openbb_status, Mapping)
        else build_openbb_public_status(root)
    )
    mark_stage("openbb_status")
    service_states = (
        _refresh_service_states(
            snapshot_path=(root / "artifacts/live/data_monitor/refresh_services.json"),
            now=observed,
            prefer_snapshot=True,
        )
        if refresh_services is None
        else {str(key): dict(value) for key, value in refresh_services.items()}
    )
    runtime_progress = _runtime_progress(root)
    mark_stage("service_state_and_runtime_progress")
    _specialize_groups(
        groups,
        shioaji=shioaji,
        openbb=openbb,
        root=root,
        now=observed,
        refresh_services=service_states,
        runtime_progress=runtime_progress,
    )
    mark_stage("specialize_groups")
    finmind_summary = _read_json(root / "data_finmind/status.json", {})
    finmind_complement = _read_json(root / "data_finmind/complement/status.json", {})
    finmind_sponsor = _read_json(root / "data_finmind/sponsor/status.json", {})
    finmind_service = service_states.get("finmind_free", {})
    finmind_complement_service = service_states.get("finmind_complement", {})
    finmind_sponsor_service = service_states.get("finmind_sponsor", {})
    for group in groups:
        if group.get("id") != "group:finmind-free":
            continue
        complete = _integer(finmind_summary.get("complete_session_day_tasks")) or 0
        total = _integer(finmind_summary.get("total_session_day_tasks")) or 0
        extra = finmind_complement.get("series") if isinstance(finmind_complement.get("series"), Mapping) else {}
        delegated = set(finmind_complement.get("delegated_to_sponsor", [])) if isinstance(finmind_complement.get("delegated_to_sponsor"), list) else set()
        primary_extra = {name: item for name, item in extra.items() if name not in delegated and isinstance(item, Mapping)}
        extra_complete = sum(_integer(item.get("complete")) or 0 for item in primary_extra.values())
        extra_empty = sum(_integer(item.get("observed_empty")) or 0 for item in primary_extra.values())
        extra_total = sum(_integer(item.get("target")) or 0 for item in primary_extra.values())
        extra_blocked = sum((_integer(item.get("not_entitled")) or 0) +
                            (_integer(item.get("invalid_request")) or 0)
                            for item in primary_extra.values())
        paid = finmind_sponsor.get("series") if isinstance(finmind_sponsor.get("series"), Mapping) else {}
        paid_complete = sum(_integer(item.get("complete")) or 0 for item in paid.values() if isinstance(item, Mapping))
        paid_empty = sum(_integer(item.get("observed_empty")) or 0 for item in paid.values() if isinstance(item, Mapping))
        paid_total = sum(_integer(item.get("target")) or 0 for item in paid.values() if isinstance(item, Mapping))
        paid_blocked = sum(_integer(item.get("blocked")) or 0 for item in paid.values() if isinstance(item, Mapping))
        state = str(finmind_summary.get("state") or "not_started")
        active = finmind_service.get("active") is True
        extra_active = finmind_complement_service.get("active") is True
        paid_active = finmind_sponsor_service.get("active") is True
        group["coverage"] = _coverage(
            complete + extra_complete + extra_empty + paid_complete + paid_empty,
            total + extra_total + paid_total,
            unit="已建立任務", label="Free 與 Sponsor 已查驗分區（含空回）"
        ) if total + extra_total + paid_total else None
        group["status"] = (
            "degraded" if state.startswith("calendar_") or state in {"not_entitled", "invalid_token", "invalid_request", "ip_banned"} or extra_blocked or paid_blocked
            else "updating" if (active and state in {"running", "backfilling"}) or
                               (extra_active and finmind_complement.get("state") == "running") or
                               (paid_active and finmind_sponsor.get("state") == "running")
            else "current" if state == "current"
            else "waiting" if active or extra_active or paid_active else "deferred"
        )
        group["status_label"] = (
            f"盤中 {complete:,}/{total:,} 日；Free {extra_complete + extra_empty:,}/{extra_total:,}；Sponsor {paid_complete + paid_empty:,}/{paid_total:,}；受阻 {extra_blocked + paid_blocked:,}"
            if total or extra_total or paid_total else "尚未取得來源日曆與清冊"
        )
        group["latest_at_utc"] = finmind_summary.get("observed_at_utc")
        group["rows"] = sum(
            _integer(item.get("rows")) or 0
            for item in (finmind_summary.get("series") or {}).values()
            if isinstance(item, Mapping)
        ) + sum(_integer(item.get("rows")) or 0 for item in extra.values() if isinstance(item, Mapping)) + sum(_integer(item.get("rows")) or 0 for item in paid.values() if isinstance(item, Mapping)) if isinstance(finmind_summary.get("series"), Mapping) else None
        group["automation_eligible"] = active or extra_active or paid_active
        group["acquisition_enabled"] = active or extra_active or paid_active
        group["eta"] = _unknown_eta(
            "running_unmeasured" if group["status"] == "updating" else "waiting_schedule",
            "FinMind 三支 worker 共用官方帳號額度；回應大小、權限與空回使全範圍完工時間未知。",
        )
        group["warnings"] = [
            *group.get("warnings", []),
            "FinMind 新聞依使用者要求不下載；原始歷史尚未驗證 PIT 可訓練性。",
        ]
    finmind_sources = _finmind_free_sources(
        root, now=observed, service=finmind_service,
        complement_service=finmind_complement_service,
    )
    finlab_sources = _finlab_candidate_sources(root, now=observed)
    finlab_acquisition = _finlab_acquisition_status(
        root, finlab_sources,
        service=service_states.get("finlab_local", {}), now=observed,
    )
    for group in groups:
        if group.get("id") != "group:finlab-research":
            continue
        total = finlab_acquisition["catalog_total"]
        downloaded = finlab_acquisition["downloaded"]
        group["coverage"] = _coverage(
            downloaded, total, unit="目錄鍵", label="FinLab 帳號下載覆蓋率"
        )
        group["status"] = {
            "catalog_downloaded_not_pit_validated": "current",
            "running": "updating",
            "scheduled": "waiting",
            "waiting_quota": "waiting",
            "service_failed": "degraded",
            "timer_disabled": "deferred",
            "catalog_unknown": "degraded",
        }[finlab_acquisition["state"]]
        group["status_label"] = (
            f"已下載 {downloaded:,}/{total:,} 個目錄鍵；歷史 PIT 尚未驗證"
            if total is not None else "SDK 目錄未核實；下載總進度未知"
        )
        group["latest_at_utc"] = finlab_acquisition["last_receipt_at_utc"]
        group["freshness"] = _freshness(
            _parse_time(finlab_acquisition["last_receipt_at_utc"]),
            now=observed, window_seconds=48 * 3600,
        )
        group["automation_eligible"] = finlab_acquisition["timer_active"]
        group["acquisition_enabled"] = finlab_acquisition["timer_active"]
        group["eta"] = finlab_acquisition["eta"]
        group["detail"] = finlab_acquisition["basis"]
        group["warnings"] = [
            *group.get("warnings", []),
            "原始值仍僅供本機研究；目錄下載率不代表歷史完整、當時可用或可跨主機複製。",
        ]
    mark_stage("finlab_sources_and_acquisition")
    finlab_group = next((row for row in groups if row.get("id") == "group:finlab-research"), None)
    finlab_catalog_endpoint = (
        {
            **finlab_group,
            "id": "finlab:catalog-acquisition",
            "parent_id": "group:finlab-research",
            "scope": "source_registry",
            "title": "FinLab SDK 目錄逐鍵下載",
            "registry_alias": False,
            "record_stats": {
                "count": None, "first": None, "last": None,
                "files_inspected": 0, "files_total": None,
                "state": "not_applicable",
                "basis": "此端點的分子／分母是已下載資料鍵／SDK 目錄鍵；原始表列數不可跨鍵加總。",
            },
        }
        if finlab_group is not None else None
    )
    history_logical = _crypto_history_sources(root, now=observed)
    logical = (
        _tw_public_sources(root, now=observed)
        + _shioaji_sources(shioaji, now=observed)
        + _openbb_sources(openbb, now=observed)
        + _crypto_feature_sources(root, now=observed)
        + _credential_registry_sources(root, now=observed)
        + _product_granularity_sources(root, now=observed)
        + _crypto_acquisition_sources(root, now=observed)
        + _free_public_registry_sources(root, now=observed)
        + finmind_sources
        + ([finlab_catalog_endpoint] if finlab_catalog_endpoint is not None else [])
        + finlab_sources
        + _yahoo_inventory_rows()
        + history_logical
    )
    mark_stage("logical_sources")
    _apply_verified_storage_freshness(
        groups + logical, inventory_datasets, now=observed
    )
    logical_for_rollup = _enrich_and_sort_rows(
        logical,
        now=observed,
        refresh_services=service_states,
    )
    _rollup_storage_groups(groups, logical_for_rollup)
    # Child operation/publication state was already computed for the group
    # roll-up above. Reuse that same immutable-in-this-build observation;
    # enriching logical rows again repeats registry/timer work and can even
    # observe a different source state in one published snapshot.
    rows = _enrich_and_sort_rows(
        groups + _physical_inventory_rows(),
        now=observed,
        refresh_services=service_states,
    )
    mark_stage("enrich_and_rollup")
    rows.extend(logical_for_rollup)
    rows.sort(key=_row_sort_key)
    for index, row in enumerate(rows, start=1):
        row["sort_index"] = index
    for row in rows:
        category = _market_category(row)
        row["market_category"] = category
        row["market_category_label"] = _MARKET_CATEGORY_LABELS[category]
        row["record_stats"] = _record_stats_for_row(row, inventory_datasets)
    groups = [row for row in rows if row.get("scope") == "storage_group"]
    logical = [row for row in rows if row.get("scope") != "storage_group"]
    status_counts: dict[str, int] = {}
    operation_counts = {state: 0 for state in _OPERATION_ORDER}
    for row in rows:
        status = str(row.get("status") or "unavailable")
        status_counts[status] = status_counts.get(status, 0) + 1
        operation = str(row.get("operation_state") or "unable")
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
    active_data_rows = [
        row
        for row in logical
        if row.get("operation_state") not in {"deferred", "control", "reference"}
    ]
    data_operation_counts = {state: 0 for state in _OPERATION_ORDER}
    for row in active_data_rows:
        operation = str(row.get("operation_state") or "unable")
        data_operation_counts[operation] = data_operation_counts.get(operation, 0) + 1
    group_operation_counts = {state: 0 for state in _OPERATION_ORDER}
    for row in groups:
        operation = str(row.get("operation_state") or "unable")
        group_operation_counts[operation] = group_operation_counts.get(operation, 0) + 1
    attention = data_operation_counts["unable"]
    healthy = len(active_data_rows) - attention
    worst_status = max(
        (str(row.get("status") or "unavailable") for row in active_data_rows),
        key=lambda value: _STATUS_PRIORITY.get(value, 99),
        default="unavailable",
    )
    if attention:
        health = (
            "critical" if worst_status in {"blocked", "unavailable"} else "degraded"
        )
    elif data_operation_counts["catching_up"]:
        health = "updating"
    else:
        health = "active"
    known_rows = sum(
        value
        for value in (_integer(row.get("rows")) for row in groups)
        if value is not None
    )
    timing_defined = sum(
        bool(str((row.get("automation") or {}).get("schedule_label") or "").strip())
        for row in rows
    )
    automatic = sum(
        (row.get("automation") or {}).get("automatic_update") is True for row in rows
    )
    next_run_known = sum(
        bool((row.get("automation") or {}).get("next_run_at_utc")) for row in rows
    )
    systemd_next_run_known = sum(
        (row.get("automation") or {}).get("next_run_basis") == "systemd_timer"
        for row in rows
    )
    publication_detected = sum(
        bool((row.get("publication") or {}).get("detected_at_utc")) for row in rows
    )
    publication_exact = sum(
        (row.get("publication") or {}).get("exact_time_declared") is True
        for row in rows
    )
    progress_denominator_known = sum(
        _number((row.get("acquisition_progress") or {}).get("ratio")) is not None
        for row in rows
    )
    first_data_observed = sum(
        (row.get("acquisition_progress") or {}).get("first_data_observed") is True
        for row in rows
    )
    preparing_next_date = sum(
        bool((row.get("acquisition_progress") or {}).get("preparing_for_date"))
        for row in rows
    )
    deferred_count = sum(row.get("operation_state") == "deferred" for row in logical)
    control_count = sum(row.get("operation_state") == "control" for row in logical)
    reference_count = sum(row.get("operation_state") == "reference" for row in logical)
    credential_rows = [row for row in logical if row.get("scope") == "credential_gate"]
    credential_ready = sum(
        row.get("credential_state") == "configured" for row in credential_rows
    )
    active_scope_count = len(active_data_rows)
    completed_in_scope = data_operation_counts["complete"]
    integrity_checks = _monitor_integrity_checks(
        rows,
        active_data_endpoints=active_scope_count,
    )
    mark_stage("row_stats_and_integrity")
    physical_stats = [
        stats for key, stats in inventory_datasets.items()
        if (
            (key.startswith(("tw-public:", "yahoo:", "physical:")) and key != "yahoo:equities")
            or key in {
                "group:tw-minute-train", "group:tw-microstructure-train",
                "group:okx", "group:bybit", "group:binance",
                "group:forex-frankfurter", "group:coinmetrics-community",
            }
        )
    ]
    return {
        "schema_version": DATA_MONITOR_SCHEMA_VERSION,
        "generated_at_utc": _iso(observed),
        "health": health,
        "read_only": True,
        "production_control_possible": False,
        "summary": {
            "registered_items": len(rows),
            "storage_groups": len(groups),
            "logical_sources": len(logical),
            "product_granularities": sum(
                row.get("scope") == "product_granularity" for row in logical
            ),
            "credential_gates": sum(
                row.get("scope") == "credential_gate" for row in logical
            ),
            "crypto_fact_families": sum(
                row.get("scope") == "crypto_fact_family" for row in logical
            ),
            "healthy_or_progressing": healthy,
            "attention_required": attention,
            "catching_up": data_operation_counts["catching_up"],
            "streaming": data_operation_counts["streaming"],
            "completed": completed_in_scope,
            "deferred": deferred_count,
            "control_items": control_count,
            "reference_items": reference_count,
            "contract_violations": integrity_checks["violations"],
            "credential_ready": credential_ready,
            "credential_attention": len(credential_rows) - credential_ready,
            "active_scope_items": active_scope_count,
            "active_data_endpoints": active_scope_count,
            "group_rollups": len(groups),
            "unable": data_operation_counts["unable"],
            "known_group_rows": known_rows,
            "verified_inventory_items": sum(
                row["record_stats"]["state"] == "verified" for row in rows
            ),
            "inventory_applicable_items": sum(
                row["record_stats"]["state"] != "not_applicable" for row in rows
            ),
            "physical_inventory_items": len(physical_stats),
            "physical_inventory_verified_items": sum(
                stats.get("state") == "verified" for stats in physical_stats
            ),
            "physical_inventory_time_bounded_items": sum(
                stats.get("state") == "verified"
                and stats.get("first") is not None
                and stats.get("last") is not None
                for stats in physical_stats
            ),
            "physical_inventory_invalid_items": sum(
                stats.get("state") == "invalid" for stats in physical_stats
            ),
            "status_counts": status_counts,
            "operation_state_counts": operation_counts,
            "data_endpoint_state_counts": data_operation_counts,
            "group_state_counts": group_operation_counts,
            "source_level_ratio": (
                (completed_in_scope + data_operation_counts["streaming"])
                / active_scope_count
                if active_scope_count
                else 0.0
            ),
        },
        "endpoint_inventory": {
            "total": len(rows),
            "active_scope_total": active_scope_count,
            "deferred": deferred_count,
            "control": control_count,
            "reference": reference_count,
            "group_rollups": len(groups),
            "ordered_states": [
                {"state": state, "label": _OPERATION_LABELS[state], "rank": rank}
                for state, rank in _OPERATION_ORDER.items()
            ],
            "state_counts": operation_counts,
            "timing_defined": timing_defined,
            "timing_coverage_ratio": timing_defined / len(rows) if rows else 1.0,
            "automatic_update_registered": automatic,
            "explicit_nonautomatic": len(rows) - automatic,
            "next_planned_at_known": next_run_known,
            "systemd_exact_next_run_known": systemd_next_run_known,
            "declared_calendar_next_run_known": (
                next_run_known - systemd_next_run_known
            ),
            "publication_detected": publication_detected,
            "publication_exact_time_declared": publication_exact,
            "publication_cadence_or_boundary_only": len(rows) - publication_exact,
            "progress_denominator_known": progress_denominator_known,
            "first_data_observed": first_data_observed,
            "preparing_next_data_date": preparing_next_date,
            "sort_contract": (
                "operation_rank, execution_state, measured_eta, next_run, "
                "provider, title, endpoint_id"
            ),
            "display_sort_contract": (
                "market_category (Taiwan first, crypto last), provider, "
                "operation_rank, execution_state, measured_eta, next_run"
            ),
        },
        "provider_summaries": _provider_summaries(rows),
        "market_categories": [
            {
                "id": category,
                "label": label,
                "items": sum(row["market_category"] == category for row in rows),
                "verified_items": sum(
                    row["market_category"] == category
                    and row["record_stats"]["state"] == "verified"
                    for row in rows
                ),
                "unverified_items": sum(
                    row["market_category"] == category
                    and row["record_stats"]["state"] in {"unverified", "scanning", "empty"}
                    for row in rows
                ),
                "invalid_items": sum(
                    row["market_category"] == category
                    and row["record_stats"]["state"] == "invalid"
                    for row in rows
                ),
            }
            for category, label in _MARKET_CATEGORY_LABELS.items()
        ],
        "record_inventory_progress": {
            "cached_files": record_inventory["cached_files"],
            "refreshed_files": record_inventory["refreshed_files"],
            "identity_rechecked_files": record_inventory.get("identity_rechecked_files", 0),
            "identity_unbound_files": record_inventory.get("identity_unbound_files", 0),
            "inspected_files": sum(int(stats.get("files_inspected") or 0) for stats in physical_stats),
            "selected_files": sum(int(stats.get("files_total") or 0) for stats in physical_stats),
            "invalid_files": sum(int(stats.get("invalid_files") or 0) for stats in physical_stats),
        },
        "integrity_checks": integrity_checks,
        "refresh_services": service_states,
        "active_progress": runtime_progress,
        "tw_public_acquisition": build_tw_public_acquisition_progress(
            root,
            now=observed,
            next_full_scan_at_utc=(
                service_states.get("tw_public_0830", {}).get("next_run_at_utc")
            ),
        ),
        "finlab_acquisition": finlab_acquisition,
        "groups": groups,
        "sources": rows,
        "definitions": {
            "freshness": "最新回執是否落在各來源允許的更新時間窗。",
            "completeness": "該來源以自己的日期、標的、任務或資料集單位稽核；不同單位不相加。",
            "eta": "只有執行中且存在有效吞吐率時才估算；配額、休市或零速率會明示未知。",
            "operation_state": (
                "固定排序為：正在抓／還沒到最新、正在串流、已完成／已到最新、"
                "無法完成、已延後、設定／憑證閘門、清冊參照；後三者不進入資料完成率。"
            ),
            "streaming": (
                "服務常駐不等於正在串流；必須同時位於交易時窗且最近十分鐘有落盤心跳。"
            ),
            "schedule": (
                "next_run_at_utc 取自 systemd timer；完成後間隔、配額回補、"
                "盤別與按需端點則明示其排程契約，不捏造精確時間。"
            ),
            "publication": (
                "發布時間與我方取得排程分開；只有來源明示或版本變更收據才標為發布證據，"
                "掃描邊界與 cadence 不冒充每個端點的官方 SLA。"
            ),
            "acquisition_progress": (
                "進度優先取完整度 receipt/manifest 的分子分母；分母未知時保持未知。"
                "首筆資料到達即顯示下一個日曆資料日，但完整性未通過前不標示完成。"
            ),
            "source_level_progress": (
                "只計算非群組、非憑證、非延後、非清冊別名的主動資料端點；"
                "這是端點狀態比例，不是資料列數完成率。"
            ),
            "realtime_boundary": "即時 Tick／BidAsk 是連續流，沒有總完工日；歷史 Tick 不能重建未曾擷取的五檔委託簿。",
            "record_stats": "最早／最新來自實際 Parquet 時間欄位的 footer 統計；有時區值轉為 UTC，無時區欄位保留來源時間，跨欄位群組只比較日期。總筆數為儲存列數，不等於唯一事件數或發布後可用時間；缺時間統計時僅時間界限未知。",
            "tw_public_boundary": "臺灣官方資料只透過完整稽核後的不可變快照切換，不直接修改已發佈版本。",
        },
    }


def build_tw_public_monitor_status(
    repo_root: Path,
    *,
    now: datetime | None = None,
    refresh_services: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build only the TW official-source rows consumed by the day-trade page.

    The complete data monitor joins every registered market, provider and
    storage roll-up.  Running that multi-second inventory on the public request
    thread used to contend with the latency-sensitive day-trade status route,
    even though the day-trade page renders only the ``group:tw-public`` rows.
    Keep the same source/publication/progress contracts while avoiding all
    unrelated providers and storage scans.
    """

    root = Path(repo_root)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    service_states = (
        _refresh_service_states(
            snapshot_path=(root / "artifacts/live/data_monitor/refresh_services.json"),
            now=observed,
            prefer_snapshot=True,
        )
        if refresh_services is None
        else {str(key): dict(value) for key, value in refresh_services.items()}
    )
    rows = _enrich_and_sort_rows(
        _tw_public_sources(root, now=observed),
        now=observed,
        refresh_services=service_states,
    )
    operation_counts = {state: 0 for state in _OPERATION_ORDER}
    status_counts: dict[str, int] = {}
    for row in rows:
        operation = str(row.get("operation_state") or "unable")
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
        status = str(row.get("status") or "unavailable")
        status_counts[status] = status_counts.get(status, 0) + 1
    attention = operation_counts["unable"]
    worst_status = max(
        (str(row.get("status") or "unavailable") for row in rows),
        key=lambda value: _STATUS_PRIORITY.get(value, 99),
        default="unavailable",
    )
    if attention:
        health = (
            "critical" if worst_status in {"blocked", "unavailable"} else "degraded"
        )
    elif operation_counts["catching_up"]:
        health = "updating"
    else:
        health = "active"
    completed = sum(
        bool(
            (row.get("acquisition_progress") or {}).get("up_to_date")
            or (row.get("acquisition_progress") or {}).get("coverage_complete")
            or (row.get("acquisition_progress") or {}).get("batch_complete")
        )
        for row in rows
    )
    return {
        "schema_version": DATA_MONITOR_SCHEMA_VERSION,
        "generated_at_utc": _iso(observed),
        "health": health,
        "read_only": True,
        "production_control_possible": False,
        "scope": "tw_public_official_sources",
        "summary": {
            "registered_items": len(rows),
            "completed_or_preparing": completed,
            "attention_required": attention,
            "status_counts": status_counts,
            "operation_state_counts": operation_counts,
        },
        "sources": rows,
        "definitions": {
            "scope": (
                "Only official TW public-source rows used by the day-trade page; "
                "the complete cross-provider inventory remains on /data-monitor/."
            ),
            "publication": (
                "Official publication evidence remains separate from local probe, "
                "download and acceptance timestamps."
            ),
            "progress": (
                "Completion uses each source's own receipt-backed denominator; "
                "unknown denominators remain unknown."
            ),
        },
    }


def build_data_monitor_feature_inventory(
    repo_root: Path, *, monitor_status: Mapping[str, Any] | None = None,
    inventory: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Public, field-level projection of the cached physical Parquet inventory."""

    if inventory is None:
        inventory = build_feature_inventory(repo_root)
    status = monitor_status or build_data_monitor_public_status(repo_root)
    source_rows = {
        str(row.get("record_inventory_key") or row.get("id")): row
        for row in status.get("sources", ())
        if isinstance(row, Mapping)
    }
    category_order = {category: index for index, category in enumerate(_MARKET_CATEGORY_LABELS)}
    rows: list[dict[str, Any]] = []
    source_meta: dict[str, tuple[Any, Any, str, str]] = {}
    for field in inventory["rows"]:
        dataset = field["dataset_id"]
        meta = source_meta.get(dataset)
        if meta is None:
            source = source_rows.get(dataset)
            if source is None:
                if dataset.startswith("physical:"):
                    family = dataset.removeprefix("physical:")
                    group, label, _, _ = PHYSICAL_FAMILIES[family]
                    source = {"id": f"inventory:{family}", "parent_id": f"group:{group}",
                              "title": label, "provider": _GROUP_META.get(group, {}).get("provider", "公開資料")}
                else:
                    source = {"id": dataset, "title": dataset, "provider": "公開資料"}
            category = str(source.get("market_category") or _market_category(source))
            meta = (
                source.get("title") or dataset,
                source.get("provider") or "公開資料",
                category,
                _MARKET_CATEGORY_LABELS.get(category, "跨市場／其他"),
            )
            source_meta[dataset] = meta
        source_title, provider, category, category_label = meta
        field_name = str(field["field"])
        conditional_tw_evidence = (
            dataset == "physical:tw-public:stock-features"
            and field_name in {
                "fallback_reason", "raw_ohlc_scale_factor",
                "raw_ohlc_scale_reference_date", "adjustment_reference_date",
                "adjustment_reference_price", "adjustment_reference_kind",
                "ohlc_normalization", "return_quarantine_reason",
                "official_listing_evidence",
            }
        )
        unverified_tw_feature = (
            dataset == "physical:tw-public:training-features"
            and field_name.startswith("twpub_")
            and field.get("non_null_count") == 0
        )
        availability_note = (
            "事件／異常才有值的證據欄；空值不代表日價缺漏。"
            if conditional_tw_evidence else
            "目前建置無可驗證的非空訓練值；需查原始歷史版本與發布時間，不能回填未驗證值。"
            if unverified_tw_feature else None
        )
        rows.append({
            **field,
            "source_title": source_title,
            "provider": provider,
            "market_category": category,
            "market_category_label": category_label,
            "field_role": "conditional" if conditional_tw_evidence else "key" if field["field"] in {
                "date", "trade_date", "trading_date", "ts", "timestamp", "time",
                "event_ts", "event_ts_utc", "snapshot_ts_ns", "symbol", "asset",
                "instrument", "contract", "exchange", "source", "data_source",
            } else "value",
            "availability_note": availability_note,
        })
    rows.sort(key=lambda row: (
        category_order.get(row["market_category"], 99),
        str(row["provider"]), str(row["source_title"]),
        str(row["dataset_id"]), str(row["field"]),
    ))
    return {
        "schema_version": 1,
        "generated_at_utc": status.get("generated_at_utc"),
        "read_only": True,
        "production_control_possible": False,
        "summary": {
            "fields": len(rows),
            "datasets_with_schema": inventory["datasets_with_schema"],
            "datasets_total": inventory["datasets_total"],
            "files_with_schema": inventory["files_with_schema"],
            "files_total": inventory["files_total"],
            "state": inventory["state"],
        },
        "basis": inventory["basis"],
        "definitions": {
            "non_null_count": "僅當所有含此欄位檔案的 row-group null_count 完整且資料集所有檔案已核實，才顯示精確非空值筆數；否則只顯示已核實下界。",
            "dataset_bounds": "首末時間是資料集時間欄位界限，不是此 feature 第一／最後一個非空值，也不是發布或訓練可用時間。",
            "duplicate_scope": "每個實體資料集 × 欄位各列一次；原始表、衍生特徵及來源替代檔互不加總。",
        },
        "rows": rows,
    }


__all__ = [
    "DATA_MONITOR_SCHEMA_VERSION",
    "build_data_monitor_feature_inventory",
    "build_data_monitor_public_status",
    "build_tw_public_monitor_status",
]
