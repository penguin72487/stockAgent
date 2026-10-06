#!/usr/bin/env python3
"""Audit Taiwan source histories with the canonical calendar/lineage contracts.

No panel builds, model checks, provider calls, data rewriting or TEJ inspection.
The shared producer-specific audits distinguish calendar absence, inactive
contracts and source-unavailable receipts from unresolved observations.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
from downloader.artifact_io import atomic_write_json
from scripts.audit_source_anomalies import csv_file
from scripts import audit_tw_public_data_layer as native


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--end-date', required=True)
    args = parser.parse_args()
    root, out = args.root.resolve(), args.output_dir
    public = root / 'data_tw_public'
    calendar, _, _ = native._load_verified_taiex_session_calendar(public)
    sessions = np.asarray(calendar['date'].to_numpy(), dtype='datetime64[D]')
    sessions = sessions[sessions <= np.datetime64(args.end_date)]
    config = native.load_config(root / 'configs/markets/tw_public.yaml')
    print('[source-history] verified_calendar_loaded', flush=True)
    quotes, quote_summary, findings = native.audit_quote_source_files(
        public / 'stocks', sessions, workers=2, require_official=True, source_sessions=sessions)
    print('[source-history] stock_source_values_and_keys_checked', flush=True)
    builds, build_findings = native.audit_official_symbol_build(public / 'stocks', public)
    sources, source_findings = native.audit_historical_sources(public, sessions, config)
    receipts, receipt_findings = native.audit_source_receipts(public, config)
    csv_file(out / 'quotes.csv', quotes)
    csv_file(out / 'historical_sources.csv', [asdict(p) for p in sources])
    csv_file(out / 'findings.csv', [asdict(f) for f in [*findings, *build_findings, *source_findings, *receipt_findings]])
    atomic_write_json(out / 'summary.json', {
        'contract': 'canonical_tw_source_history_audit_v1', 'observed_at_utc': datetime.now(UTC).isoformat(),
        'TEJ_excluded': True, 'network_calls': 0, 'calendar_sessions': len(sessions),
        'calendar_first': str(sessions[0]), 'calendar_last': str(sessions[-1]),
        'quotes': quote_summary, 'official_build': builds, 'source_receipts': receipts,
        'historical_source_tables': len(sources), 'all_source_history_complete': False,
        'limits': ['Native source receipt observations differ from proof of every issuer field',
                   'No training panel or stricter historical feature admission was rebuilt']})
    print('[source-history] audit_finished_not_download_complete', flush=True)


if __name__ == '__main__':
    main()
