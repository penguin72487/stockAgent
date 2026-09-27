#!/usr/bin/env python3
"""Prepare the explicitly verified TX/MTX monthly margin training release."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from stockagent.data.tw_futures_margin_preparation import (
    RuleArchive, align_rules, build_rule_events, compact, prepare_daily,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=Path("data_taifex_public_history/rules"))
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--official-evidence", type=Path, required=True)
    parser.add_argument("--final-settlement", type=Path, required=True)
    parser.add_argument("--source-reviews", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2021, 5, 20))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 9, 4))
    args = parser.parse_args()
    rules_dir = args.output_dir / "rules"
    if (rules_dir / "manifest.json").exists():
        raise FileExistsError("use a new output directory; completed releases are immutable")
    archive = RuleArchive(args.archive, rules_dir, args.source_reviews)
    margins, positions, audit = build_rule_events(archive, args.start, args.end)
    # The 2017 dated contract amendment establishes the common session clock,
    # +/-10% reference and combined 4:1 limit. Only the TX/MTX pages (11-24)
    # are relied on; the archive's 80-page extraction limit is not waived.
    specs_url = "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/網站公告(5).pdf"
    specs = archive.document(specs_url)
    text = compact(specs["text"])
    for phrase in ("上午八時四十五分至下午一時四十五分", "四比一", "上下各百分之十"):
        if phrase not in text:
            raise ValueError(f"historical contract evidence lost: {phrase}")
    implementation_url = "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/1060418上線新聞稿_修正NCP時間.pdf"
    implementation = archive.document(implementation_url)
    if "5月15日" not in compact(implementation["text"]):
        raise ValueError("historical session implementation date missing")
    frame, summary = prepare_daily(args.daily, args.official_evidence, args.final_settlement,
                                    args.output_dir / "daily", args.start, args.end)
    rules = align_rules(frame, margins, positions)
    daily = args.output_dir / "daily/continuous_daily.parquet"
    rules_path = rules_dir / "rules.parquet"
    atomic_write_parquet(rules_path, rules)
    events_path = rules_dir / "events.json"
    events = dict(margins=margins, positions=positions, announcement_audit=audit)
    atomic_write_json(events_path, events)
    # Include portable derivative proofs. Raw daily source bytes are verified
    # locally by the canonical loader and remain in the original source store.
    for path, kind in ((args.official_evidence, "verified_official_daily_rows"),
                       (args.official_evidence.with_name("official_evidence_manifest.json"), "daily_source_verification"),
                       (args.final_settlement.with_name("manifest.json"), "final_settlement_manifest"),
                       (events_path, "verified_rule_events"),
                       (Path("stockagent/data/tw_price_rules.py"), "dated_price_grid_implementation"),
                       (Path("docs/tw_futures_price_grid_evidence_2026-09-27.md"), "dated_price_grid_evidence")):
        archive.copy(path, sha256_file(path), url="", kind=kind)
    manifest = dict(dataset="taifex_futures_margin_rules", schema_version=1,
        status="complete", point_in_time_verified=True,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        source_daily_sha256=sha256_file(daily), outputs={"rules": {"sha256": sha256_file(rules_path)}},
        sources=list(archive.sources.values()), scope=summary,
        historical_spec_source=specs_url, historical_spec_pages_one_based=[11, 24],
        historical_spec_content_sha256=specs["content_sha256"],
        session_implementation_source=implementation_url,
        rules_known_at_policy="publication_date_235959_upper_bound_with_hash_bound_official_date_confirmation",
        legacy_rule_review=bool(archive.legacy),
        delayed_position_relaxation_policy="retain_previous_stricter_cap_until_publication_day_end; never defer tightening",
        price_limit_policy="TX_MTX_7_percent_before_2015_06_01_then_10_percent",
        close_effective_policy="post_regular_close_account_phase_1345_expiry_cash_settlement_1330",
        position_policy="natural_person_absolute_gross_TX_equivalent_conservative_no_offsets",
        zero_print_policy="official_marks_for_valuation_only_no_new_fills",
        omissions=["all_other_products", "weekly_contracts", "before_" + summary["start"], "first_observation_without_prior_physical_settlement"],
        not_verified=["intraday_and_night_margin_calls", "broker_specific_surcharges_and_notification_times", "SPAN_offsets"],
        builder_sha256=sha256_file(Path("stockagent/data/tw_futures_margin_preparation.py")))
    atomic_write_json(rules_dir / "manifest.json", manifest)
    try:
        validate_margin_rule_source(rules_path, daily)
    except Exception:
        atomic_write_json(rules_dir / "manifest.json", dict(manifest, status="failed", point_in_time_verified=False))
        raise
    summary.update(margin_event_counts={p: len(e) for p, e in margins.items()},
                   position_events=len(positions), official_source_files=len(archive.sources),
                   validated_rules=rules.height, status="data_prepared_remote_training_not_yet_verified")
    atomic_write_json(args.output_dir / "build_summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
