#!/usr/bin/env python3
"""Audit narrowly recoverable pre-night-session reference trades.

The output is a source-bound research candidate, not an admitted training clock.
It deliberately does not claim historical provider delivery, execution or fills.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from stockagent.data.tw_futures_decision_clock import opening_reference_contract_from_manifest


NIGHT_START = date(2017, 5, 15)
NOTICE_SHA256 = "5eb4e9e928c47999ef499867875e1062bf117abe8db21658ca0a9935477b1c34"
NOTICE_URL = "https://www.taifex.com.tw/chinese/event/afterhourstrading/4175_001.pdf"
EQUITY_NIGHT_START = {"stock_future": date(2024, 1, 22), "etf_future": date(2022, 6, 27)}
EQUITY_NOTICE_SHA256 = "e32b3482fee476e7d26585e59392d716cc27de49bceaeca80a41555adbcc1d06"
EQUITY_NOTICE_URL = "https://www.taifex.com.tw/file/taifex/CHINESE/10/moth_all/202510_all.pdf"
WEEKEND_NOTICE_SHA256 = "0de9f1708ba06e4d1a8ce81ce7850c30686c5147d4fb899642613f3dec795d9f"
WEEKEND_NOTICE_URL = "https://www.taifex.com.tw/file/taifex/CHINESE/4/%E7%AC%AC%E4%BA%8C%E9%9A%8E%E6%AE%B5%E5%AE%A3%E5%B0%8E%E7%B0%A1%E5%A0%B1%E6%AA%94.pdf"


def preliminary_candidates(references: pl.DataFrame, coverage: pl.DataFrame,
                           final_coverage: pl.DataFrame, trades: pl.DataFrame,
                           daily_classification: pl.DataFrame | None = None,
                           *, weekend: bool = False,
                           zero_afterhours: pl.DataFrame | None = None,
                           early_holiday_sessions: dict[date, date] | None = None) -> pl.DataFrame:
    """Select source-backed recent-trade candidates from the same physical contract.

    A current open, including an ambiguous one, prevents a prior fallback after
    its availability bound. Block trades are not covered by this raw tape, so
    either day's positive block volume excludes the candidate. No assumption
    about absent exchange dates or future listed identities is needed.
    """
    keys = ["date", "physical_contract"]
    if (references.select([*keys, "decision_ts"]).is_duplicated().any()
            or coverage.select(keys).is_duplicated().any()
            or final_coverage.select(keys).is_duplicated().any()):
        raise ValueError("pre-night audit requires unique dated physical identities")
    usable = (coverage.filter(pl.col("verified").fill_null(False)
                             & (pl.col("block_volume") == 0))
        .join(final_coverage.filter(pl.col("verified").fill_null(False)
                                    & (pl.col("block_volume") == 0))
              .select(*keys, pl.col("source_file_sha256").alias("_final_source")),
              on=keys, how="inner", validate="1:1")
        .filter(pl.col("source_file_sha256") == pl.col("_final_source")))
    extended = daily_classification is not None
    zero_night = zero_afterhours is not None
    if early_holiday_sessions is not None:
        if extended or zero_night or weekend or not early_holiday_sessions:
            raise ValueError("early holiday policy must be a separate pre-night scope")
        if any(not isinstance(current, date) or not isinstance(previous, date)
               or not (previous < current < NIGHT_START)
               for current, previous in early_holiday_sessions.items()):
            raise ValueError("invalid early holiday session bounds")
    if extended and zero_night:
        raise ValueError("equity day-only and explicit zero-night policies are distinct")
    if weekend and not (extended or zero_night):
        raise ValueError("weekend policy requires equity or explicit zero-night evidence")
    if zero_night:
        assert zero_afterhours is not None
        zero_keys = ["date", "product", "contract"]
        if (zero_afterhours.is_empty() or zero_afterhours.select(zero_keys).is_duplicated().any()
                or zero_afterhours["source_sha256"].null_count()):
            raise ValueError("explicit zero-night evidence must have unique, sourced physical keys")
        end = zero_afterhours["date"].max() + timedelta(days=1)
    else:
        end = max(EQUITY_NIGHT_START.values()) if extended else NIGHT_START
    if extended:
        assert daily_classification is not None
        if daily_classification.select(keys).is_duplicated().any():
            raise ValueError("duplicate dated physical classification")
        usable = usable.join(daily_classification.select(*keys, "asset_class"),
                             on=keys, how="inner", validate="1:1")
    events = (trades.filter(pl.col("trade_date") < end)
        .join(usable.select(pl.col("date").alias("trade_date"), "physical_contract",
                           pl.col("source_file_sha256").alias("_coverage_source")),
              on=["trade_date", "physical_contract"], how="inner", validate="m:1")
        .filter((pl.col("source_sha256") == pl.col("_coverage_source"))
                & (pl.col("trade_ts").dt.date() == pl.col("trade_date"))
                & (pl.col("available_by").dt.date() == pl.col("trade_date"))))
    event_keys = ["trade_date", "physical_contract"]
    opens = (events.filter(pl.col("is_session_open"))
        .select(pl.col("trade_date").alias("date"), "physical_contract",
                pl.col("available_by").alias("_open_available_by")))
    if opens.select(keys).is_duplicated().any():
        raise ValueError("multiple source opening events for the same physical day")
    # If open and last share a timestamp, prefer the non-opening event. The
    # source's same-second price set is still independently checked below.
    last = (events.sort([*event_keys, "trade_ts", "is_session_open"],
                        descending=[False, False, False, True])
        .group_by(event_keys, maintain_order=True).last()
        .select(pl.col("trade_date").alias("_previous_date"), "physical_contract",
                pl.col("price").alias("reference_quote"),
                pl.col("trade_ts").alias("reference_trade_ts"),
                pl.col("available_by").alias("reference_available_by"),
                pl.col("source_sha256").alias("reference_source_sha256")))
    current = usable.select(*keys, "product", "contract", "outright_volume",
                            *(["asset_class"] if extended else []))
    if zero_night:
        current = current.join(zero_afterhours.select("date", "product", "contract",
            pl.col("source_sha256").alias("night_zero_source_sha256")),
            on=["date", "product", "contract"], how="inner", validate="m:1")
    old = usable.select(pl.col("date").alias("_previous_date"), "physical_contract",
                        pl.col("product").alias("_prior_product"),
                        pl.col("contract").alias("_prior_contract"),
                        *([pl.col("asset_class").alias("_prior_asset_class")] if extended else []))
    date_scope = (pl.col("date") < end) & (pl.col("reference_kind") == "unavailable")
    if zero_night:
        date_scope &= pl.col("date") >= NIGHT_START
    if weekend:
        date_scope &= (pl.col("date") >= date(2019, 1, 7)) & (pl.col("date").dt.weekday() == 1)
    selected_refs = references.filter(date_scope).select(*keys, "symbol", "decision_ts")
    if early_holiday_sessions is not None:
        sessions = pl.DataFrame({"date": list(early_holiday_sessions),
                                 "_previous_date": list(early_holiday_sessions.values())},
                                schema={"date": pl.Date, "_previous_date": pl.Date})
        selected_refs = selected_refs.join(sessions, on="date", how="inner", validate="m:1")
    else:
        selected_refs = selected_refs.with_columns(
            (pl.col("date") - pl.duration(days=3 if weekend else 1)).alias("_previous_date"))
    result = (selected_refs
        .join(current, on=keys, how="inner", validate="m:1")
        .join(opens, on=keys, how="left", validate="m:1")
        .filter((pl.col("_open_available_by").is_not_null() | (pl.col("outright_volume") == 0))
                & (pl.col("_open_available_by").is_null()
                   | (pl.col("_open_available_by") > pl.col("decision_ts"))))
        .join(old, on=["_previous_date", "physical_contract"], how="inner", validate="m:1")
        .filter((pl.col("product") == pl.col("_prior_product"))
                & (pl.col("contract") == pl.col("_prior_contract")))
        .join(last, on=["_previous_date", "physical_contract"], how="inner", validate="m:1")
        .filter(pl.col("reference_available_by") < pl.col("decision_ts")))
    if extended:
        result = result.filter((pl.col("date") >= NIGHT_START)
            & (pl.col("asset_class") == pl.col("_prior_asset_class"))
            & (((pl.col("asset_class") == "stock_future")
                & (pl.col("date") < EQUITY_NIGHT_START["stock_future"]))
               | ((pl.col("asset_class") == "etf_future")
                  & (pl.col("date") < EQUITY_NIGHT_START["etf_future"]))))
    return result.select("date", "physical_contract", "symbol", "decision_ts", "product", "contract",
                "_previous_date", "reference_quote", "reference_trade_ts", "reference_available_by",
                "reference_source_sha256", *(["asset_class"] if extended else []),
                *(["night_zero_source_sha256"] if zero_night else []))


def last_second_proofs(raw: pl.DataFrame, targets: pl.DataFrame) -> pl.DataFrame:
    """Check actual final-second prices; the source has no within-second order."""
    key = ["product", "contract", "reference_trade_ts"]
    if targets.select(key).is_duplicated().any():
        raise ValueError("last-second targets must be unique")
    observed = (raw.filter((pl.col("volume") > 0) & (pl.col("price") > 0))
        .select(pl.col("futures_id").alias("product"), pl.col("contract_date").alias("contract"),
                pl.col("date").alias("_time"), pl.col("price").cast(pl.Float32).alias("_price")))
    latest = observed.group_by("product", "contract").agg(pl.col("_time").max().alias("_latest_time"))
    grouped = (targets.with_columns(pl.col("reference_trade_ts").dt.strftime("%Y-%m-%d %H:%M:%S").alias("_time"))
        .join(observed, on=["product", "contract", "_time"], how="left")
        .group_by(key).agg(pl.col("_price").drop_nulls().n_unique().alias("price_count"),
                           pl.col("_price").drop_nulls().first().alias("_raw_price")))
    return (targets.join(grouped, on=key, how="left", validate="1:1")
        .join(latest, on=["product", "contract"], how="left", validate="m:1")
        .with_columns(((pl.col("price_count") == 1)
                       & (pl.col("reference_quote").cast(pl.Float32) == pl.col("_raw_price"))
                       & (pl.col("reference_trade_ts").dt.strftime("%Y-%m-%d %H:%M:%S") == pl.col("_latest_time")))
                      .fill_null(False).alias("last_second_proven")))


def _pin(path: Path) -> dict:
    return {"path": str(path.resolve()), "sha256": sha256_file(path)}


def build(args) -> dict:
    workspace, output = args.workspace.resolve(), args.output.resolve()
    if (not output.is_relative_to(workspace) or not output.relative_to(workspace).parts[0].startswith("data_")
            or output.exists() or output.is_symlink()):
        raise ValueError("a new data_* output under the workspace is required")
    if sha256_file(args.notice) != NOTICE_SHA256:
        raise ValueError("the dated exchange notice differs from the reviewed source")
    if bool(args.daily_classification) != bool(args.equity_notice):
        raise ValueError("equity classification and the equity night notice must be supplied together")
    extended = bool(args.daily_classification)
    zero_doc_path = args.zero_afterhours_manifest
    zero_night = bool(zero_doc_path)
    if extended and zero_night:
        raise ValueError("equity day-only and explicit zero-night policies are distinct")
    weekend = bool(args.weekend_notice)
    if weekend and not (extended or zero_night):
        raise ValueError("the weekend rule requires equity or explicit zero-night evidence")
    early_holidays, holiday_pins, holiday_implementation = None, [], None
    if args.early_holiday_rules:
        if extended or zero_night or weekend:
            raise ValueError("early holiday policy must be a separate pre-night scope")
        from scripts import tw_futures_early_holiday_evidence
        early_holidays, holiday_pins = tw_futures_early_holiday_evidence.early_holiday_closures(args.early_holiday_rules)
        holiday_implementation = _pin(Path(tw_futures_early_holiday_evidence.__file__))
    if extended and sha256_file(args.equity_notice) != EQUITY_NOTICE_SHA256:
        raise ValueError("the equity night source differs from the reviewed exchange publication")
    if weekend and sha256_file(args.weekend_notice) != WEEKEND_NOTICE_SHA256:
        raise ValueError("the weekend source differs from the reviewed exchange publication")
    zero_data, zero_pins = None, []
    if zero_night:
        zero_doc_path = zero_doc_path.resolve()
        zero_doc = json.loads(zero_doc_path.read_text())
        if (zero_doc.get("contract") != "taifex_explicit_zero_afterhours_source_evidence_v1"
                or zero_doc.get("training_ready") is not False or not zero_doc.get("originals")):
            raise ValueError("unsupported explicit zero-night evidence")
        named_pins = [(zero_doc_path, sha256_file(zero_doc_path)),
                      (Path(zero_doc["data_path"]), zero_doc["data_sha256"]),
                      (Path(zero_doc["source_manifest"]), zero_doc["source_manifest_sha256"]),
                      (Path(zero_doc["source_data"]), zero_doc["source_data_sha256"])]
        named_pins.extend((Path(item["path"]), item["sha256"]) for item in zero_doc["originals"])
        for path, expected in named_pins:
            pin = _pin(path)
            if pin["sha256"] != expected:
                raise ValueError("explicit zero-night input differs from its recorded source")
            zero_pins.append(pin)
        zero_data = pl.read_parquet(zero_doc["data_path"])
        if zero_data.height != zero_doc["rows"]:
            raise ValueError("explicit zero-night row count differs")
    end = ((zero_data["date"].max() + timedelta(days=1)) if zero_night else
           max(EQUITY_NIGHT_START.values()) if extended else NIGHT_START)
    holiday_days = (sorted(set(early_holidays) | set(early_holidays.values()))
                    if early_holidays is not None else None)
    initial_capacity = capacity_check(output.parent)
    checkpoint = args.checkpoint.resolve()
    manifest_path = checkpoint / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("contract") != "verified_bulk_window_checkpoint_v1":
        raise ValueError("unsupported bulk checkpoint")
    for name in ("coverage", "trades"):
        if sha256_file(checkpoint / f"{name}.parquet") != manifest["outputs"][name]["sha256"]:
            raise ValueError(f"checkpoint {name} hash differs")
    references, reference_pins = [], []
    for clock, path in (("08:45", args.early), ("09:00", args.late)):
        ref_path = path.resolve()
        doc_path = ref_path.with_name("opening_reference_manifest.json")
        entry_path = ref_path.with_name("manifest.json")
        doc, entry = json.loads(doc_path.read_text()), json.loads(entry_path.read_text())
        opening_reference_contract_from_manifest(doc, clock)
        if (doc["data_sha256"] != sha256_file(ref_path) or doc["pair_identity"] != entry["pair_identity"]
                or doc["source_daily_sha256"] != entry["source_daily_sha256"]):
            raise ValueError("reference, paired entries and daily identities differ")
        if extended and doc["source_daily_sha256"] != sha256_file(args.daily_classification):
            raise ValueError("classification differs from the paired financial release")
        reference_scan = pl.scan_parquet(ref_path).filter(pl.col("date") < end)
        if early_holidays is not None:
            reference_scan = reference_scan.filter(pl.col("date").is_in(sorted(early_holidays)))
        references.append(reference_scan.collect())
        reference_pins.append({"data": _pin(ref_path), "manifest": _pin(doc_path), "entries": _pin(entry_path),
                               "pair_identity": doc["pair_identity"]})
    if reference_pins[0]["pair_identity"] != reference_pins[1]["pair_identity"]:
        raise ValueError("references are not the same paired release")
    coverage_scan = pl.scan_parquet(checkpoint / "coverage.parquet").filter(pl.col("date") < end)
    final_scan = pl.scan_parquet(args.final_coverage).filter(pl.col("date") < end)
    events_scan = pl.scan_parquet(checkpoint / "trades.parquet").filter(pl.col("trade_date") < end)
    if holiday_days is not None:
        coverage_scan = coverage_scan.filter(pl.col("date").is_in(holiday_days))
        final_scan = final_scan.filter(pl.col("date").is_in(holiday_days))
        events_scan = events_scan.filter(pl.col("trade_date").is_in(holiday_days))
    coverage, final_coverage, events = coverage_scan.collect(), final_scan.collect(), events_scan.collect()
    classification = (pl.scan_parquet(args.daily_classification).filter(pl.col("date") < end)
                      .select("date", "physical_contract", "asset_class").collect() if extended else None)
    preliminary = preliminary_candidates(pl.concat(references), coverage, final_coverage, events, classification,
                                         weekend=weekend, zero_afterhours=zero_data,
                                         early_holiday_sessions=early_holidays)
    if args.limit_days:
        days = sorted(preliminary["_previous_date"].unique().to_list())[:args.limit_days]
        preliminary = preliminary.filter(pl.col("_previous_date").is_in(days))
    targets = preliminary.select("_previous_date", "physical_contract", "product", "contract",
        "reference_trade_ts", "reference_quote", "reference_source_sha256").unique()
    objects = {item["date"]: item for item in manifest["identity"]["objects"]}
    partitions = targets.partition_by("_previous_date", as_dict=True)

    def verify_partition(partition):
        (day,), rows = partition
        item = objects.get(day.isoformat())
        if not item or item.get("source_kind") or any(rows["reference_source_sha256"] != item["sha256"]):
            return rows.with_columns(pl.lit(False).alias("last_second_proven")), {"date": str(day), "state": "source_mismatch"}
        raw_path, receipt = Path(item["path"]), Path(item["receipt_path"])
        if (sha256_file(raw_path) != item["sha256"] or sha256_file(receipt) != item["receipt_sha256"]):
            raise ValueError(f"raw or receipt SHA mismatch for {day}")
        result = last_second_proofs(pl.read_parquet(raw_path), rows)
        if sha256_file(raw_path) != item["sha256"] or sha256_file(receipt) != item["receipt_sha256"]:
            raise ValueError(f"raw or receipt changed during readback for {day}")
        return result, {"date": str(day), "source_sha256": item["sha256"],
                        "receipt_sha256": item["receipt_sha256"], "targets": rows.height,
                        "proved": result.filter(pl.col("last_second_proven")).height}

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for frame, info in pool.map(verify_partition, partitions.items()):
            results.append((frame, info))
            if len(results) % 100 == 0 or len(results) == len(partitions):
                print(json.dumps({"stage": "raw_last_seconds", "dates_checked": len(results),
                                  "dates_total": len(partitions)}), flush=True)
    proven = (pl.concat([frame for frame, _ in results], how="diagonal_relaxed")
              .filter(pl.col("last_second_proven")) if results else targets.head(0))
    selected = (preliminary.join(proven.select("_previous_date", "physical_contract", "reference_trade_ts",
                                                "reference_source_sha256"),
        on=["_previous_date", "physical_contract", "reference_trade_ts", "reference_source_sha256"],
        how="semi").sort(["date", "physical_contract", "decision_ts"]))
    pins = {"checkpoint_manifest": _pin(manifest_path), "coverage": _pin(checkpoint / "coverage.parquet"),
            "trades": _pin(checkpoint / "trades.parquet"), "final_coverage": _pin(args.final_coverage),
            "references": reference_pins, "exchange_notice": _pin(args.notice),
            "implementation": _pin(Path(__file__))}
    if extended:
        pins.update(daily_classification=_pin(args.daily_classification), equity_notice=_pin(args.equity_notice))
    if weekend:
        pins["weekend_notice"] = _pin(args.weekend_notice)
    if zero_night:
        pins["zero_afterhours_inputs"] = zero_pins
    if early_holidays is not None:
        pins["early_holiday_originals"] = holiday_pins
        pins["early_holiday_implementation"] = holiday_implementation
    # Recheck large source inputs after the candidate computation as well.
    for pin in [pins["checkpoint_manifest"], pins["coverage"], pins["trades"], pins["final_coverage"],
                pins["exchange_notice"], *([pins["daily_classification"], pins["equity_notice"]] if extended else []),
                *([pins["weekend_notice"]] if weekend else []),
                *[v for r in reference_pins for k,v in r.items() if k != "pair_identity"], *zero_pins,
                *holiday_pins, *([holiday_implementation] if holiday_implementation else [])]:
        if sha256_file(Path(pin["path"])) != pin["sha256"]:
            raise ValueError("an input changed while generating the candidate")
    if selected.estimated_size() > 512 * 1024**2:
        raise ValueError("candidate exceeds the bounded output estimate; split dates")
    publication_capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / "candidates.parquet"
    atomic_write_parquet(payload, selected)
    if not selected.equals(pl.read_parquet(payload)):
        raise ValueError("candidate parquet failed readback")
    result = {"contract": ("tw_futures_early_holiday_prenight_candidate_v2" if early_holidays is not None else
                           "tw_futures_zero_afterhours_weekend_candidate_v1" if zero_night and weekend else
                           "tw_futures_zero_afterhours_adjacent_candidate_v1" if zero_night else
                           "tw_futures_equity_weekend_candidate_v1" if weekend else
                           "tw_futures_equity_day_only_adjacent_candidate_v1" if extended
                           else "tw_futures_prenight_adjacent_day_candidate_v1"),
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "sources": pins,
        "notice_url": NOTICE_URL, "no_night_before": NIGHT_START.isoformat(),
        "source_fingerprint": hashlib.sha256(json.dumps(pins, sort_keys=True).encode()).hexdigest(),
        "source_dates": sorted([info for _, info in results], key=lambda info: info["date"]),
        "preliminary_rows": preliminary.height, "candidate_rows": selected.height,
        "counts_by_clock": {k.strftime("%H:%M"): n for k,n in
                            selected.group_by(pl.col("decision_ts").dt.time()).len().iter_rows()},
        "data_path": str(payload), "data_sha256": sha256_file(payload), "training_ready": False,
        "initial_capacity": initial_capacity, "publication_capacity": publication_capacity,
        "limitations": ["The retained vendor source has no historical receive-time proof.",
            "Same-day full-book validation is retrospective; selection is research only, not a causal eligibility certificate.",
            "Only adjacent calendar days and zero-block-volume verified sources are considered.",
            "Candidates do not certify the execution price, order capacity, margin, holiday accounting or a broker fill."]}
    if extended:
        result["equity_notice_url"] = EQUITY_NOTICE_URL
        result["no_equity_night_before"] = {key: value.isoformat() for key, value in EQUITY_NIGHT_START.items()}
        result["counts_by_asset_class"] = dict(selected.group_by("asset_class").len().iter_rows())
        result["limitations"].append("Equity classification is bound to the financial research release, not a PIT certificate.")
    if weekend:
        result["weekend_notice_url"] = WEEKEND_NOTICE_URL
        result["weekend_policy"] = "Friday to Monday only, from 2019-01-07; no Saturday makeup trading"
        result["limitations"][2] = "Only Friday to Monday from 2019 and zero-block-volume verified sources are considered."
    if zero_night:
        result["limitations"].append("The final reported zero after-hours volume is retrospective, not a decision-time feed receipt.")
    if early_holidays is not None:
        result["limitations"][2] = "Only seven reviewed pre-night holiday reopenings and zero-block-volume verified sources are considered."
        result["limitations"].append("The market closure notices do not establish broker account valuations or corporate continuation.")
    atomic_write_json(output / "manifest.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("workspace", "checkpoint", "final-coverage", "early", "late", "notice", "output"):
        parser.add_argument("--" + option, type=Path, required=True)
    parser.add_argument("--daily-classification", type=Path)
    parser.add_argument("--equity-notice", type=Path)
    parser.add_argument("--weekend-notice", type=Path)
    parser.add_argument("--zero-afterhours-manifest", type=Path)
    parser.add_argument("--early-holiday-rules", type=Path,
                        help="Retained reviewed 2011-2017 calendar originals; separate research scope")
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--limit-days", type=int, default=0)
    args = parser.parse_args()
    if args.limit_days < 0:
        parser.error("--limit-days cannot be negative")
    result = build(args)
    print(json.dumps({k: result[k] for k in ("preliminary_rows", "candidate_rows", "counts_by_clock", "training_ready")}), flush=True)


if __name__ == "__main__":
    main()
