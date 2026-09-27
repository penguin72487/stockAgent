#!/usr/bin/env python3
"""Read-only TAIFEX source inventory: receipts are not historical completeness.

API discovery is from the saved official Swagger, never a second API registry.
Only small JSON evidence and Parquet footers are read; no provider requests,
data-row scans, queue changes, or dataset writes are performed. Reports are the
only output. Current snapshots must not be projected into historical rule dates.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text


MAX_JSON_BYTES = 20 * 1024 * 1024
API_BASE = "https://openapi.taifex.com.tw/v1"
# Web-only discovery anchors requested by the user. These are not API aliases.
WEB_SOURCES = (
    ("his_news", "歷史公告／保證金、限額、價格規則修訂", "11/hisNews", "historical_announcements"),
    ("contract_adjustments", "股票／ETF 契約調整公告", "4/contractAdj", "contract_adjustment"),
    ("position_limits_non_equity", "非個股類交易人部位限額", "4/traderPLNonEquity", "position_limit"),
    ("position_limits_equity", "個股類交易人部位限額", "4/traderPLEquity", "position_limit"),
    ("contract_specs", "股票期貨契約規格與漲跌幅", "2/sTF", "contract_specification_price_limit"),
)


def _json(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, UnicodeError):
        return {}


def _child(root: Path, value: Any) -> Path | None:
    """Receipt-owned paths must stay inside their declared dataset root."""
    if not isinstance(value, str) or not value:
        return None
    path = root / value
    try:
        path.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return None
    return path


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _slug(endpoint: str) -> str:
    # Same deterministic filename contract as the canonical OpenAPI collector.
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", endpoint.strip("/"))
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def _date(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()[:10]
    if isinstance(value, str):
        text = value.strip().replace("/", "-")
        if re.fullmatch(r"\d{8}", text):
            text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
        try:
            return date.fromisoformat(text[:10]).isoformat()
        except ValueError:
            pass
    return None


def _utc_timestamp(value: Any) -> str | None:
    """Preserve a timezone-backed observation clock; never invent midnight."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _rule_archive_quality(manifest: dict[str, Any]) -> dict[str, Any]:
    """Optional manifest summaries only; absence and invalid counts stay unknown."""
    raw = manifest.get("quality")
    if not isinstance(raw, dict):
        return {}
    quality = {}
    for key in ("parsing_status_counts", "parser_version_counts"):
        counts = raw.get(key)
        if isinstance(counts, dict):
            quality[key] = {str(name): _integer(value) for name, value in counts.items()}
    return quality


def _footer(path: Path) -> dict[str, Any]:
    """Read metadata only. Missing date statistics stay unknown, not mtime."""
    result: dict[str, Any] = {"owner_path_exists": path.is_file(), "rows": None,
                              "first_date": None, "last_date": None}
    if not result["owner_path_exists"] or path.suffix != ".parquet":
        return result
    try:
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(path)
        result["rows"] = metadata.num_rows
        result["count_basis"] = "parquet_footer_not_full_content_audit"
        names = {metadata.schema.column(i).name for i in range(metadata.num_columns)}
        # tw-public's generic date can be the capture/vintage key. Only use
        # explicit source dates there, never report the capture as settlement.
        date_columns = {"Date", "日期", "交易日期", "資料日期", "settlement_date",
                        "最後結算日", "TheFinalSettlementDay"}
        if "_as_of_date" not in names:
            date_columns.add("date")
        source_ranges = []
        used_columns = []
        for column in range(metadata.num_columns):
            name = metadata.schema.column(column).name
            if name not in date_columns:
                continue
            ranges = []
            for group in range(metadata.num_row_groups):
                stats = metadata.row_group(group).column(column).statistics
                if stats is not None and stats.num_values == 0:
                    continue
                if stats is None or not stats.has_min_max:
                    break
                first, last = _date(stats.min), _date(stats.max)
                if first is None or last is None:
                    break
                ranges.append((first, last))
            else:
                if ranges:
                    source_ranges.extend(ranges)
                    used_columns.append(name)
        if source_ranges:
            result.update(first_date=min(a for a, _ in source_ranges),
                          last_date=max(b for _, b in source_ranges),
                          date_basis="footer_explicit_source_dates:" + ",".join(used_columns))
    except (OSError, ValueError, TypeError, ImportError):
        result["metadata_error"] = "unreadable_parquet_footer"
    return result


def _rule_kind(text: str) -> str | None:
    lowered = text.casefold()
    for needles, kind in (
        (("margin", "保證金"), "margin"),
        (("positionlimit", "position_limit", "限額"), "position_limit"),
        (("contractadj", "adjusted", "調整"), "contract_adjustment"),
        (("openingprice", "opening_price", "參考價"), "reference_price"),
        (("fee", "費率"), "fee_schedule"),
    ):
        if any(needle in lowered for needle in needles):
            return kind
    return None


def _base(dataset: str, name: str, endpoint: str, kind: str) -> dict[str, Any]:
    return {"dataset": dataset, "name": name, "endpoint": endpoint, "source_kind": kind,
            "official_category": None, "rule_kind": _rule_kind(dataset + name),
            "status": "missing", "receipt_status": None, "rows": None,
            "first_date": None, "last_date": None, "first_capture_date": None,
            "last_capture_date": None, "observed_at_utc": None, "owner": None,
            "owner_path": None, "owner_path_exists": None, "history_mode": "unknown",
            "history_complete": None, "gaps": [], "evidence_paths": []}


def _api_rows(root: Path, public: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    latest = _json(public / "openapi_latest.json")
    swagger_path = _child(public, latest.get("swagger_path"))
    swagger = _json(swagger_path) if swagger_path is not None else {}
    digest = latest.get("swagger_sha256")
    valid_swagger = bool(swagger.get("paths")) and bool(swagger_path)
    if valid_swagger and digest:
        valid_swagger = hashlib.sha256(swagger_path.read_bytes()).hexdigest() == digest
    paths = swagger.get("paths", {}) if valid_swagger else {}
    datasets = {item.get("endpoint"): item for item in latest.get("datasets", [])
                if isinstance(item, dict) and isinstance(item.get("endpoint"), str)}
    delegated = latest.get("delegated_endpoints", {})
    if not isinstance(delegated, dict):
        delegated = {}
    # Never omit a recorded endpoint when a damaged/missing Swagger is detected.
    endpoints = sorted(set(datasets) | set(delegated) | {
        key for key, value in paths.items() if isinstance(value, dict) and "get" in value})
    failed = {item.get("endpoint") for item in latest.get("failed_endpoints", []) if isinstance(item, dict)}
    capture = _date(latest.get("capture_date"))
    captures: dict[str, list[str]] = {}
    for manifest_path in sorted((public / "manifests/openapi").glob("*.json")):
        manifest = _json(manifest_path)
        capture_date = _date(manifest.get("capture_date"))
        if capture_date:
            for item in manifest.get("datasets", []):
                if isinstance(item, dict) and item.get("status") in {"complete", "source_empty"}:
                    captures.setdefault(str(item.get("endpoint")), []).append(capture_date)
    rows = []
    for endpoint in endpoints:
        operation = paths.get(endpoint, {}).get("get", {})
        row = _base(endpoint.strip("/"), str(operation.get("summary") or endpoint), endpoint, "openapi")
        row.update(source_url=API_BASE + endpoint, official_category=operation.get("tags", []),
                   history_mode="rolling_or_current_snapshot", history_complete=None,
                   owner="taifex_openapi_catalog", observed_at_utc=latest.get("captured_at_utc"))
        row["evidence_paths"] = [str(public / "openapi_latest.json")]
        recorded = datasets.get(endpoint, {})
        row["receipt_status"] = recorded.get("status")
        if endpoint in delegated:
            owner = str(delegated[endpoint])
            row.update(owner=owner, history_mode="canonical_owner_scope_requires_audit")
            if owner.startswith("data_") and re.fullmatch(r"[\w/.-]+\.parquet", owner):
                # Canonical data_* may be authorized live-workspace symlinks.
                # Reject lexical traversal, not the catalog's external target.
                owner_path = root / owner if ".." not in Path(owner).parts else None
                if owner_path is not None:
                    row.update(owner_path=str(owner_path), **_footer(owner_path))
                row["status"] = "delegated_present_unverified" if row["owner_path_exists"] else "delegated_missing"
                row["gaps"] = ["delegation_is_not_scope_or_history_completeness_proof"]
            else:
                tape_manifest = root / "data_tw_index_derivatives_ticks/manifest.json"
                tape = _json(tape_manifest)
                row.update(status="delegated_scope_unverified", owner_path=str(tape_manifest),
                           owner_path_exists=tape_manifest.is_file(),
                           first_date=_date(tape.get("date_start")), last_date=_date(tape.get("date_end")))
                row["evidence_paths"].append(str(tape_manifest))
                row["gaps"] = ["owner_is_descriptive_not_an_endpoint_verified_path",
                               "raw_all_product_archives_do_not_prove_all_product_normalization",
                               "normalized_tick_scope_TX_TXO_only_spread_endpoint_parity_unverified"]
        else:
            receipt_path = public / "receipts/openapi" / (capture or "missing") / f"{_slug(endpoint)}.json"
            receipt = _json(receipt_path)
            raw = _child(public, receipt.get("raw_path"))
            normalized = _child(public, receipt.get("normalized_path"))
            raw_ok = raw is not None and raw.is_file() and raw.stat().st_size == receipt.get("raw_bytes")
            normalized_ok = (receipt.get("status") == "source_empty" or
                             (normalized is not None and normalized.is_file() and
                              normalized.stat().st_size == receipt.get("normalized_bytes")))
            identity_ok = receipt.get("endpoint") == endpoint and receipt.get("capture_date") == capture
            if identity_ok and raw_ok and normalized_ok and receipt.get("status") in {"complete", "source_empty"}:
                row.update(status="snapshot_preserved" if receipt["status"] == "complete" else "source_empty",
                           rows=_integer(receipt.get("rows")), owner_path=str(normalized or raw),
                           owner_path_exists=True, observed_at_utc=receipt.get("captured_at_utc"),
                           count_basis="receipt_and_artifact_size_not_full_content_audit")
            else:
                row["status"] = "failed" if endpoint in failed else "missing_or_invalid_receipt"
                row["gaps"].append("receipt_identity_or_local_artifact_missing_or_size_mismatch")
            dates = captures.get(endpoint, []) + ([capture] if row["status"] in {"snapshot_preserved", "source_empty"} and capture else [])
            row.update(first_capture_date=min(dates) if dates else None,
                       last_capture_date=max(dates) if dates else None,
                       capture_date_basis="archive_manifest_not_all_historical_receipts_reverified")
            row["gaps"].extend(["source_event_date_range_not_measured_from_snapshot",
                                "capture_history_is_not_historical_rule_effective_date_coverage"])
            row["evidence_paths"].append(str(receipt_path))
        rows.append(row)
    return rows, {"swagger_available_and_digest_valid": bool(valid_swagger),
                  "swagger_path": str(swagger_path) if swagger_path else None,
                  "catalog_capture_date": capture, "declared_api_endpoints": latest.get("catalog_endpoints"),
                  "catalog_endpoint_count": len(endpoints), "network_requests": 0, "data_row_scans": 0,
                  "full_checksum_verification": False}


def _history_rows(root: Path, public: Path) -> list[dict[str, Any]]:
    manifest = _json(public / "manifest.json")
    histories = [(item, public / "manifest.json", public) for item in manifest.get("datasets", [])
                 if isinstance(item, dict)]
    vix = _json(public / "vix_latest.json")
    if vix:
        histories.append((vix, public / "vix_latest.json", public))
    result = []
    for item, evidence, owner_root in histories:
        name = str(item.get("dataset") or "unknown_history")
        row = _base(name, name, str(item.get("source_page") or name), "web_history")
        output = _child(owner_root, item.get("output_path"))
        exists = output is not None and output.is_file()
        row.update(status="history_preserved_scope_limited" if exists else "missing_output",
                   receipt_status=item.get("status"), rows=_integer(item.get("rows")),
                   first_date=_date(item.get("first_date")), last_date=_date(item.get("last_date")),
                   owner="taifex_public_history", owner_path=str(output) if output else None,
                   owner_path_exists=exists, history_mode="retained_history_with_source_window",
                   observed_at_utc=item.get("completed_at_utc", manifest.get("completed_at_utc")),
                   count_basis="manifest_not_recomputed", evidence_paths=[str(evidence)])
        if name in {"large_trader_futures_all", "large_trader_options_all"}:
            # The all-product quarterly CSV is not the legacy rolling query
            # UI. Keep its source boundary, acquisition gaps and PIT clocks
            # separate, including when an older manifest lacks new metadata.
            counts = {key: _integer(item.get(key)) for key in (
                "expected_session_count", "observed_session_count", "missing_session_count",
                "deferred_session_count", "availability_pending_rows", "availability_aligned_rows",
                "first_publication_floor_rows", "offline_reparse_requests",
            )}
            source_range = {
                "requested_start_date": _date(item.get("requested_start_date", manifest.get("large_trader_requested_start_date"))),
                "effective_source_start_date": _date(item.get("effective_source_start_date", manifest.get("large_trader_effective_source_start_date"))),
                "source_history_start_date": _date(item.get("source_history_start_date")),
                "first_publication_date": _date(item.get("first_publication_date")),
                "first_publication_timing": item.get("first_publication_timing"),
                "request_window": item.get("request_window"),
                "history_boundary": item.get("history_boundary"),
            }
            unsupported = item.get("source_not_supported")
            if isinstance(unsupported, dict):
                source_range["source_not_supported"] = {
                    "start_date": _date(unsupported.get("start_date")),
                    "end_date": _date(unsupported.get("end_date")),
                    "reason": unsupported.get("reason"),
                    "verified_session_count": _integer(unsupported.get("verified_session_count")),
                }
            row.update(history_mode="all_product_quarterly_csv_retained_history",
                       coverage_status=item.get("coverage_status"), source_range=source_range,
                       acquisition_counts=counts, availability_state=item.get("availability_state"),
                       product_universe_completeness=item.get("product_universe_completeness"))
            if exists and (item.get("status") == "partial" or (counts["missing_session_count"] or 0) > 0
                           or item.get("coverage_status") == "unverified_gaps"):
                row["status"] = "history_partial"
            row["gaps"].append("all_product_csv_session_coverage_not_product_or_pit_completeness")
            notes = ["全市場 CSV：單次至多三個曆月；不是只保留近三年歷史。"]
            start = source_range["effective_source_start_date"]
            notes.append(f"本批來源有效起日 {start}。" if start else "本批來源有效起日尚無收據。")
            observed, expected, missing = (counts[key] for key in (
                "observed_session_count", "expected_session_count", "missing_session_count"))
            notes.append(f"交易日覆蓋 {_cell(observed)}/{_cell(expected)}；缺 {_cell(missing)} 日"
                         f"（狀態：{_cell(item.get('coverage_status'))}）。")
            first_publication = source_range["first_publication_date"]
            notes.append(f"首度發布日 {first_publication}；盤後時點為規律推測，不是當年逐分證據。"
                         if first_publication else "首度發布日尚未出現在本批收據；不可由最早資料日倒推。")
            if counts["availability_pending_rows"] is not None:
                notes.append(f"待下一個已驗證交易日對齊 {counts['availability_pending_rows']} 列；不是下載失敗。")
            if source_range.get("source_not_supported"):
                notes.append("官方歷史起日前不支援區間另列，不算下載缺口。")
            row["notes"] = notes
        elif name.startswith(("institutional", "large_trader")):
            row["gaps"].append("official_web_query_rolling_three_year_window_not_all_history")
        if name == "large_trader_futures_tx":
            row["gaps"].append("TX_only_other_futures_and_options_history_not_covered")
        if "vix" in name:
            row["gaps"].append("rolling_recent_page_not_all_historical_vix")
        result.append(row)
    return result


def _canonical_rows(root: Path) -> list[dict[str, Any]]:
    """Read existing canonical manifests, without copying their API ownership."""
    specs = (
        ("all_futures_daily", "全商品期貨日資料", "data_tw_index_futures/manifest.json", "all_futures_daily", "path"),
        ("monthly_options_chain", "TXO 近月全履約價日資料", "data_tw_index_options_daily/manifest.json", "full_chain_quality", "full_chain_path"),
        ("weekly_options_chain", "TXO 最近週到期全履約價日資料", "data_tw_index_options_daily/manifest_weekly.json", "full_chain_quality", "full_chain_path"),
        ("futures_final_settlement_history", "期貨歷史最後結算", "data_tw_futures/final_settlement_v1/manifest.json", "", "normalized"),
        ("txo_final_settlement_history", "TXO 歷史最後結算", "data_tw_index_options_daily/manifest_final_settlement.json", "", "normalized"),
    )
    result = []
    for key, label, relative, section, path_key in specs:
        evidence = root / relative
        manifest = _json(evidence)
        details = manifest.get(section, {}) if section else manifest
        details = details if isinstance(details, dict) else {}
        quality = details.get("quality", details)
        raw_path = details.get(path_key, manifest.get(path_key))
        if raw_path is None and isinstance(manifest.get("outputs"), dict):
            raw_path = manifest["outputs"].get(key)
        if isinstance(raw_path, dict):
            raw_path = raw_path.get("path")
        if isinstance(raw_path, str):
            output = Path(raw_path) if Path(raw_path).is_absolute() else root / raw_path
        else:
            output = None
        row = _base(key, label, str(manifest.get("official_download_endpoint") or manifest.get("official_page") or key), "canonical_history")
        row.update(status="history_preserved_scope_limited" if output and output.is_file() else "missing_output",
                   receipt_status=manifest.get("status"), rows=_integer(quality.get("rows")),
                   first_date=_date(quality.get("first_date", quality.get("first_settlement_date"))),
                   last_date=_date(quality.get("last_date", quality.get("last_settlement_date"))),
                   observed_at_utc=manifest.get("generated_at_utc", manifest.get("generated_at")),
                   owner=str(evidence.parent), owner_path=str(output) if output else None,
                   owner_path_exists=output.is_file() if output else False, history_mode="canonical_retained_history",
                   count_basis="manifest_not_recomputed", evidence_paths=[str(evidence)])
        if "options_chain" in key:
            row["gaps"] = ["TXO_only_nearest_expiry_not_every_option_product_or_expiry"]
        if key == "txo_final_settlement_history":
            row["gaps"] = ["TXO_only_since_manifest_start_not_all_option_products"]
        if key == "futures_final_settlement_history":
            row["gaps"] = ["index_stock_etf_scope_since_2014_other_categories_or_earlier_not_proven"]
        result.append(row)
    return result


def _web_rows(public: Path) -> list[dict[str, Any]]:
    manifest_path = public / "rules/manifest.json"
    manifest = _json(manifest_path)
    sources = manifest.get("sources", [])
    if isinstance(sources, dict):
        sources = [dict(value, source_id=key) for key, value in sources.items() if isinstance(value, dict)]
    definitions = [(key, label, "https://www.taifex.com.tw/cht/" + suffix, kind)
                   for key, label, suffix, kind in WEB_SOURCES]
    known = {item[0] for item in definitions}
    for source in sources:
        if isinstance(source, dict) and source.get("source_id") not in known and source.get("source_id"):
            key = str(source["source_id"])
            definitions.append((key, str(source.get("name") or key), str(source.get("url") or ""),
                                str(source.get("kind") or _rule_kind(key) or "unknown")))
            known.add(key)
    result = []
    for key, label, url, rule_kind in definitions:
        evidence = next((value for value in sources if isinstance(value, dict) and
                         (value.get("source_id") == key or value.get("url") == url or value.get("source_url") == url)), {})
        row = _base(key, label, url, "web_rules")
        observed = _utc_timestamp(evidence.get("observed_at_utc"))
        captured = observed if evidence.get("status") in {"complete", "current", "indexed", "raw_preserved"} else None
        row.update(source_url=url, official_category="交易與結算規則／公告", rule_kind=rule_kind,
                   status=str(evidence.get("status") or "missing"), receipt_status=evidence.get("status"),
                   owner="taifex_public_rules", owner_path=str(manifest_path), owner_path_exists=manifest_path.is_file(),
                   rows=_integer(evidence.get("rows")), first_date=_date(evidence.get("first_date")),
                   last_date=_date(evidence.get("last_date")), observed_at_utc=observed or manifest.get("observed_at_utc"),
                   last_capture_date=_date(captured), last_capture_at_utc=captured,
                   capture_date_basis="source_specific_success_observed_at_utc_not_first_capture_or_data_date" if captured else None,
                   history_mode="historical_announcements" if key == "his_news" else "current_rules_plus_dated_notices",
                   evidence_paths=[str(manifest_path)])
        if key == "his_news":
            coverage = manifest.get("coverage", {})
            counts = manifest.get("counts", {})
            row.update(first_date=_date(evidence.get("first_date", coverage.get("first_published_date"))),
                       last_date=_date(evidence.get("last_date", coverage.get("last_published_date"))),
                       rows=_integer(evidence.get("rows", counts.get("announcements"))))
            row["archive_coverage"] = coverage
            row["archive_counts"] = counts
            quality = _rule_archive_quality(manifest)
            if quality:
                row["archive_quality"] = quality
        if not evidence:
            row["gaps"].append("no_source_specific_rule_receipt")
        row["gaps"].append("raw_notices_are_not_verified_product_effective_time_numeric_rules")
        result.append(row)
    return result


def build_inventory(root: Path, now: datetime | None = None) -> dict[str, Any]:
    root = root.resolve()
    public = root / "data_taifex_public_history"
    rows, evidence = _api_rows(root, public)
    rows += _history_rows(root, public) + _canonical_rows(root) + _web_rows(public)
    summary = {"inventory_rows": len(rows),
               "by_status": dict(Counter(row["status"] for row in rows)),
               "by_source_kind": dict(Counter(row["source_kind"] for row in rows))}
    archive_quality = next((row["archive_quality"] for row in rows if row.get("archive_quality")), None)
    if archive_quality is not None:
        summary["rule_archive_quality"] = archive_quality
    return {"schema_version": 1, "observed_at_utc": (now or datetime.now(timezone.utc)).isoformat(),
            "scope": "saved_official_swagger_existing_canonical_manifests_and_requested_rule_pages",
            "all_taifex_website_exhaustiveness_proven": False, "all_history_complete": False,
            "evidence": evidence, "summary": summary,
            "limitations": ["counts_across_sources_are_not_additive_due_to_overlapping_grains",
                "metadata_and_path_presence_not_full_content_hash_audit",
                "unknown_source_dates_are_null_never_replaced_with_capture_date",
                "snapshot_and_delegation_do_not_prove_historical_completeness"], "datasets": rows}


def _cell(value: Any) -> str:
    if value is None:
        return "未證實"
    if isinstance(value, (list, dict)):
        value = json.dumps(value, ensure_ascii=False)
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(inventory: dict[str, Any]) -> str:
    kinds = {"openapi": "官方 OpenAPI", "web_rules": "規則／公告來源",
             "web_history": "官方網頁歷史", "canonical_history": "既有標準化歷史"}
    statuses = {"snapshot_preserved": "快照已保存；歷史未證實", "source_empty": "來源空值有收據",
                "delegated_present_unverified": "委派檔存在；範圍待核對", "delegated_missing": "委派檔缺失",
                "delegated_scope_unverified": "逐筆原始庫存在與完整範圍需另查",
                "history_preserved_scope_limited": "已保存限定範圍歷史", "history_partial": "歷史部分保存；仍有覆蓋缺口", "missing": "尚無來源收據",
                "missing_output": "宣告的歷史產物不存在", "failed": "來源請求失敗",
                "missing_or_invalid_receipt": "缺收據／檔案／身分不符"}
    gaps = {"source_event_date_range_not_measured_from_snapshot": "快照內資料日期尚未量測",
            "capture_history_is_not_historical_rule_effective_date_coverage": "抓取紀錄不等於歷史生效規則",
            "delegation_is_not_scope_or_history_completeness_proof": "委派不代表商品／歷史範圍完整",
            "owner_is_descriptive_not_an_endpoint_verified_path": "原委派是文字描述，尚無精確端點對照",
            "raw_all_product_archives_do_not_prove_all_product_normalization": "全商品原始 ZIP 不等於全商品正規化",
            "normalized_tick_scope_TX_TXO_only_spread_endpoint_parity_unverified": "正規化僅 TX/TXO；價差端點對照未核對",
            "official_web_query_rolling_three_year_window_not_all_history": "公開頁面僅滾動三年查詢窗",
            "all_product_csv_session_coverage_not_product_or_pit_completeness": "交易日覆蓋不代表商品全生命週期或 PIT 完整",
            "TX_only_other_futures_and_options_history_not_covered": "僅 TX；其他商品／選擇權大額交易人歷史未涵蓋",
            "rolling_recent_page_not_all_historical_vix": "VIX 僅現行近期下載頁可得期間",
            "TXO_only_nearest_expiry_not_every_option_product_or_expiry": "僅 TXO 最近到期，不含全部商品／遠月",
            "TXO_only_since_manifest_start_not_all_option_products": "僅 TXO，非全商品或完整上市期間",
            "index_stock_etf_scope_since_2014_other_categories_or_earlier_not_proven": "2014 年起指數／股票／ETF；更早及其他類別未證實",
            "no_source_specific_rule_receipt": "尚無該來源專屬收據",
            "raw_notices_are_not_verified_product_effective_time_numeric_rules": "公告原文仍須核對商品、金額、發布及生效時鐘",
            "receipt_identity_or_local_artifact_missing_or_size_mismatch": "收據身分、檔案存在或大小檢查未通過"}
    lines = ["# TAIFEX 公開資料清冊", "", f"盤點時間：{inventory['observed_at_utc']}", "",
             "資料粒度：每個官方 API／既有歷史產物／指定公告來源一列。分類及名稱取自保存的官方 Swagger。",
             "僅讀本機 JSON／Parquet footer；未下載、未掃描資料列、未驗證全部檔案雜湊。",
             "快照已保存、委派檔存在、歷史已保存和完整歷史已證實是不同狀態；此報告不宣稱已抓齊。",
             "最早／最新是資料日期；抓取日期另外列出。不同來源可能重疊，筆數不可直接加總。", "",
             "| 類型 | 列數 |", "|---|---:|"]
    lines += [f"| {_cell(kinds.get(key, key))} | {value} |" for key, value in inventory["summary"]["by_source_kind"].items()]
    if quality := inventory["summary"].get("rule_archive_quality"):
        lines += ["", "公告解析品質（manifest 計數；非下載失敗數、不可與公告／文件列數相加）：",
                  _cell(quality)]
    for kind in ("web_rules", "canonical_history", "web_history", "openapi"):
        lines += ["", f"## {kinds[kind]}", "", "| 名稱／端點 | 狀態 | 筆數 | 最早／最新資料 | 最早／最新抓取 | 缺口 |", "|---|---|---:|---|---|---|"]
        for row in inventory["datasets"]:
            if row["source_kind"] == kind:
                lines.append("| " + " | ".join([_cell(row["name"]) + " · " + _cell(row["endpoint"]),
                    _cell(statuses.get(row["status"], row["status"])), _cell(row["rows"]),
                    _cell(row["first_date"]) + " → " + _cell(row["last_date"]),
                    _cell(row["first_capture_date"]) + " → " + _cell(row["last_capture_date"]),
                    _cell("；".join([*(gaps.get(gap, gap) for gap in row["gaps"]), *row.get("notes", [])]))]) + " |")
    lines += ["", "JSON／CSV 同時保留 owner、實際路徑是否存在、證據路徑、官方分類及規則種類；null 代表尚未證實。", ""]
    return "\n".join(lines)


def write_reports(inventory: dict[str, Any], output_dir: Path) -> dict[str, str]:
    paths = {kind: output_dir / f"taifex_public_inventory.{kind}" for kind in ("json", "csv", "md")}
    atomic_write_json(paths["json"], inventory)
    fields = sorted({key for row in inventory["datasets"] for key in row})
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in inventory["datasets"]:
        writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                         for key, value in row.items()})
    atomic_write_text(paths["csv"], stream.getvalue())
    atomic_write_text(paths["md"], render_markdown(inventory))
    return {key: str(path) for key, path in paths.items()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/data_quality/taifex_public_inventory"))
    args = parser.parse_args(argv)
    inventory = build_inventory(args.root)
    paths = write_reports(inventory, args.output_dir)
    print(json.dumps({"summary": inventory["summary"], "reports": paths}, ensure_ascii=False))
    return 0 if inventory["evidence"]["swagger_available_and_digest_valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
