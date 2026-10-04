#!/usr/bin/env python3
"""Export every registered monitor source without contacting any provider.

The monitor snapshot is the existing source registry projection. This export
does not claim a source is complete merely because a job or file exists.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import io
import json
from pathlib import Path
import sys
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.acquisition_policy import (
    ACQUISITION_OWNERS, FACT_IDENTITY, evaluate_secondary_admission,
    source_admission_class, source_blocker,
)
from downloader.artifact_io import atomic_write_json, atomic_write_text

DEFAULT_SNAPSHOT = ROOT / "artifacts/live/data_monitor/public_status.json"
FIELDS = (
    "id", "endpoint_id", "registry_alias", "provider", "title", "market_category", "scope",
    "in_active_scope", "acquisition_enabled", "automation_eligible", "publishable",
    "acquisition_role", "work_class", "acquisition_blocker", "quota_scope",
    "dedup_identity", "dedup_evidence", "acquisition_owner_endpoints",
    "dispatch_order", "validation_policy", "validation_admission",
    "update_owner", "cadence", "operation_state", "status", "status_label",
    "coverage_current", "coverage_total", "coverage_unit", "record_count",
    "first_observed", "last_observed", "record_evidence", "source_link",
    "snapshot_at_utc",
    "record_count_basis", "schedule_state", "job_running", "service_keys",
    "next_run_at_utc", "next_run_basis", "expected_release_at_utc",
    "expected_release_basis", "publication_clock_kind", "last_checked_at_utc",
    "receipt_reported_rows", "receipt_first_data_date", "receipt_last_data_date",
    "receipt_observed_at_utc", "receipt_path", "receipt_evidence",
    "configured_request_start", "configured_request_grain", "history_start_basis",
)


def quota_scope(source: Mapping[str, Any]) -> str:
    """Routing labels, not invented limits or an alternative limiter registry."""
    provider = str(source.get("provider") or "")
    name = str(source.get("id") or "")
    if provider.startswith("FinLab") or name.startswith("finlab:"):
        return "finlab_account_daily_bytes; observed_provider_reset"
    if provider.startswith("FinMind") or name.startswith("finmind:"):
        return "finmind-v4-data; shared_token_hourly_and_provider_cooldown"
    if "Shioaji" in provider:
        return "shioaji_person_connections_quote_requests_daily_bytes"
    if provider.startswith("OpenBB") or name.startswith("openbb:"):
        return "underlying_provider_bucket; OpenBB_is_not_independent_quota"
    if provider == "Binance":
        return "binance_IP_request_weight_and_endpoint_bucket"
    if provider == "OKX":
        return "okx_endpoint_IP_or_IP_instrument_bucket"
    if provider == "Bybit":
        return "bybit_IP_plus_endpoint_response_limits"
    return "existing_provider_named_limiter; numeric_limit_not_inferred"


def validation_admission(source: Mapping[str, Any]) -> str:
    name = str(source.get("id") or "")
    if name.startswith("finmind:sponsor:"):
        return "enforced_in_Sponsor_next_for_priority8_API_only"
    if name.startswith("finlab:"):
        return "enforced_in_FinLab_sync_for_secondary_price_refresh_only"
    return "not_wired_as_optional_validation; independent_required_acquisition"


def acquisition_role(source: Mapping[str, Any]) -> str:
    """Only classify source authority where this repo has an explicit rule."""
    name = str(source.get("id") or "")
    provider = str(source.get("provider") or "")
    if name == "finmind:sponsor:TaiwanStockInstitutionalInvestorsBuySellWide":
        return "derived_from_finmind_long_no_api"
    if name == "finmind:sponsor:TaiwanStockPrice":
        return "early_history_and_gap_fill_then_validation"
    if name == "finlab:price:收盤價":
        return "first_acquisition_then_surplus_validation"
    if provider.startswith(("TWSE", "TPEx", "MOPS", "TAIFEX", "TDCC")):
        return "official_primary_for_own_fields"
    if provider == "永豐 Shioaji":
        return "broker_primary_for_own_grain"
    if provider == "FinLab":
        return "independent_research_source_unreconciled"
    if provider == "FinMind":
        return "independent_source_or_gap_fill_unreconciled"
    return "independent_source_unreconciled"


def rows_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    sources = snapshot.get("sources")
    if not isinstance(sources, list):
        raise ValueError("monitor snapshot has no source registry")
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping) or not isinstance(source.get("id"), str):
            raise ValueError("invalid source registry item")
        source_id = source["id"]
        if source_id in seen:
            raise ValueError(f"duplicate source id: {source_id}")
        seen.add(source_id)
        coverage = source.get("coverage") if isinstance(source.get("coverage"), Mapping) else {}
        records = source.get("record_stats") if isinstance(source.get("record_stats"), Mapping) else {}
        automation = source.get("automation") if isinstance(source.get("automation"), Mapping) else {}
        publication = source.get("publication") if isinstance(source.get("publication"), Mapping) else {}
        rows.append({
            "id": source_id, "endpoint_id": source.get("endpoint_id"),
            "registry_alias": source.get("registry_alias"),
            "provider": source.get("provider"), "title": source.get("title"),
            "market_category": source.get("market_category"), "scope": source.get("scope"),
            "in_active_scope": source.get("in_active_scope"),
            "acquisition_enabled": source.get("acquisition_enabled"),
            "automation_eligible": source.get("automation_eligible"),
            "publishable": source.get("publishable"),
            "acquisition_role": acquisition_role(source), "update_owner": source.get("update_owner"),
            "work_class": source_admission_class(source),
            "acquisition_blocker": source_blocker(source),
            "quota_scope": quota_scope(source), "dedup_identity": FACT_IDENTITY,
            "dedup_evidence": "explicit_ownership_only; same_name_is_not_equivalence_proof",
            "acquisition_owner_endpoints": "|".join(ACQUISITION_OWNERS.get(source_id, ())),
            "dispatch_order": "live_and_due_incremental_reserved > missing_history > optional_tick > API_validation",
            "validation_policy": "all_required_acquisition_gate + local_queue + shared_surplus; local_file_audit_no_API",
            "validation_admission": validation_admission(source),
            "cadence": source.get("cadence"), "operation_state": source.get("operation_state"),
            "status": source.get("status"), "status_label": source.get("status_label"),
            "coverage_current": coverage.get("current"), "coverage_total": coverage.get("total"),
            "coverage_unit": coverage.get("unit"), "record_count": records.get("count"),
            "first_observed": records.get("first"), "last_observed": records.get("last"),
            "record_evidence": records.get("state"), "source_link": source.get("detail_link"),
            "snapshot_at_utc": snapshot.get("generated_at_utc"),
            "record_count_basis": records.get("basis"),
            "schedule_state": automation.get("schedule_state"),
            "job_running": automation.get("job_running"),
            "service_keys": "|".join(automation.get("service_keys") or ()),
            "next_run_at_utc": automation.get("next_run_at_utc"),
            "next_run_basis": automation.get("next_run_basis"),
            "expected_release_at_utc": publication.get("expected_release_at_utc"),
            "expected_release_basis": publication.get("expected_release_basis"),
            "publication_clock_kind": publication.get("schedule_kind"),
            "last_checked_at_utc": publication.get("last_checked_at_utc"),
        })
    return rows


def policy_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    providers: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        counts = providers[str(row.get("provider") or "unclassified")]
        counts["registered_rows"] += 1
        counts[str(row["work_class"])] += 1
        if row.get("acquisition_blocker"):
            counts["required_endpoints_not_ready"] += 1
    return {
        "snapshot_at_utc": rows[0].get("snapshot_at_utc") if rows else None,
        "registered_rows": len(rows),
        "providers": {key: dict(value) for key, value in sorted(providers.items())},
        "interpretation": "Registry rows are not task/symbol/field counts. Unreconciled sources are not deduplicated or declared complete.",
    }


def read_object(path: Path) -> dict[str, Any]:
    """Missing evidence is unknown; malformed existing evidence fails visibly."""
    if not path.is_file():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def enrich_finmind_receipts(rows: list[dict[str, Any]], root: Path) -> None:
    """Do not replace inventory statistics with overlapping task row totals.

    In particular, a checked/requested end partition is not a data date. Keep
    first/last_data_date from the worker receipt in separately labelled fields.
    """
    from downloader.finmind_scheduling import SPECS
    from downloader.download_finmind_complement import ALL_DATASETS, GLOBAL_START_YEAR

    statuses = {name: read_object(root / f"data_finmind/{name}/status.json")
                for name in ("sponsor", "complement")}
    for row in rows:
        parts = str(row["id"]).split(":", 2)
        if len(parts) == 2 and parts[0] == "finmind" and parts[1] in ALL_DATASETS:
            _, dataset = parts
            owner = "complement"
        elif len(parts) == 3 and parts[0] == "finmind" and parts[1] in statuses:
            _, owner, dataset = parts
        else:
            continue
        spec = SPECS.get(dataset) if owner == "sponsor" else None
        if spec is not None:
            row.update(configured_request_start=spec.first_date.isoformat() if spec.first_date else None,
                       configured_request_grain=spec.grain,
                       history_start_basis="configured_query_lower_bound_not_verified_source_earliest")
        if owner == "complement" and dataset in GLOBAL_START_YEAR:
            row.update(configured_request_start=f"{GLOBAL_START_YEAR[dataset]}-01-01",
                       configured_request_grain="provider_specific_historical_range",
                       history_start_basis="configured_query_lower_bound_not_verified_source_earliest")
        status = statuses[owner]
        receipt = (status.get("series") or {}).get(dataset)
        if not isinstance(receipt, Mapping):
            continue
        row.update(receipt_reported_rows=receipt.get("rows"),
                   receipt_first_data_date=receipt.get("first_data_date"),
                   receipt_last_data_date=receipt.get("last_data_date"),
                   receipt_observed_at_utc=status.get("observed_at_utc"),
                   receipt_path=f"data_finmind/{owner}/status.json",
                   receipt_evidence="worker_receipt_only; not_unique_row_or_current_file_hash_audit")


def credential_owner(source_id: str) -> str | None:
    """Explicit joins only: brand similarity must not imply quota ownership."""
    if source_id.startswith("credential:"):
        return None  # A credential row is never executable acquisition evidence.
    if source_id.startswith("keyed-public:"):
        return source_id.split(":", 2)[1]
    exact = {
        "crypto-fact:aggregate_market_cap_supply": "coingecko",
        "crypto-fact:aggregate_asset_identity": "coingecko",
        "crypto-fact:developer_activity": "github",
        "crypto-fact:token_unlocks_and_emissions": "dune",
        "crypto-fact:custom_onchain_queries": "dune",
        "crypto-fact:cex_reserves_and_flows": "dune",
        "crypto-fact:bridge_flows": "dune",
        "crypto-etf:sec-edgar": "sec_identity",
        "free-source:sec_edgar": "sec_identity",
        "free-source:fred_crypto_macro_initial_releases": "openbb_fred",
    }
    if source_id in exact:
        return exact[source_id]
    if source_id.startswith("dune-query:"):
        return "dune"
    if source_id.startswith("product:alpaca_us_equities:"):
        return "alpaca"
    prefix = source_id.split(":", 1)[0]
    if prefix in {"finlab", "finmind", "dune", "coingecko", "etherscan", "coinglass"}:
        return prefix
    if prefix == "shioaji":
        return "shioaji"
    if prefix == "openbb":
        provider = source_id.split(":", 2)[1]
        return {"cftc": "openbb_cftc_legacy", "congress_gov": "openbb_congress",
                "sec": "sec_identity", "un_comtrade": "un_comtrade"}.get(provider, f"openbb_{provider}")
    return None


API_FIELDS = ("catalog_id", "provider", "credential_state", "credential_names", "credential_source",
              "authentication_verified", "entitlement_verified", "registered_data_rows",
              "active_data_rows", "acquisition_source_ids", "keyed_catalog_adapter", "documentation",
              "credential_observed_at_utc", "catalog_scope", "catalog_rate_basis",
              "catalog_refresh_ttl_seconds", "upstream_cadence", "interpretation")


def api_rows(credentials: Mapping[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if credentials.get("secret_values_included") is not False:
        raise ValueError("credential receipt must explicitly exclude secret values")
    from downloader.download_keyed_public_catalogs import SPEC_BY_PROVIDER

    result = []
    for provider in credentials.get("providers", []):
        catalog_id = provider.get("catalog_id") or provider.get("id")
        linked = [row for row in rows if credential_owner(row["id"]) == catalog_id]
        spec = SPEC_BY_PROVIDER.get(catalog_id)
        names = sorted(set(provider.get("required_names", []) + provider.get("any_of_names", [])
                           + provider.get("optional_names", [])))
        result.append({
            "catalog_id": catalog_id, "provider": provider.get("provider"),
            "credential_state": provider.get("state"), "credential_names": "|".join(names),
            "credential_source": provider.get("source"),
            "authentication_verified": False, "entitlement_verified": False,
            "registered_data_rows": len(linked),
            "active_data_rows": sum(row.get("in_active_scope") is True for row in linked),
            "acquisition_source_ids": "|".join(row["id"] for row in linked),
            "keyed_catalog_adapter": spec.implementation if spec else "not_in_keyed_catalog_collector",
            "documentation": provider.get("registration_url"),
            "credential_observed_at_utc": credentials.get("generated_at_utc"),
            "catalog_scope": spec.scope if spec else None,
            "catalog_rate_basis": spec.rate_basis if spec else None,
            "catalog_refresh_ttl_seconds": spec.ttl_seconds if spec else None,
            "upstream_cadence": spec.upstream_cadence if spec else None,
            "interpretation": "presence_only; row_counts_include_aliases; zero_linked_rows_is_not_proof_no_collector_exists",
        })
    return result


def write_csv(path: Path, rows: list[dict[str, Any]], fields: tuple[str, ...] | None = None) -> None:
    stream = io.StringIO(newline="")
    if fields is None:
        fields = tuple(sorted({key for row in rows for key in row}))
    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                     for key, value in row.items()} for row in rows)
    atomic_write_text(path, stream.getvalue())


def write_bundle(directory: Path, rows: list[dict[str, Any]], *, credentials: Mapping[str, Any],
                 registry: Mapping[str, Any], taifex: Mapping[str, Any],
                 evidence_root: Path | None = None) -> dict[str, Any]:
    """Small receipt-only export: no provider calls and no recursive data scan."""
    providers = api_rows(credentials, rows)
    discovered, catalog_issues = [], []
    if evidence_root is not None:
        from scripts.public_catalog_inventory import catalog_inventory
        discovered, catalog_issues = catalog_inventory(evidence_root)
    summary = policy_summary(rows)
    summary.update(schema_version=2, api_credential_entries=len(providers),
                   credential_state_counts=dict(Counter(row["credential_state"] for row in providers)),
                   source_groups=len(registry.get("sources", [])),
                   taifex_catalog_entries=len(taifex.get("datasets", [])),
                   discovered_public_datasets=len(discovered),
                   discovered_catalog_issues=catalog_issues,
                   inventory_rows_with_record_dates=sum(bool(row.get("first_observed") and row.get("last_observed")) for row in rows),
                   rows_with_worker_receipt_dates=sum(bool(row.get("receipt_first_data_date") and row.get("receipt_last_data_date")) for row in rows),
                   global_bulk_downloads_authorized_now=False,
                   global_scope="global_details_requested; capacity_and_quota_estimate_first",
                   exhaustive_all_internet_data=False)
    write_csv(directory / "data_inventory.csv", rows, FIELDS)
    write_csv(directory / "api_inventory.csv", providers, API_FIELDS)
    write_csv(directory / "source_groups.csv", registry.get("sources", []))
    write_csv(directory / "taifex_endpoints.csv", taifex.get("datasets", []))
    write_csv(directory / "discovered_public_datasets.csv", discovered,
              ("provider", "dataset_id", "title", "advertised_first", "advertised_last",
               "year_or_vintage", "upstream_cadence", "documentation", "capture_at",
               "history_downloaded", "evidence"))
    atomic_write_json(directory / "catalog_issues.json", catalog_issues)
    atomic_write_json(directory / "summary.json", summary)
    lines = ["# 全來源 API 與資料清冊", "", f"監控快照：{summary['snapshot_at_utc']}", "",
             f"登錄 {len(rows):,} 筆監控來源列、{len(providers)} 筆憑證來源、{summary['source_groups']} 個公共來源群組。",
             "這些不是互斥的 API、股票或特徵數；FinLab 別名、TAIFEX 端點與彙總列有重疊，不可相加當資料量。", "",
             "## 可直接查的清冊", "",
             "- [資料與排程](data_inventory.csv)：實際首末筆／列數證據、抓取頻率、推估發布時間、下次排程、配額歸屬、阻擋原因。",
             "- [API 位置](api_inventory.csv)：只含環境變數名稱與存在狀態；不含 key、帳號、前後綴或雜湊。",
             "- [公開來源群組](source_groups.csv)：來源文件、歷史取得方式、實作及研究使用限制。",
             "- [TAIFEX 端點明細](taifex_endpoints.csv)：官方目錄、取得狀況、實際日期與未證實缺口。", "",
             f"另將已校驗的 NOAA／Census／FIRMS／BEA 目錄展開為 [{len(discovered):,} 個資料集／年份項目](discovered_public_datasets.csv)；",
             "這是可取得資料的目錄，不是已下載其中所有觀測值。缺目錄／格式問題另列於 catalog_issues.json。", "",
             "## 日期、筆數與完整性怎麼看", "",
             "`first_observed/last_observed/record_count` 依 `record_evidence` 與 `record_count_basis` 判讀；空白是未量測，不是零。",
             "FinLab 寬表的列數通常是日期列，不是日期乘全股票數。不同來源粒度不同，不計算跨來源總筆數。",
             "`receipt_*` 是下載器收據所報告的實際資料日期／列數，未重驗每個檔案雜湊，不冒充唯一庫存總筆數。",
             "`configured_request_start` 僅是排程查詢下界，不能證明來源最早日期或區間內無缺口。",
             "`expected_release_*` 是來源發布契約或推估；`next_run_*` 是本機排程，兩者不是同一件事。",
             "API 憑證存在不等於登入有效、付費權限、歷史完整或允許全球大量下載。", "",
             "## 全球明細容量與配額", "",
             "依使用者要求先估容量／配額；新的有 key 來源先做有界目錄或目前快照探測，不下載全部衛星原始影像。",
             "既有金融回補仍由原下載器、共用限速器與收據負責；本匯出不另開重複下載。",
             "詳細估算見 docs/global_public_data_capacity_2026-09-27.md。", "",
             "## API 設定現況", "", "|來源|憑證存在狀態|已映射資料列（含別名）|", "|---|---|---:|"]
    for item in providers:
        lines.append(f"|{item['provider']}|{item['credential_state']}|{item['registered_data_rows']}|")
    atomic_write_text(directory / "README.md", "\n".join(lines) + "\n")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--output", type=Path, help="Write CSV here instead of stdout")
    parser.add_argument("--policy-output", type=Path,
                        help="Optional local provider/admission summary JSON; no provider API")
    parser.add_argument("--bundle-dir", type=Path, help="Write API/data/schedule CSVs and an evidence guide")
    parser.add_argument("--credentials", type=Path, default=ROOT / "artifacts/data_credentials/status.json")
    parser.add_argument("--source-registry", type=Path, default=ROOT / "configs/free_public_data_sources.json")
    parser.add_argument("--taifex-inventory", type=Path,
                        default=ROOT / "artifacts/data_quality/taifex_public_inventory_2026-09-27/taifex_public_inventory.json")
    args = parser.parse_args(argv)
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    rows = rows_from_snapshot(snapshot)
    enrich_finmind_receipts(rows, ROOT)
    if args.bundle_dir:
        summary = write_bundle(args.bundle_dir, rows, credentials=read_object(args.credentials),
                               registry=read_object(args.source_registry), taifex=read_object(args.taifex_inventory),
                               evidence_root=ROOT)
        print(json.dumps({"bundle_dir": str(args.bundle_dir), "registered_rows": len(rows),
                          "api_credential_entries": summary["api_credential_entries"]}, ensure_ascii=False))
    if args.policy_output:
        summary = policy_summary(rows)
        summary["api_validation_admission"] = evaluate_secondary_admission(snapshot_path=args.snapshot)
        atomic_write_json(args.policy_output, summary)
    if args.output:
        write_csv(args.output, rows, FIELDS)
    elif not args.bundle_dir:
        writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
