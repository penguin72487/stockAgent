"""Resumable numeric Census/BEA and Taiwan MOI archives on canonical primitives.

Default invocation is a zero-network plan. Independent providers may run in
parallel; each has a shared host limiter, a request budget, a writer lock and
durable per-dataset receipts. Historical completion applies only to the declared
query, never all data offered by a provider or historical point-in-time safety.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file
from downloader.common import load_env_file
from downloader.download_keyed_public_catalogs import (
    CatalogError, RequestBudget, SafeCatalogTransport, _redact_value,
)
from downloader.public_economic_sources import (
    EconomicJob, core_jobs, moi_season_jobs, parse_economic_json, parse_moi_zip, request_url,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_VERSION = 2
PROVIDERS = ("census", "bea", "moi")
KEY_NAMES = ("CENSUS_API_KEY", "BEA_API_KEY")
AUTH_FAILURES = frozenset({"credential_rejected", "credential_activation_required", "http_401", "http_403"})


def file_signature(path: Path) -> list[int]:
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def now() -> datetime:
    return datetime.now(UTC)


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def receipt_path(root: Path, job: EconomicJob) -> Path:
    return root / "receipts" / job.provider / f"{job.dataset}.json"


def job_fingerprint(job: EconomicJob) -> str:
    contract = asdict(job)
    if job.provider == "frankfurter":
        contract["params"].pop("to", None)  # moving watermark, not source identity
    return hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()


def verified_receipt(root: Path, job: EconomicJob) -> dict:
    receipt = read_json(receipt_path(root, job))
    if (receipt.get("contract_version") != (3 if job.provider == "moi" else CONTRACT_VERSION)
            or receipt.get("query_fingerprint") != job_fingerprint(job)
            or receipt.get("status") != "acquired" or not receipt.get("files")):
        return {}
    try:
        for item in receipt["files"]:
            rel = Path(item["path"])
            path = root / rel
            if (rel.is_absolute() or ".." in rel.parts or not path.resolve().is_relative_to(root.resolve())
                    or not path.is_file() or path.stat().st_size != item["bytes"]):
                return {}
            # Immutable version files were hashed at publication. A changed
            # inode/ctime/mtime/size requires a new hash; unchanged files need
            # only stat checks, not an hourly scan of every historical byte.
            signature = file_signature(path)
            if signature != item.get("verified_file_signature"):
                if sha256_file(path) != item["sha256"] or file_signature(path) != signature:
                    return {}
    except (OSError, KeyError, TypeError):
        return {}
    return receipt


def is_due(receipt: dict, stamp: datetime, seconds: int) -> bool:
    try:
        observed = datetime.fromisoformat(receipt["observed_at_utc"])
        return observed.tzinfo is None or stamp < observed or stamp >= observed + timedelta(seconds=seconds)
    except (ValueError, KeyError, TypeError):
        return True


def correction_start(receipt: dict) -> str | None:
    """Two observation-year revision overlap, full historical recheck quarterly."""
    if not receipt or is_due({"observed_at_utc": receipt.get("last_full_history_check_at_utc")}, now(), 90 * 86400):
        return None
    latest = receipt.get("last_observation")
    try:
        return f"{int(str(latest)[:4]) - 2}-01-01"
    except (ValueError, TypeError):
        return None


def merge_revision(old: pa.Table, fresh: pa.Table, date_column: str, since: str) -> pa.Table:
    # Replace the queried period, not only equal keys. Removed/revised rows must
    # not survive forever. Empty or malformed responses never reach this path.
    if date_column not in old.column_names or date_column not in fresh.column_names:
        raise CatalogError("invalid_payload_schema")
    if any(str(value)[:4] < since[:4] for value in fresh[date_column].to_pylist()):
        raise CatalogError("invalid_payload_schema")
    retained = old.filter(pc.less(pc.utf8_slice_codeunits(old[date_column], 0, 4), since[:4]))
    return pa.concat_tables([retained, fresh], promote_options="default")


def _file_entry(root: Path, path: Path, **extra) -> dict:
    signature = file_signature(path)
    digest = sha256_file(path)
    if signature != file_signature(path):
        raise CatalogError("source_changed_during_validation")
    return {"path": str(path.relative_to(root)), "bytes": path.stat().st_size,
            "sha256": digest, "verified_file_signature": signature, **extra}


def acquire(job: EconomicJob, root: Path, credentials: dict[str, str], budget: RequestBudget,
            *, max_bytes: int, min_free_bytes: int, transport_factory=SafeCatalogTransport,
            recheck_credentials: bool = False) -> dict:
    stamp = now()
    previous = verified_receipt(root, job)
    row = {"provider": job.provider, "dataset": job.dataset, "status": "pending",
           "rows": previous.get("rows", 0), "history_complete": False,
           "checked_at_utc": stamp.isoformat()}
    if job.credential and not credentials.get(job.credential):
        return {**row, "status": "credential_missing"}
    if previous and not is_due(previous, stamp, job.ttl_seconds):
        return {**previous, "status": "current_cached"}
    attempt_path = root / "attempts" / job.provider / f"{job.dataset}.json"
    attempt = read_json(attempt_path)
    try:
        if (datetime.fromisoformat(attempt.get("retry_at_utc", "")) > stamp
                and not (recheck_credentials and attempt.get("status") in AUTH_FAILURES)):
            return {**row, "status": attempt["status"], "retry_at_utc": attempt["retry_at_utc"]}
    except (ValueError, TypeError):
        pass
    before = budget.used
    client = None
    try:
        if shutil.disk_usage(root).free < min_free_bytes + max_bytes * 4:
            raise CatalogError("disk_reserve_reached")
        client = transport_factory(job.transport_spec(), budget, maximum_bytes=max_bytes,
                                   secrets=tuple(credentials.values()))
        since = correction_start(previous) if job.provider in {"census", "bea", "frankfurter"} else None
        body = None
        legacy = read_json(receipt_path(root, job))
        raw_receipt_path = root / "raw_receipts" / job.provider / f"{job.dataset}.json"
        candidates = [legacy]
        if job.provider == "moi":
            candidates.append(read_json(raw_receipt_path))
        observed = None
        for candidate in candidates:
            if (candidate.get("query_fingerprint") == job_fingerprint(job) and not candidate.get("incremental_since")
                    and not is_due(candidate, stamp, job.ttl_seconds)):
                # Parser upgrades and failed normalizations reuse verified raw
                # input. A raw-only receipt is never acquisition completion.
                for item in candidate.get("files", []):
                    path = root / item.get("path", "")
                    if (item.get("kind") == "raw" and path.resolve().is_relative_to(root.resolve())
                            and path.is_file() and sha256_file(path) == item.get("sha256")):
                        body = path.read_bytes()
                        observed = candidate["observed_at_utc"]
                        break
            if body is not None:
                break
        if body is None:
            body = client.fetch(request_url(job, credentials.get(job.credential or "", ""), since=since),
                                {"User-Agent": "stockAgent-economic-research/1.0", "Accept": "application/json,application/zip"})
        observed = observed or now().isoformat()
        if job.provider == "moi":
            # Preserve this bounded, credential-free public archive before
            # parsing. Local failures must not consume another network quota.
            digest = hashlib.sha256(body).hexdigest()
            raw_path = root / "objects" / job.provider / f"{digest}.zip"
            if not raw_path.exists():
                atomic_write_bytes(raw_path, body)
            elif sha256_file(raw_path) != digest:
                raise CatalogError("existing_object_hash_mismatch")
            atomic_write_json(raw_receipt_path, {"status": "raw_acquired_unvalidated", "provider": job.provider,
                              "dataset": job.dataset, "query_fingerprint": job_fingerprint(job),
                              "observed_at_utc": observed, "history_complete": False,
                              "files": [_file_entry(root, raw_path, kind="raw")]})
            tables, metadata = parse_moi_zip(body)
            encoded, suffix = body, "zip"
        else:
            table, metadata = parse_economic_json(job, body)
            # BEA's request echo contains UserID. Retain only sanitized JSON.
            clean = _redact_value(json.loads(body, parse_float=str), tuple(credentials.values()))
            encoded, suffix = json.dumps(clean, ensure_ascii=False, sort_keys=True).encode(), "json"
            if since and previous:
                old_files = [f for f in previous["files"] if f.get("kind") == "parquet"]
                if len(old_files) != 1:
                    raise CatalogError("invalid_payload_schema")
                table = merge_revision(pq.read_table(root / old_files[0]["path"]), table, metadata["date_column"], since)
                values = table[metadata["date_column"]].to_pylist()
                metadata.update(first_observation=min(values), last_observation=max(values))
            tables = {"observations": table}
        digest = hashlib.sha256(encoded).hexdigest()
        raw_path = root / "objects" / job.provider / f"{digest}.{suffix}"
        if not raw_path.exists():
            atomic_write_bytes(raw_path, encoded)
        elif sha256_file(raw_path) != digest:
            raise CatalogError("existing_object_hash_mismatch")
        files = [_file_entry(root, raw_path, kind="raw")]
        # Versioned outputs: publish a receipt only after every member succeeds.
        # Old good data remains reachable if a new response/parse is interrupted.
        generation = now().isoformat().replace(":", "").replace("+", "_")
        for name, table in tables.items():
            path = root / "normalized" / job.provider / job.dataset / generation / f"{name}.parquet"
            table = table.replace_schema_metadata({b"source": job.provider.encode(),
                     b"dataset": job.dataset.encode(), b"observed_at_utc": observed.encode(),
                     b"historical_point_in_time": b"false", b"raw_sha256": digest.encode()})
            atomic_write_parquet(path, table)
            files.append(_file_entry(root, path, kind="quarantine" if name == "rejected_rows" else "parquet", rows=table.num_rows))
        receipt = {**row, **metadata, "status": "acquired", "contract_version": 3 if job.provider == "moi" else CONTRACT_VERSION,
                   "query_fingerprint": job_fingerprint(job), "source": job.endpoint,
                   "query": job.params, "incremental_since": since, "observed_at_utc": observed,
                   "last_full_history_check_at_utc": previous.get("last_full_history_check_at_utc") if since else observed,
                   "normalized_at_utc": now().isoformat(),
                   "next_check_at_utc": (datetime.fromisoformat(observed) + timedelta(seconds=job.ttl_seconds)).isoformat(),
                   "files": files, "rows": metadata.get("observation_rows", sum(t.num_rows for t in tables.values())),
                   "requests_this_run": budget.used - before, "history_complete": False,
                   "requested_scope_acquired": True, "historical_point_in_time": False,
                   "publication_time_status": "unknown; source period is not publication time"}
        atomic_write_json(root / "receipt_history" / job.provider / job.dataset / f"{generation}.json", receipt)
        atomic_write_json(receipt_path(root, job), receipt)
        return receipt
    except Exception as exc:
        reason = str(exc) if isinstance(exc, CatalogError) else "local_processing_failed"
        delay = max(60, getattr(client, "retry_after_seconds", 0))
        if reason in AUTH_FAILURES:
            delay = max(delay, 3600)
        elif reason not in {"provider_throttled", "http_429", "provider_cooldown"}:
            delay = max(delay, 300)
        row.update(status=reason, requests_this_run=budget.used - before,
                   error_type=type(exc).__name__,
                   retry_at_utc=(now() + timedelta(seconds=delay)).isoformat(),
                   previous_good_receipt=bool(previous))
        if reason != "request_budget_exhausted":
            atomic_write_json(attempt_path, row)
        return row


def run_provider(provider: str, root: Path, credentials: dict[str, str], *, max_requests: int,
                 max_bytes: int, min_free_bytes: int, recheck_credentials: bool = False) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    budget = RequestBudget(max_requests)
    with (root / f".{provider}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"provider": provider, "state": "worker_active"}
        jobs = [j for j in core_jobs() if j.provider == provider]
        if provider == "moi":
            catalog_path = root / "catalog" / "moi_seasons.json"
            catalog = read_json(catalog_path)
            if is_due(catalog, now(), 86400):
                discovery = EconomicJob("moi", "season_catalog", "https://plvr.land.moi.gov.tw/DownloadSeason_ajax_list", {})
                try:
                    client = SafeCatalogTransport(discovery.transport_spec(), budget, maximum_bytes=max_bytes, secrets=())
                    body = client.fetch(discovery.endpoint, {})
                    jobs = moi_season_jobs(body)
                    catalog = {"observed_at_utc": now().isoformat(), "jobs": [asdict(j) for j in jobs]}
                    atomic_write_json(catalog_path, catalog)
                except Exception:
                    if not catalog.get("jobs"):
                        return {"provider": provider, "state": "catalog_unavailable", "requests": budget.used}
            jobs = [EconomicJob(**item) for item in catalog["jobs"]]
            # Current release is separate from historical quarterly batches.
            jobs.insert(0, EconomicJob("moi", "current", "https://plvr.land.moi.gov.tw/Download",
                                      {"type": "zip", "fileName": "lvr_landcsv.zip"}, kind="published_snapshot"))
        rows = []
        for job in jobs:
            row = acquire(job, root, credentials, budget, max_bytes=max_bytes, min_free_bytes=min_free_bytes,
                          recheck_credentials=recheck_credentials)
            rows.append(row)
            atomic_write_json(root / "status" / f"{provider}.json",
                              {"provider": provider, "state": "running", "updated_at_utc": now().isoformat(),
                               "planned_datasets": len(jobs), "processed": len(rows), "requests": budget.used, "datasets": rows})
            # One credential/quota failure is provider-wide. Other independent
            # providers continue in their own worker instead of sleeping here.
            if row["status"] in AUTH_FAILURES | {"http_429", "provider_throttled"}:
                rows += [{"provider": provider, "dataset": j.dataset, "status": "provider_deferred"} for j in jobs[len(rows):]]
                break
        acquired = sum(r["status"] in {"acquired", "current_cached"} for r in rows)
        result = {"provider": provider, "state": "requested_scope_current" if acquired == len(jobs) else "partial",
                  "updated_at_utc": now().isoformat(), "planned_datasets": len(jobs), "acquired_datasets": acquired,
                  "requests": budget.used, "max_requests": max_requests, "datasets": rows,
                  "history_complete": False, "historical_point_in_time": False}
        atomic_write_json(root / "status" / f"{provider}.json", result)
        return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--providers", nargs="+", choices=PROVIDERS, default=list(PROVIDERS))
    parser.add_argument("--output-root", type=Path, default=ROOT / "data_public_economic")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--recheck-credentials", action="store_true",
                        help="After key activation/replacement, recheck auth failures now; never bypass quota/429 cooldowns")
    parser.add_argument("--max-requests", type=int, default=24, help="per independent provider, including retries")
    parser.add_argument("--max-response-mb", type=float, default=64)
    parser.add_argument("--min-free-gb", type=float, default=20)
    args = parser.parse_args(argv)
    if not 0 <= args.max_requests <= 100 or not math.isfinite(args.max_response_mb) or not 0 < args.max_response_mb <= 64 or not math.isfinite(args.min_free_gb) or args.min_free_gb < 0:
        parser.error("invalid request or storage budget")
    if not args.execute:
        print(json.dumps({"mode": "plan", "requests": 0, "jobs": [asdict(j) for j in core_jobs() if j.provider in args.providers],
                          "moi": "discover published seasons and current ZIP", "max_response_bytes": int(args.max_response_mb * 1024**2)}, ensure_ascii=False))
        return 0
    load_env_file(args.env_file, allowed_names=KEY_NAMES)
    credentials = {key: os.environ.get(key, "").strip() for key in KEY_NAMES}
    with ThreadPoolExecutor(max_workers=len(set(args.providers))) as pool:
        results = list(pool.map(lambda provider: run_provider(provider, args.output_root, credentials,
                           max_requests=args.max_requests, max_bytes=int(min(args.max_response_mb, 16 if provider == "bea" else 64) * 1024**2),
                           min_free_bytes=int(args.min_free_gb * 1024**3),
                           recheck_credentials=args.recheck_credentials), dict.fromkeys(args.providers)))
    # Summary is assembled from all provider files, including providers not
    # selected in a targeted repair run. It is not a global completeness claim.
    summary = {"schema_version": CONTRACT_VERSION, "updated_at_utc": now().isoformat(),
               "history_complete": False, "providers": [read_json(args.output_root / "status" / f"{p}.json") for p in PROVIDERS]}
    atomic_write_json(args.output_root / "download_summary.json", summary)
    print(json.dumps({"providers": [{k: r.get(k) for k in ["provider", "state", "requests", "planned_datasets", "acquired_datasets"]} for r in results]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
