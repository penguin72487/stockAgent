"""Source-qualified v2 extension of the existing Frankfurter downloader.

ECB continues to own legacy pair files. Never overwrite them with v2's default
blended rates. One native pivot/base request serves every quote for a provider.
"""
from dataclasses import asdict
import fcntl
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json
from downloader.download_keyed_public_catalogs import CatalogError, RequestBudget, SafeCatalogTransport
from downloader.download_public_economic_history import acquire, is_due, now, read_json
from downloader.public_economic_sources import EconomicJob

DEFAULT_PROVIDERS = ("BBK", "BOC", "RBA", "AMCM", "HKMA")


def official_jobs(catalog: list[dict], selected=DEFAULT_PROVIDERS) -> list[EconomicJob]:
    jobs = []
    for row in catalog:
        key = row.get("key", "")
        if key not in selected or key == "ECB":
            continue
        if not row.get("pivot_currency") or not row.get("start_date") or not row.get("end_date"):
            raise CatalogError("invalid_payload_schema")
        # Endpoint-specific provider, never anonymous blended /v2/rates.
        jobs.append(EconomicJob("frankfurter", key, f"https://api.frankfurter.dev/v2/providers/{key.lower()}/rates",
                                {"base": row["pivot_currency"], "from": row["start_date"], "to": row["end_date"]},
                                ttl_seconds=86400, interval=0.1, kind="official_reference_rate"))
    if set(j.dataset for j in jobs) != set(selected):
        raise CatalogError("invalid_payload_schema")
    return sorted(jobs, key=lambda job: (job.params["from"], job.dataset))


def run_official_history(root: Path, *, max_requests: int = 16) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    budget = RequestBudget(max_requests)
    with (root / ".download.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = root / "catalog.json"
        cached = read_json(path)
        if is_due(cached, now(), 86400):
            spec = EconomicJob("frankfurter", "providers", "https://api.frankfurter.dev/v2/providers", {}).transport_spec()
            client = SafeCatalogTransport(spec, budget, maximum_bytes=2 * 1024**2, secrets=())
            rows = json.loads(client.fetch(spec.endpoint, {"User-Agent": "stockAgent-economic-research/1.0", "Accept": "application/json"}))
            official_jobs(rows)  # Validate before replacing the good catalog.
            cached = {"observed_at_utc": now().isoformat(), "providers": rows}
            atomic_write_json(path, cached)
        jobs = official_jobs(cached["providers"])
        results = [acquire(job, root, {}, budget, max_bytes=64 * 1024**2, min_free_bytes=20 * 1024**3) for job in jobs]
        summary = {"provider": "frankfurter", "contract": "source-separated native pivot rates; no blending",
                   "updated_at_utc": now().isoformat(), "requests": budget.used, "datasets": results,
                   "history_complete": False, "jobs": [asdict(j) for j in jobs]}
        atomic_write_json(root / "download_summary.json", summary)
        return summary
