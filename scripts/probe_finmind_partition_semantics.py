"""Bounded diagnostic: one existing blocked query per named FinMind source."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import SourceError, _fetch_rows
from downloader.download_finmind_sponsor import _end
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import fixed_incremental_demand


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    root = repo / "data_finmind"
    load_env_file(repo / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    targets = ("TaiwanStockInfoWithWarrantSummary", "TaiwanBusinessIndicator",
               "CnnFearGreedIndex", "TaiwanOptionVix")
    report = {"observed_at_utc": datetime.now(UTC).isoformat(), "mode": "bounded_query_shape_probe",
              "max_requests": len(targets), "probes": []}
    with sqlite3.connect(f"file:{root / 'sponsor/queue.sqlite3'}?mode=ro", uri=True) as conn, requests.Session() as session:
        account = verified_account(session, token, root)
        limiter = rate_limiter(account)
        for dataset in targets:
            now = datetime.now(UTC)
            budget = backfill_budget(account, root, fixed_incremental_requests=fixed_incremental_demand(root, now), now=now)
            if not budget["allowed"]:
                report["halted"] = budget["basis"]
                break
            row = conn.execute("SELECT partition,kind FROM tasks WHERE dataset=? AND state='blocked' "
                               "AND error_code='response_outside_partition' ORDER BY partition DESC LIMIT 1", (dataset,)).fetchone()
            if row is None:
                continue
            start = date.fromisoformat(row[0])
            end = _end(start, row[1])
            params = {"dataset": dataset, "start_date": str(start), "end_date": str(end)}
            record = {"params": params, "started_at_utc": now.isoformat()}
            try:
                rows = _fetch_rows(session, limiter, root, dataset, token, params)
            except SourceError as error:
                record["error_code"] = error.code
                report["probes"].append(record)
                break
            stamps = [row.get("date") for row in rows]
            valid = [stamp[:10] for stamp in stamps if isinstance(stamp, str)]
            outside = sorted({stamp for stamp in valid if not str(start) <= stamp < str(end)})
            raw = json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
            record.update({"rows": len(rows), "invalid_date_types": len(stamps) - len(valid),
                           "source_first_date": min(valid, default=None), "source_last_date": max(valid, default=None),
                           "outside_dates": outside[:12], "outside_date_count": len(outside),
                           "outside_rows": sum(not str(start) <= stamp < str(end) for stamp in valid),
                           "response_rows_canonical_sha256": hashlib.sha256(raw).hexdigest(),
                           "response_rows_canonical_bytes": len(raw), "completed_at_utc": datetime.now(UTC).isoformat()})
            report["probes"].append(record)
            del rows, raw, valid, stamps
    # A later diagnostic (including zero remaining blocked rows) must not
    # overwrite the original evidence used to authorize a query-shape repair.
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    output = repo / "artifacts/data_quality" / f"finmind_partition_semantics_{stamp}.json"
    atomic_write_json(output, report)
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
