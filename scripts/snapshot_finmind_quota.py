"""Sample official FinMind account quota without exposing a token to the UI."""

from pathlib import Path
import argparse
from datetime import UTC, datetime
import os
import time

import requests

from downloader.common import load_env_file
from downloader.artifact_io import atomic_write_json
from downloader.finmind_account import backfill_budget, verified_account
from downloader.finmind_eta import snapshot_finmind_estimate
from downloader.finmind_scheduling import incremental_reservation


ETA_ERROR_CODES = frozenset({
    'priority_override_query_grain_unverified',
    'priority_override_must_be_subset_of_required_history',
    'stage_request_counts_do_not_reconcile',
})


def sample_local_dispatch(root: Path, account: dict, *, now: datetime | None = None) -> dict:
    """One minute-sampled scheduling receipt, independent of ETA success.

    The gateway displays this decision's timestamp; it does not reimplement
    worker admission or open the active WAL ledger to invent a newer balance.
    No token, raw provider message, queue mutation or additional API call.
    """
    now = now or datetime.now(UTC)
    from stockagent.live.finmind_dashboard import _traffic
    plan = incremental_reservation(root, now)
    budget = backfill_budget(account, root, fixed_incremental_requests=plan['reserve_requests'],
                             now=now, prioritize_due=True, reservation_plan=plan)
    receipt = {'schema_version': 2, 'observed_at_utc': now.isoformat(),
               'account_observed_at_utc': account.get('observed_at_utc'),
               'official_requests_per_hour': account['official_requests_per_hour'],
               'allocation': budget, 'traffic': _traffic(root, now, account['official_requests_per_hour']),
               'extra_provider_calls': 0}
    atomic_write_json(root / 'dispatch_status.json', receipt)
    return receipt


def sample_local_eta(root: Path) -> dict | None:
    """Publish an ETA only after a full snapshot; keep refresh health separate.

    A successful quota observation does not imply a successful estimate. The
    short local health receipt explains failures without publishing exception
    text, overwriting old estimate evidence, or calling the provider again.
    """
    started, clock = datetime.now(UTC), time.monotonic()
    try:
        estimate = snapshot_finmind_estimate(root)
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and str(error) in ETA_ERROR_CODES else 'eta_snapshot_failed'
        health = {'schema_version': 1, 'state': 'failed', 'error_code': code,
                  'exception_type': type(error).__name__}
        estimate = None
        print(f"FinMind ETA sample failed: {health['exception_type']} ({code}); previous estimate expires normally")
    else:
        health = {'schema_version': 1, 'state': 'ok', 'error_code': None,
                  'estimate_observed_at_utc': estimate.get('observed_at_utc'),
                  'estimate_state': estimate['state']}
        print(f"FinMind completion scenarios: {estimate['state']}")
    health.update(attempt_started_at_utc=started.isoformat(),
                  finished_at_utc=datetime.now(UTC).isoformat(),
                  elapsed_seconds=round(time.monotonic() - clock, 3))
    try:
        atomic_write_json(root / 'eta_refresh_status.json', health)
    except OSError:
        print('FinMind ETA refresh health receipt unavailable')
    return estimate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--eta-only', action='store_true', help='estimate from local receipts only; no provider call')
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if args.eta_only:
        return 0 if sample_local_eta(repo_root / 'data_finmind') is not None else 1
    load_env_file(repo_root / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        return 2
    with requests.Session() as session:
        account = verified_account(session, token, repo_root / "data_finmind")
    print(f"FinMind {account['tier']}: {account['provider_used_in_hour']}/"
          f"{account['official_requests_per_hour']} requests in provider hour")
    sample_local_dispatch(repo_root / 'data_finmind', account)
    # One bounded local snapshot per existing quota observation, not per browser
    # refresh. Estimation cannot trigger a market-data API or alter a queue.
    # Bootstrap missing estimates too; a removed/never-created ETA file must
    # not permanently disable every following minute's refresh.
    sample_local_eta(repo_root / 'data_finmind')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
