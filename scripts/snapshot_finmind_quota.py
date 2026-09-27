"""Sample official FinMind account quota without exposing a token to the UI."""

from pathlib import Path
import argparse
import os

import requests

from downloader.common import load_env_file
from downloader.finmind_account import verified_account


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--eta-only', action='store_true', help='estimate from local receipts only; no provider call')
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if args.eta_only:
        from downloader.finmind_eta import snapshot_finmind_estimate
        estimate = snapshot_finmind_estimate(repo_root / 'data_finmind')
        print(f"FinMind completion scenarios: {estimate['state']}")
        return 0
    load_env_file(repo_root / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        return 2
    with requests.Session() as session:
        account = verified_account(session, token, repo_root / "data_finmind")
    print(f"FinMind {account['tier']}: {account['provider_used_in_hour']}/"
          f"{account['official_requests_per_hour']} requests in provider hour")
    # One bounded local snapshot per existing quota observation, not per browser
    # refresh. Estimation cannot trigger a market-data API or alter a queue.
    if (repo_root / 'data_finmind' / 'eta_status.json').is_file():
        try:
            from downloader.finmind_eta import snapshot_finmind_estimate
            estimate = snapshot_finmind_estimate(repo_root / 'data_finmind')
        except Exception as error:
            # Preserve the last evidence (it expires after five minutes). An
            # estimator failure must not invalidate a successful quota sample.
            print(f"FinMind ETA sample failed: {type(error).__name__}; previous estimate expires normally")
        else:
            print(f"FinMind completion scenarios: {estimate['state']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
