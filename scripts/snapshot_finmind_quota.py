"""Sample official FinMind account quota without exposing a token to the UI."""

from pathlib import Path
import os

import requests

from downloader.common import load_env_file
from downloader.finmind_account import verified_account


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    load_env_file(repo_root / ".env", allowed_names=("FINMIND_TOKEN",))
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        return 2
    with requests.Session() as session:
        account = verified_account(session, token, repo_root / "data_finmind")
    print(f"FinMind {account['tier']}: {account['provider_used_in_hour']}/"
          f"{account['official_requests_per_hour']} requests in provider hour")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
