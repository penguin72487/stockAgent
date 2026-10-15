"""Inspect authorized CNA setup or bounded MCP capabilities, never article data.

Default is local-only. --connect requires a key, the reviewed configuration,
and the same exclusive HTTP-owner lock used by the canonical news service.
This does not install a Codex connector, log in, refresh keys or call a paid
article tool. Missing credentials are not permission denial or completion.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path

from downloader.artifact_io import atomic_write_json
from downloader.cna_authorized_api import CNA_ENDPOINT, CNA_KEY_NAME, discover_cna_capabilities, load_cna_key
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import utc_now


def inspect_setup(config_path, env_file, *, connect=False, http_root=None):
    config = json.loads(config_path.read_text())
    if (config.get("contract_version") != 1 or config.get("provider") != "cna"
            or config.get("endpoint") != CNA_ENDPOINT or config.get("key_env_name") != CNA_KEY_NAME
            or config.get("user_authorization_declared") is not True
            or config.get("phase") != "capability_discovery_only"
            or any(config.get(field) is not False for field in
                   ("fulltext_pipeline_enabled", "article_tool_calls_enabled", "publish", "remote_training_export"))):
        raise NewsError("cna_unreviewed_capability_config")
    key = load_cna_key(env_file)
    result = {"contract_version": 1, "provider": "cna", "observed_at_utc": utc_now(),
              "endpoint": CNA_ENDPOINT, "key_env_name": CNA_KEY_NAME, "credential_present": bool(key),
              "user_authorization_declared": True, "connection_verified": False,
              "http_requests_sent": 0, "tool_calls_sent": 0, "tools": [],
              "state": "local_configured_not_connected" if key else "credential_missing",
              "full_text_entitlement_verified": False, "quota_verified": False,
              "history_start_verified": False, "all_history_complete": False}
    if not connect or not key:
        return result
    if http_root is None:
        # Resolve the very same configured lock, not a parallel hard-coded
        # workspace if the canonical collector root changes later.
        from downloader.news_fulltext import load_config
        news_config, _, repo = load_config(Path("configs/news_fulltext.json"))
        http_root = repo / news_config["root"]
    http_root.mkdir(parents=True, exist_ok=True)
    with (http_root / "http-owner.lock").open("a") as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise NewsError("news_http_owner_already_running") from None
        try:
            result.update(discover_cna_capabilities(key))
        finally:
            fcntl.flock(guard, fcntl.LOCK_UN)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news/cna_authorized_api.json"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--connect", action="store_true", help="Initialize/list tools only, bounded to 45s/8 pages")
    args = parser.parse_args(argv)
    try:
        result = inspect_setup(args.config, args.env_file, connect=args.connect)
    except NewsError as exc:
        result = {"contract_version": 1, "provider": "cna", "observed_at_utc": utc_now(),
                  "state": "capability_check_failed", "error": exc.code,
                  "connection_verified": False, "all_history_complete": False}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    os.chmod(args.output, 0o600)
    print(json.dumps({field: result.get(field) for field in
                      ("provider", "state", "credential_present", "connection_verified",
                       "http_requests_sent", "tool_calls_sent", "tool_count", "error")}, ensure_ascii=False))
    return 0 if result["state"] != "capability_check_failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
