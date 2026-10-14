#!/usr/bin/env python3
"""Provision one verified DigitalPlat hostname on Cloudflare's Free plan.

DNS delegation remains a registrar operation. This command never changes the
router, the legacy DDNS entry, trading services, or an unrelated DNS record.
Writes require --apply; ambiguous writes are not automatically retried.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

import requests
from dotenv.parser import parse_stream

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_text

CF_API = "https://api.cloudflare.com/client/v4"
DP_API = "https://domain-api.digitalplat.org/api/v1"
ORIGIN = "http://127.0.0.1:8770"


def read_key(path: Path, name: str) -> str:
    if path.stat().st_mode & 0o077:
        raise ValueError("Credential file must not be accessible to group/other")
    with path.open() as stream:
        matches = [binding for binding in parse_stream(stream) if binding.key == name]
    if len(matches) != 1 or matches[0].error or not matches[0].value:
        raise ValueError(f"Exactly one valid {name} assignment is required")
    value = matches[0].value.strip()
    if not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"Invalid {name} format")
    return value


def session(key: str) -> requests.Session:
    result = requests.Session()
    result.trust_env = False
    result.headers.update({"Authorization": "Bearer " + key})
    return result


def cf_call(client: requests.Session, method: str, path: str, **kwargs):
    response = client.request(
        method, CF_API + path, timeout=(5, 25), allow_redirects=False, **kwargs
    )
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError(f"Cloudflare returned non-JSON HTTP {response.status_code}") from None
    if not response.ok or not payload.get("success"):
        codes = [error.get("code") for error in payload.get("errors", [])]
        raise RuntimeError(f"Cloudflare {method} failed: HTTP {response.status_code}, codes={codes}")
    return payload["result"]


def require_owned_domain(hostname: str, payload: dict) -> dict:
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.dpdns\.org", hostname):
        raise ValueError("Expected one direct dpdns.org hostname")
    if payload.get("success") is not True or not isinstance(payload.get("data"), list):
        raise ValueError("DigitalPlat ownership inventory was not accepted")
    matches = [row for row in payload["data"] if row.get("domain") == hostname]
    if len(matches) != 1 or matches[0].get("status") != "ok":
        raise ValueError("Target must be uniquely present and active in the DigitalPlat account")
    return matches[0]


def require_free_zone(zone: dict, hostname: str) -> None:
    plan = zone.get("plan", {})
    if (zone.get("name") != hostname or zone.get("type") != "full"
            or plan.get("legacy_id") != "free" or plan.get("price") != 0):
        raise ValueError("Expected the exact hostname on a full Cloudflare Free zone")


def ingress_for(hostname: str) -> dict:
    return {"ingress": [
        {"hostname": hostname, "service": ORIGIN, "originRequest": {}},
        {"service": "http_status:404"},
    ]}


def require_compatible_ingress(config: dict | None, hostname: str) -> None:
    current = normalized_config(config)
    if not current or current == {"ingress": []}:
        return
    if current != ingress_for(hostname):
        raise ValueError("Existing tunnel configuration differs; refusing to overwrite it")


def normalized_config(config: dict | None) -> dict:
    result = dict(config or {})
    # Cloudflare adds this disabled default on readback even when the write
    # contains only public HTTP ingress. Never normalize enabled private routes.
    if result.get("warp-routing") == {"enabled": False}:
        result.pop("warp-routing")
    return result


def require_compatible_dns(records: list[dict], hostname: str, target: str) -> None:
    at_apex = [row for row in records if row.get("name") == hostname]
    for row in at_apex:
        if row.get("type") in {"A", "AAAA", "CNAME"} and not (
            row.get("type") == "CNAME" and row.get("content") == target
            and row.get("proxied") is True
        ):
            raise ValueError("Existing address record differs; refusing to replace it")


def private_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                      durable=True, mode=0o600)


def provision(args: argparse.Namespace) -> dict:
    dp = session(read_key(args.env_file, "DPDNS_API_KEY"))
    response = dp.get(DP_API + "/domains", timeout=(5, 20), allow_redirects=False)
    if response.status_code != 200:
        raise RuntimeError(f"DigitalPlat inventory HTTP {response.status_code}")
    owned = require_owned_domain(args.hostname, response.json())
    cf = session(read_key(args.env_file, "CLOUDFLARE_API_KEY"))
    accounts = cf_call(cf, "GET", "/accounts")
    if len(accounts) != 1:
        raise ValueError("Cloudflare account selection is ambiguous")
    account_id = accounts[0]["id"]
    zones = cf_call(cf, "GET", "/zones", params={"name": args.hostname})
    if len(zones) > 1:
        raise ValueError("Cloudflare zone selection is ambiguous")
    receipt = {
        "schema_version": 1, "hostname": args.hostname,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "registrar_nameservers_before": owned.get("nameservers", []),
        "registrar_expires_at": owned.get("expires_at"),
        "origin": ORIGIN, "actions": [], "stage": "planned",
        "legacy_ddns_changed": False, "router_changed": False,
        "registrar_ns_changed_by_this_command": False,
    }

    def save() -> None:
        receipt["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        private_json(args.receipt, receipt)

    if not args.apply:
        if zones:
            require_free_zone(zones[0], args.hostname)
            receipt["nameservers"] = zones[0]["name_servers"]
        return receipt
    if args.receipt.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        prior = args.receipt.read_text()
        if json.loads(prior).get("hostname") != args.hostname:
            raise ValueError("Existing receipt belongs to another hostname")
        atomic_write_text(args.receipt.with_name(args.receipt.stem + "-" + stamp + ".json"),
                          prior, durable=True, mode=0o600)
    receipt["stage"] = "provisioning"
    save()
    if not zones:
        zone = cf_call(cf, "POST", "/zones", json={
            "name": args.hostname, "account": {"id": account_id}, "type": "full",
        })
        receipt["actions"].append("created_free_zone")
    else:
        zone = zones[0]
    require_free_zone(zone, args.hostname)
    if zone["account"]["id"] != account_id:
        raise ValueError("Target zone account differs")
    zone_id = zone["id"]
    receipt.update(account_id=account_id, zone_id=zone_id,
                   nameservers=zone["name_servers"], zone_status=zone["status"])
    save()

    health = requests.Session()
    health.trust_env = False
    response = health.get(ORIGIN + "/healthz", timeout=(2, 5))
    if response.status_code != 200:
        raise RuntimeError("Read-only public gateway is not reachable")
    tunnel_name = "stockagent-public-" + args.hostname.replace(".", "-")
    tunnels = cf_call(cf, "GET", f"/accounts/{account_id}/cfd_tunnel",
                      params={"is_deleted": "false"})
    matching = [item for item in tunnels if item.get("name") == tunnel_name]
    if len(matching) > 1:
        raise ValueError("Tunnel name selection is ambiguous")
    if matching:
        tunnel = matching[0]
        if tunnel.get("config_src") != "cloudflare":
            raise ValueError("Existing tunnel is not remotely managed")
    else:
        tunnel = cf_call(cf, "POST", f"/accounts/{account_id}/cfd_tunnel",
                         json={"name": tunnel_name, "config_src": "cloudflare"})
        receipt["actions"].append("created_remote_tunnel")
    tunnel_id = tunnel["id"]
    receipt.update(tunnel_id=tunnel_id, tunnel_name=tunnel_name)
    save()
    config_path = f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations"
    current = cf_call(cf, "GET", config_path).get("config")
    require_compatible_ingress(current, args.hostname)
    config = ingress_for(args.hostname)
    if normalized_config(current) != config:
        cf_call(cf, "PUT", config_path, json={"config": config})
        receipt["actions"].append("configured_loopback_readonly_origin")
        save()
    if normalized_config(cf_call(cf, "GET", config_path).get("config")) != config:
        raise RuntimeError("Tunnel ingress readback differs or is missing")

    target = tunnel_id + ".cfargotunnel.com"
    records_path = f"/zones/{zone_id}/dns_records"
    records = cf_call(cf, "GET", records_path, params={"name": args.hostname})
    require_compatible_dns(records, args.hostname, target)
    if not any(row.get("type") == "CNAME" for row in records):
        cf_call(cf, "POST", records_path, json={
            "name": args.hostname, "type": "CNAME", "content": target,
            "proxied": True, "ttl": 1,
        })
        receipt["actions"].append("created_proxied_tunnel_cname")
        save()
    reread = cf_call(cf, "GET", records_path, params={"name": args.hostname})
    require_compatible_dns(reread, args.hostname, target)
    if not any(row.get("type") == "CNAME" for row in reread):
        raise RuntimeError("Tunnel DNS record readback is missing")

    for setting, wanted in {"always_use_https": "on", "ssl": "strict",
                            "min_tls_version": "1.2", "ipv6": "on"}.items():
        endpoint = f"/zones/{zone_id}/settings/{setting}"
        before = cf_call(cf, "GET", endpoint)
        if before.get("value") != wanted:
            if not before.get("editable", False):
                raise RuntimeError(f"Required setting {setting} is not editable")
            cf_call(cf, "PATCH", endpoint, json={"value": wanted})
            receipt["actions"].append("set_" + setting)
            save()
        if cf_call(cf, "GET", endpoint).get("value") != wanted:
            raise RuntimeError(f"Setting {setting} readback differs")

    token = cf_call(cf, "GET", f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/token")
    if not isinstance(token, str) or len(token) < 32 or any(char.isspace() for char in token):
        raise RuntimeError("Unexpected tunnel token format")
    args.token_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if args.token_file.is_symlink():
        raise ValueError("Token destination must not be a symlink")
    atomic_write_text(args.token_file, token + "\n", durable=True, mode=0o600)
    receipt["token_file"] = str(args.token_file)
    latest = cf_call(cf, "GET", f"/zones/{zone_id}")
    receipt.update(stage="cloudflare_provisioned", zone_status=latest["status"],
                   delegation_matches=(set(owned.get("nameservers", []))
                                       == set(zone["name_servers"])))
    save()
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--token-file", type=Path,
                        default=Path("/etc/stockagent/cloudflared/tunnel-token"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        result = provision(args)
    except (ValueError, RuntimeError, requests.RequestException) as error:
        # Do not print response bodies, request headers, or arbitrary exception
        # representations from credential-bearing HTTP libraries.
        if isinstance(error, requests.RequestException):
            print("Provider transport failed; inspect authoritative state before retry", file=sys.stderr)
        else:
            print(str(error), file=sys.stderr)
        return 1
    print(json.dumps({key: result.get(key) for key in (
        "hostname", "stage", "zone_status", "nameservers", "delegation_matches", "actions",
    )}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
