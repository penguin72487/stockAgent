from pathlib import Path

import pytest

from scripts.configure_public_cloudflare_tunnel import (
    ingress_for, read_key, require_compatible_dns, require_compatible_ingress,
    require_free_zone, require_owned_domain,
)

HOSTNAME = "penguin72487.dpdns.org"


def test_secret_input_requires_unique_private_field(tmp_path):
    env = tmp_path / "env"
    env.write_text("DPDNS_API_KEY=test-only\n")
    env.chmod(0o600)
    assert read_key(env, "DPDNS_API_KEY") == "test-only"
    env.write_text("DPDNS_API_KEY=test-only\nDPDNS_API_KEY=duplicate\n")
    with pytest.raises(ValueError, match="Exactly one"):
        read_key(env, "DPDNS_API_KEY")
    env.write_text("DPDNS_API_KEY=test-only\n")
    env.chmod(0o644)
    with pytest.raises(ValueError, match="group/other"):
        read_key(env, "DPDNS_API_KEY")


def test_secret_value_cannot_inject_an_http_header(tmp_path):
    env = tmp_path / "env"
    env.write_text('DPDNS_API_KEY="test-only\nInjected: not-allowed"\n')
    env.chmod(0o600)
    with pytest.raises(ValueError, match="format"):
        read_key(env, "DPDNS_API_KEY")


@pytest.mark.parametrize("hostname", ["other.dpdns.org", "x.penguin72487.dpdns.org", "-bad.dpdns.org"])
def test_only_uniquely_owned_direct_hostname_is_accepted(hostname):
    payload = {"success": True, "data": [{"domain": HOSTNAME, "status": "ok"}]}
    with pytest.raises(ValueError):
        require_owned_domain(hostname, payload)
    assert require_owned_domain(HOSTNAME, payload)["domain"] == HOSTNAME


def test_actual_cloudflare_free_plan_uses_legacy_id_not_opaque_plan_id():
    zone = {"name": HOSTNAME, "type": "full", "plan": {
        "id": "0feeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "legacy_id": "free", "price": 0,
    }}
    require_free_zone(zone, HOSTNAME)
    zone["plan"]["price"] = 20
    with pytest.raises(ValueError, match="Free zone"):
        require_free_zone(zone, HOSTNAME)


def test_ingress_has_one_loopback_gateway_and_a_denied_fallback():
    config = ingress_for(HOSTNAME)
    assert config["ingress"][0]["service"] == "http://127.0.0.1:8770"
    assert config["ingress"][-1] == {"service": "http_status:404"}
    require_compatible_ingress(config, HOSTNAME)
    with pytest.raises(ValueError, match="refusing"):
        require_compatible_ingress({"ingress": [{"service": "http://localhost:2019"}]}, HOSTNAME)


def test_cloudflare_disabled_warp_default_does_not_mask_private_routes():
    config = dict(ingress_for(HOSTNAME), **{"warp-routing": {"enabled": False}})
    require_compatible_ingress(config, HOSTNAME)
    config["warp-routing"]["enabled"] = True
    with pytest.raises(ValueError, match="refusing"):
        require_compatible_ingress(config, HOSTNAME)


@pytest.mark.parametrize("config", [
    {"warp-routing": {"enabled": True}},
    {"ingress": [], "warp-routing": {"enabled": True}},
    {"originRequest": {"noTLSVerify": True}},
])
def test_empty_ingress_does_not_authorize_replacing_an_unrelated_config(config):
    with pytest.raises(ValueError, match="refusing"):
        require_compatible_ingress(config, HOSTNAME)
    require_compatible_ingress(None, HOSTNAME)
    require_compatible_ingress({"ingress": []}, HOSTNAME)


@pytest.mark.parametrize("record", [
    {"type": "A", "content": "192.0.2.1"},
    {"type": "AAAA", "content": "2001:db8::1"},
    {"type": "CNAME", "content": "other.example"},
    {"type": "CNAME", "content": "test.cfargotunnel.com", "proxied": False},
])
def test_no_unrelated_address_record_is_replaced(record):
    with pytest.raises(ValueError, match="refusing"):
        require_compatible_dns([dict(record, name=HOSTNAME)], HOSTNAME, "test.cfargotunnel.com")


def test_matching_tunnel_record_is_idempotent_and_txt_is_preserved():
    require_compatible_dns([
        {"name": HOSTNAME, "type": "CNAME", "content": "test.cfargotunnel.com", "proxied": True},
        {"name": HOSTNAME, "type": "TXT", "content": "keep-verification"},
    ], HOSTNAME, "test.cfargotunnel.com")


def test_service_never_places_token_in_argv_or_exposes_metrics():
    text = (Path(__file__).resolve().parents[1] / "deploy/systemd/stockagent-public-cloudflared.service").read_text()
    assert "LoadCredential=tunnel-token:" in text
    assert "--token-file %d/tunnel-token" in text
    assert "--token " not in text
    assert "--metrics 127.0.0.1:20241" in text
    assert "DynamicUser=true" in text
    assert "Requires=" not in text
