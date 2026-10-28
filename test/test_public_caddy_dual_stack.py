from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_tunnel_caddy_uses_one_loopback_only_dual_stack_binding() -> None:
    config = (ROOT / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    assert "default_bind tcp4/127.0.0.1 tcp6/[::1]" in config
    assert config.count("default_bind ") == 1
    assert "\tbind " not in config
    assert "192.168.50.211" not in config
    assert "http://penguin72487.dpdns.org {" in config
    assert "http://www.penguin72487.dpdns.org {" in config
    assert "redir https://penguin72487.dpdns.org{uri} 308" in config
    assert "tcp4/0.0.0.0" not in config
    assert "tcp6/[::]" not in config


def test_tunnel_origin_does_not_expose_private_control_or_backend() -> None:
    config = (ROOT / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    assert "admin 127.0.0.1:2019" in config
    assert "reverse_proxy 127.0.0.1:8770" in config
    assert "@write_methods not method GET HEAD" in config
    assert "respond @write_methods 405" in config
    firewall = (ROOT / "scripts/configure_windows_public_caddy_firewall.ps1").read_text(encoding="utf-8")
    assert "LocalPort = @(80, 443)" in firewall
    assert "Program = $CaddyPath" in firewall


def test_single_upstream_route_failures_cannot_eject_all_healthy_html_routes() -> None:
    config = (ROOT / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    directives = {line.split("#", 1)[0].strip().split()[0]
                  for line in config.splitlines() if line.split("#", 1)[0].strip()}
    assert "reverse_proxy 127.0.0.1:8770 {" in config
    assert not directives.intersection({"health_uri", "health_port", "fail_duration", "max_fails",
                                        "unhealthy_status", "unhealthy_latency", "unhealthy_request_count"})
    # Removing ejection must not turn application failures into successes or
    # remove the bounded connect/header timeouts and independent recovery.
    assert "replace_status" not in directives
    assert "dial_timeout 2s" in config
    assert "response_header_timeout 20s" in config
    supervisor = (ROOT / "scripts/start_windows_public_caddy.ps1").read_text(encoding="utf-8")
    assert '$gatewayHealthUri = "http://127.0.0.1:8770/healthz"' in supervisor


def test_firewall_uses_listener_owner_and_does_not_remove_previous_rule() -> None:
    firewall = (ROOT / "scripts/configure_windows_public_caddy_firewall.ps1").read_text(encoding="utf-8")
    assert "Get-NetTCPConnection -State Listen" in firewall
    assert "Select-Object -ExpandProperty OwningProcess -Unique" in firewall
    assert "Get-Process -Id $_" in firewall
    assert "Resolve-CaddyFirewallPath $server.Path" in firewall
    assert '$parent.LinkType -eq "Junction"' in firewall
    assert "Set-NetFirewallRule -Name $rule.Name @ruleArguments" in firewall
    assert "Remove-NetFirewallRule" not in firewall
    assert firewall.index("$CaddyPath = Resolve-CaddyFirewallPath $CaddyPath") < firewall.index("Set-NetFirewallRule")
    assert "-PolicyStore ActiveStore" in firewall


def test_firewall_elevation_waits_for_and_checks_native_exit_status() -> None:
    request = (ROOT / "scripts/request_windows_public_caddy_firewall.ps1").read_text(encoding="utf-8")
    assert "-Verb RunAs" in request
    assert "-Wait -PassThru" in request
    assert "$process.ExitCode -ne 0" in request
    assert '"-ReceiptPath"' in request
