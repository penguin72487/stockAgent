from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_public_caddy_uses_one_address_independent_dual_stack_binding() -> None:
    config = (ROOT / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    assert "default_bind tcp4/0.0.0.0 tcp6/[::]" in config
    assert config.count("default_bind ") == 1
    assert "\tbind " not in config
    assert "192.168.50.211" not in config
    assert "http://penguin72487.ddnsgeek.com {" in config
    assert "redir https://penguin72487.ddnsgeek.com{uri} permanent" in config


def test_wildcard_public_sockets_do_not_expose_private_control_or_backend() -> None:
    config = (ROOT / "deploy/caddy/Caddyfile.windows").read_text(encoding="utf-8")
    assert "admin 127.0.0.1:2019" in config
    assert "reverse_proxy 127.0.0.1:8770" in config
    assert "@write_methods not method GET HEAD" in config
    assert "respond @write_methods 405" in config
    firewall = (ROOT / "scripts/configure_windows_public_caddy_firewall.ps1").read_text(encoding="utf-8")
    assert "LocalPort = @(80, 443)" in firewall
    assert "Program = $CaddyPath" in firewall


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
