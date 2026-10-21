from contextlib import contextmanager
from http.client import HTTPConnection
from pathlib import Path
import json
import threading

import pytest

from stockagent.live.dashboard_access import access_description, permitted_path, project_html
from stockagent.live.public_dashboards import sanitize_member_dashboard_payload, sanitize_tw_positions, sanitize_tw_status
from scripts import serve_public_dashboards as gateway


@contextmanager
def listener(audience):
    root = Path(__file__).resolve().parents[1]
    server = gateway.PublicDashboardServer(
        ("127.0.0.1", 0), audience=audience,
        public_static_root=root / "services/public_dashboards",
        taifex_static_root=root / "services/taifex_dashboard",
        tw_static_root=root / "services/tw_day_trade_dashboard",
        shioaji_static_root=root / "services/shioaji_api_dashboard",
        openbb_static_root=root / "services/openbb_archive_dashboard",
        data_monitor_static_root=root / "services/data_monitor_dashboard",
        traffic_static_root=root / "services/traffic_dashboard", repo_root=root,
        taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    try:
        yield server, conn
    finally:
        conn.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize("path", ["/tej/", "/finlab/api/status", "/traffic/api/history", "/data-monitor/api/features",
                                  "/news/app.js", "/taifex/", "/shioaji", "/openbb/index.html"])
def test_public_denies_private_pages_assets_and_apis_even_with_forged_role(path):
    with listener("public") as (_, conn):
        conn.request("GET", path, headers={"X-WireGuard-Role": "owner", "X-WireGuard-Identity": "trusted-lan",
                                          "X-StockAgent-Audience": "owner", "X-Forwarded-For": "192.168.50.100"})
        r = conn.getresponse()
        assert r.status == 403
        assert json.loads(r.read()) == {"error": "access_denied"}
        assert r.getheader("Cache-Control") == "no-store"


def test_html_projection_removes_private_cards_before_delivery_and_does_not_mutate_owner():
    html = (Path(__file__).resolve().parents[1] / "services/public_dashboards/index.html").read_bytes()
    public = project_html(html, owner=False)
    owner = project_html(html, owner=True)
    assert b'content="public"' in public and b'content="owner"' in owner
    for slug in [b'tej/', b'finlab/', b'traffic/', b'data-monitor/', b'news/']:
        assert b'href="' + slug + b'"' not in public
        assert b'href="' + slug + b'"' in owner
    assert public.count(b'class="dashboard-card ') == 2
    assert owner.count(b'class="dashboard-card ') == 11
    assert b'dashboard-core.js?v=access-v2' in public


def test_member_sees_all_page_shells_without_claiming_owner_prices():
    html = (Path(__file__).resolve().parents[1] / "services/public_dashboards/index.html").read_bytes()
    member = project_html(html, audience="member")
    assert b'content="member"' in member
    assert member.count(b'class="dashboard-card ') == 11
    assert access_description(audience="member") == {
        "schema_version": 1, "audience": "member", "pages": "all", "private_prices": False}
    for path in ["/news/", "/tej/", "/finlab/api/status", "/traffic/app.js"]:
        assert permitted_path(path, audience="member")


@pytest.mark.parametrize("path", ["/news/", "/tej/", "/finlab/", "/finmind/", "/traffic/",
                                  "/data-monitor/", "/shioaji/", "/taifex/", "/openbb/"])
def test_member_can_read_all_registered_page_shells(path):
    with listener("member") as (_, conn):
        conn.request("GET", path)
        response = conn.getresponse()
        assert response.status == 200
        assert response.getheader("X-StockAgent-Audience") == "member"
        assert response.getheader("Cache-Control") == "private, no-store"
        assert b'content="member"' in response.read()


def test_member_provider_payload_keeps_metadata_but_drops_raw_quotes_and_secrets():
    raw = {"summary": {"fields": 42}, "quote": {"symbol": "2330", "last_mark_price": 123.456789},
           "api_key": "do-not-publish", "private_prices": True}
    result = sanitize_member_dashboard_payload(raw)
    assert result["summary"] == {"fields": 42}
    assert result["quote"]["last_mark_price"] is None
    assert "do-not-publish" not in json.dumps(result)
    assert raw["quote"]["last_mark_price"] == 123.456789


def test_private_quotes_require_server_argument_and_keep_credential_redaction_and_source_precision():
    raw = {"simulation_only": True, "production_order_possible": False, "private_prices": True,
           "rows": [{"symbol": "2330", "market": "paper", "entry_price": 123.456789,
                     "last_mark_price": 124.125, "take_profit_price": 126.333333,
                     "api_key": "do-not-publish"}]}
    public = sanitize_tw_positions(raw)
    owner = sanitize_tw_positions(raw, private_prices=True)
    assert public['rows'][0]['entry_price'] is None
    assert public['rows'][0]['last_mark_price'] is None
    assert owner['rows'][0]['entry_price'] == 123.456789
    assert owner['rows'][0]['last_mark_price'] == 124.125
    assert owner['rows'][0]['take_profit_price'] == 126.333333
    assert 'do-not-publish' not in json.dumps(owner)
    assert raw['rows'][0]['entry_price'] == 123.456789
    status = sanitize_tw_status({"simulation_only": True, "production_order_possible": False}, private_prices=True)
    assert status['price_display_policy']['private_view'] is True
    with pytest.raises(Exception):
        sanitize_tw_positions({**raw, "production_order_possible": True}, private_prices=True)


def test_both_http_audiences_separate_quotes_cache_validators_and_ignore_identity_headers(monkeypatch):
    raw = {"simulation_only": True, "production_order_possible": False,
           "rows": [{"symbol": "2330", "market": "paper", "last_mark_price": 123.456789}],
           "offset": 0, "limit": 1, "returned": 1, "total": 1, "has_more": False}
    monkeypatch.setattr(gateway, 'build_dashboard_position_page', lambda **kwargs: raw)
    with listener('public') as (pub, pc), listener('owner') as (own, oc):
        pub.content_token = own.content_token = lambda *args, **kwargs: 'fixed-revision'
        path = '/tw-day-trade/api/positions?limit=1'
        pc.request('GET', path, headers={'X-WireGuard-Role': 'owner'})
        pr = pc.getresponse(); pbody = json.loads(pr.read()); petag = pr.getheader('ETag')
        assert pr.status == 200 and pbody['rows'][0]['last_mark_price'] is None
        oc.request('GET', path, headers={'If-None-Match': petag})
        r = oc.getresponse(); body = json.loads(r.read()); oetag = r.getheader('ETag')
        assert r.status == 200 and body['rows'][0]['last_mark_price'] == 123.456789
        assert r.getheader('Cache-Control') == 'private, no-store'
        assert oetag != petag
        pc.request('GET', path, headers={'If-None-Match': oetag})
        r = pc.getresponse()
        assert r.status == 200 and json.loads(r.read())['rows'][0]['last_mark_price'] is None
        oc.request('GET', path, headers={'If-None-Match': oetag})
        r = oc.getresponse(); r.read()
        assert r.status == 304 and r.getheader('Cache-Control') == 'private, no-store'
        assert pub._cache is not own._cache


def test_member_quotes_match_public_policy_and_cannot_validate_owner_cache(monkeypatch):
    raw = {"simulation_only": True, "production_order_possible": False,
           "rows": [{"symbol": "2330", "market": "paper", "last_mark_price": 123.456789}],
           "offset": 0, "limit": 1, "returned": 1, "total": 1, "has_more": False}
    monkeypatch.setattr(gateway, "build_dashboard_position_page", lambda **kwargs: raw)
    with listener("owner") as (owner, oc), listener("member") as (member, mc), listener("public") as (public, pc):
        for server in (owner, member, public):
            server.content_token = lambda *args, **kwargs: "fixed-revision"
        path = "/tw-day-trade/api/positions?limit=1"
        bodies, etags = {}, {}
        for audience, conn in [("owner", oc), ("member", mc), ("public", pc)]:
            conn.request("GET", path, headers={"X-WireGuard-Role": "owner", "X-StockAgent-Audience": "owner"})
            response = conn.getresponse()
            assert response.status == 200
            bodies[audience] = json.loads(response.read())
            etags[audience] = response.getheader("ETag")
        assert bodies["owner"]["rows"][0]["last_mark_price"] == 123.456789
        assert bodies["member"] == bodies["public"]
        assert bodies["member"]["rows"][0]["last_mark_price"] is None
        assert len(set(etags.values())) == 3
        mc.request("GET", path, headers={"If-None-Match": etags["owner"]})
        response = mc.getresponse()
        assert response.status == 200
        assert response.getheader("Cache-Control") == "private, no-store"
        assert json.loads(response.read())["rows"][0]["last_mark_price"] is None
        mc.request("GET", "/api/access", headers={"X-WireGuard-Role": "owner"})
        response = mc.getresponse()
        assert json.loads(response.read())["private_prices"] is False
        assert member._cache is not owner._cache and member._cache is not public._cache


def test_public_overview_never_reads_private_provider_sources(monkeypatch):
    monkeypatch.setattr(gateway, 'build_compact_tw_overview_status',
                        lambda *args, **kwargs: {'health': 'active', 'modes': [], 'source_age_seconds': 1})
    def forbidden(*args, **kwargs):
        raise AssertionError('private source was read')
    monkeypatch.setattr(gateway, 'build_finmind_public_status', forbidden)
    monkeypatch.setattr(gateway, 'build_tej_public_status', forbidden)
    with listener('public') as (server, conn):
        server.shioaji_status = server.openbb_status = server.data_monitor_status = forbidden
        data = server.public_overview()
        assert set(data) == {'schema_version', 'generated_at_utc', 'access', 'tw', 'overnight', 'unavailable_sources'}
        assert data['unavailable_sources'] == []
        conn.request('GET', '/api/access', headers={'X-WireGuard-Role': 'owner'})
        r = conn.getresponse(); access = json.loads(r.read())
        assert access['audience'] == 'public' and access['private_prices'] is False


@pytest.mark.parametrize("audience", ["owner", "member"])
def test_owner_listener_refuses_public_bind_before_opening_socket(audience):
    with pytest.raises(ValueError, match='loopback-only'):
        gateway.PublicDashboardServer(('0.0.0.0', 0), audience=audience,
            public_static_root=Path('.'), taifex_static_root=Path('.'), tw_static_root=Path('.'),
            shioaji_static_root=Path('.'), openbb_static_root=Path('.'),
            data_monitor_static_root=Path('.'), traffic_static_root=Path('.'), repo_root=Path('.'),
            taifex_upstream='http://127.0.0.1:1', tw_upstream='http://127.0.0.1:1')


def test_policy_defaults_do_not_allow_unrelated_paths():
    assert permitted_path('/tw-day-trade/api/status', owner=False)
    assert not permitted_path('/tw-day-trade-other/api/status', owner=False)
    assert not permitted_path('//tej/api/status', owner=False)
