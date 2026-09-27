"""No external requests, real credentials, or production limiter state in tests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import fcntl
import io
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request

import pytest

from downloader import download_keyed_public_catalogs as mod
from downloader.common import SharedRateLimiter


SECRET = "private-test-key/plus+space x"
CREDS = {spec.credential_name: SECRET for spec in mod.SPECS if spec.credential_name}


def payload(provider: str) -> bytes:
    data = {
        "noaa_cdo": {"metadata": {"resultset": {"count": 1}}, "results": [
            {"id": "GHCND", "mindate": "1763-01-01", "maxdate": "2026-09-25"}]},
        "bea": {"BEAAPI": {"Request": {"RequestParam": [{"ParameterName": "USERID", "ParameterValue": SECRET}]},
                           "Results": {"Dataset": [{"DatasetName": "NIPA", "DatasetDescription": "National accounts"}]}}},
        "census": {"dataset": [{"c_dataset": ["acs", "acs5"], "c_vintage": 2024}]},
        "finnhub": [{"symbol": "AAPL", "description": "Apple Inc"}],
        "cwa": {"success": "true", "records": {"Station": [
            {"StationId": "466920", "ObsTime": {"DateTime": "2026-09-27T18:00:00+08:00"},
             "WeatherElement": {"AirTemperature": 26.1}}]}},
        "moenv": [{"siteid": "1", "publishtime": "2026/09/27 18:00:00", "aqi": "40"}],
    }
    if provider == "nasa_firms":
        return b"data_id,min_date,max_date\nMODIS_SP,2000-11-01,2026-09-24\n"
    return json.dumps(data[provider]).encode()


class Limiter:
    def __init__(self):
        self.waits = 0
        self.deferrals = []

    def wait(self, **kwargs):
        self.waits += 1

    def defer(self, seconds):
        self.deferrals.append(seconds)


class Response(io.BytesIO):
    status = 200

    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers or {}


def factory_for(calls, *, failure=None):
    class Transport:
        def __init__(self, spec, budget, **kwargs):
            self.spec, self.budget = spec, budget
            self.limiter = Limiter()
            self.retry_after_seconds = 120

        def fetch(self, url, headers):
            self.budget.claim(self.spec.provider)
            calls.append(self.spec.provider)
            if failure:
                raise failure
            return payload(self.spec.provider)
    return Transport


@pytest.mark.parametrize("provider", [spec.provider for spec in mod.SPECS if spec.implementation == "implemented"])
def test_all_adapter_shapes_have_real_event_dates_only(provider):
    encoded, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER[provider], payload(provider), (SECRET,))
    assert SECRET not in encoded.decode()
    assert metadata["rows"] == 1
    assert metadata["history_complete"] is False
    if provider in {"cwa", "moenv"}:
        assert metadata["source_event_start"] == metadata["source_event_end"]
        assert "2026" in metadata["source_event_end"]
        assert metadata["catalog_complete"] is False
    else:
        assert metadata["source_event_start"] is None
        assert metadata["source_event_end"] is None
        assert metadata["catalog_complete"] is True
    assert metadata["payload_sanitized"] is (provider == "bea")


def test_noaa_partial_page_is_not_complete():
    value = json.loads(payload("noaa_cdo"))
    value["metadata"]["resultset"]["count"] = 1001
    _, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER["noaa_cdo"], json.dumps(value).encode(), ())
    assert metadata["catalog_complete"] is False
    assert metadata["advertised_history_start"] == "1763-01-01"
    assert metadata["source_event_start"] is None


def test_moenv_old_envelope_remains_readable_and_preserves_publication_clock():
    encoded, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER["moenv"],
                                         json.dumps({"records": json.loads(payload("moenv"))}).encode(), ())
    assert metadata["rows"] == 1
    assert metadata["source_event_time_kind"] == "publication"
    assert metadata["source_event_end"] == "2026/09/27 18:00:00"


def test_cwa_observation_time_is_not_relabelled_as_publication():
    _, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER["cwa"], payload("cwa"), ())
    assert metadata["source_event_time_kind"] == "observation"


@pytest.mark.parametrize("body", [b"{}", b"[]", b"<html>error</html>", b'{"error":"quota"}',
                                     b'{"dataset":[{"error":"bad key"}]}'])
def test_catalog_error_envelopes_do_not_turn_into_data(body):
    with pytest.raises(mod.CatalogError):
        mod.parse_payload(mod.SPEC_BY_PROVIDER["census"], body, ())


def test_recursive_decoded_and_encoded_success_echoes_are_redacted():
    value = {"dataset": [{"c_dataset": ["test"], SECRET: {
        "key": SECRET, "url": "https://example.invalid/" + quote(SECRET, safe=""),
        "nested": [SECRET]}}]}
    encoded, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER["census"], json.dumps(value, ensure_ascii=True).encode(), (SECRET,))
    assert metadata["payload_sanitized"] is True
    assert "[REDACTED]" in encoded.decode()
    for variant in mod._secret_variants((SECRET,)):
        assert variant.encode() not in encoded


def test_transport_key_path_never_appears_in_error_or_retry_body():
    spec = mod.SPEC_BY_PROVIDER["nasa_firms"]
    url, headers = mod.request_for(spec, SECRET)
    limiter = Limiter()
    budget = mod.RequestBudget(5)

    def bad(request, timeout):
        raise HTTPError(request.full_url, 403, "secret=" + SECRET,
                        {"X-Echo": SECRET}, io.BytesIO(("echo " + SECRET).encode()))

    client = mod.SafeCatalogTransport(spec, budget, maximum_bytes=4096,
                                      secrets=(SECRET,), opener=bad, limiter=limiter)
    with pytest.raises(mod.CatalogError, match="^http_403$") as exc:
        client.fetch(url, headers)
    assert SECRET not in str(exc.value)
    assert quote(SECRET, safe="") not in str(exc.value)
    assert budget.used == 1


def test_429_defers_exact_provider_retry_after_without_in_run_retry():
    spec = mod.SPEC_BY_PROVIDER["bea"]
    limiter = Limiter()
    budget = mod.RequestBudget(3)

    def bad(request, timeout):
        raise HTTPError(request.full_url, 429, "rate limited", {"Retry-After": "120"}, io.BytesIO(b"slow down"))

    client = mod.SafeCatalogTransport(spec, budget, maximum_bytes=1024, secrets=(SECRET,), opener=bad, limiter=limiter)
    with pytest.raises(mod.CatalogError, match="^http_429$"):
        client.fetch(*mod.request_for(spec, SECRET))
    assert budget.used == 1
    assert limiter.deferrals == [120]


def test_transport_retries_are_part_of_request_budget():
    spec = mod.SPEC_BY_PROVIDER["census"]
    limiter = Limiter()
    budget = mod.RequestBudget(1)

    def bad(request, timeout):
        raise HTTPError(request.full_url, 503, "unavailable", {}, io.BytesIO(b""))

    client = mod.SafeCatalogTransport(spec, budget, maximum_bytes=1024, secrets=(), opener=bad, limiter=limiter)
    with pytest.raises(mod.CatalogError, match="^request_budget_exhausted$"):
        client.fetch(*mod.request_for(spec, ""))
    assert budget.used == 1
    assert len(limiter.deferrals) == 1


@pytest.mark.parametrize("header", [{}, {"Content-Length": "5000"}, {"Content-Length": "invalid"}])
def test_success_read_is_byte_bounded_and_closed(header):
    spec = mod.SPEC_BY_PROVIDER["census"]
    response = Response(b"x" * 100, header)
    client = mod.SafeCatalogTransport(spec, mod.RequestBudget(1), maximum_bytes=50, secrets=(),
                                      opener=lambda *args, **kwargs: response, limiter=Limiter())
    with pytest.raises(mod.CatalogError, match="response_byte_limit|invalid_content_length"):
        client.fetch(*mod.request_for(spec, ""))
    assert response.closed


def test_failure_response_is_byte_bounded():
    spec = mod.SPEC_BY_PROVIDER["census"]

    def bad(request, timeout):
        raise HTTPError(request.full_url, 403, "bad", {}, io.BytesIO(b"x" * 100))

    client = mod.SafeCatalogTransport(spec, mod.RequestBudget(1), maximum_bytes=50, secrets=(), opener=bad, limiter=Limiter())
    with pytest.raises(mod.CatalogError, match="^response_byte_limit$"):
        client.fetch(*mod.request_for(spec, ""))


@pytest.mark.parametrize("url", ["https://evil.invalid/x", "http://api.census.gov/data.json", "https://user:secret@api.census.gov/data.json"])
def test_origin_guard_rejects_before_opener(url):
    def never(*args, **kwargs):
        pytest.fail("network opener must not run")
    client = mod.SafeCatalogTransport(mod.SPEC_BY_PROVIDER["census"], mod.RequestBudget(1),
                                      maximum_bytes=1024, secrets=(), opener=never, limiter=Limiter())
    with pytest.raises(mod.CatalogError, match="request_origin_refused"):
        client.fetch(url, {})


def test_redirect_handler_rejects_even_same_origin():
    request = Request("https://example.invalid/path")
    with pytest.raises(mod.CatalogError, match="redirect_refused"):
        mod._NoRedirect().redirect_request(request, None, 302, "redirect", {}, "https://example.invalid/other")


def test_execute_persists_immutable_sanitized_bytes_and_cache_integrity(tmp_path):
    calls = []
    spec = mod.SPEC_BY_PROVIDER["bea"]
    kwargs = dict(minimum_free_bytes=0, maximum_bytes=4096, transport_factory=factory_for(calls))
    first = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    row = first["providers"][0]
    assert first["state"] == "bounded_scope_complete"
    assert first["history_complete"] is False
    assert row["observed_at_utc"] >= row["checked_at_utc"]
    second = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    assert second["providers"][0]["status"] == "current_cached"
    assert second["providers"][0]["requests_this_run"] == 0
    assert calls == ["bea"]
    assert len(list((tmp_path / "receipt_history" / "bea").glob("*.json"))) == 1
    assert json.loads((tmp_path / "receipts" / "bea.json").read_text())["requests_this_run"] == 1
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert SECRET.encode() not in path.read_bytes()
            assert quote(SECRET, safe="").encode() not in path.read_bytes()
            assert SECRET not in str(path)
    # Corruption is neither accepted as cache nor overwritten silently.
    (tmp_path / row["object_path"]).write_bytes(b"corrupted")
    third = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    assert third["providers"][0]["status"] == "existing_object_hash_mismatch"
    assert (tmp_path / row["object_path"]).read_bytes() == b"corrupted"


def test_force_retains_versions_and_only_deduplicates_identical_payload(tmp_path):
    calls = []
    spec = mod.SPEC_BY_PROVIDER["census"]
    for _ in range(2):
        mod.execute(tmp_path, [spec], {}, minimum_free_bytes=0, maximum_bytes=4096,
                    force=True, transport_factory=factory_for(calls))
    assert len(list((tmp_path / "objects" / "census").glob("*.json"))) == 1
    assert len(list((tmp_path / "receipt_history" / "census").glob("*.json"))) == 2


def test_budget_and_unknown_scope_never_silently_expand(tmp_path):
    calls = []
    specs = [mod.SPEC_BY_PROVIDER[name] for name in ["census", "bea", "airnow", "api_data_gov"]]
    result = mod.execute(tmp_path, specs, CREDS, max_requests=1, minimum_free_bytes=0,
                         maximum_bytes=4096, transport_factory=factory_for(calls))
    assert [row["status"] for row in result["providers"]] == [
        "acquired", "request_budget_exhausted", "needs_region_date", "credential_hub"]
    assert result["requests"] == 1
    assert result["state"] == "partial"
    assert calls == ["census"]


def test_planned_non_work_is_not_a_failed_download_or_historical_complete_claim(tmp_path):
    specs = [mod.SPEC_BY_PROVIDER[name] for name in ["census", "airnow", "api_data_gov"]]
    result = mod.execute(tmp_path, specs, CREDS, minimum_free_bytes=0, maximum_bytes=4096,
                         transport_factory=factory_for([]))
    assert result["state"] == "bounded_scope_complete"
    assert result["implemented_adapters"] == result["implemented_complete"] == 1
    assert result["planned_non_work"] == [
        {"provider": "airnow", "status": "needs_region_date"},
        {"provider": "api_data_gov", "status": "credential_hub"}]
    assert result["history_complete"] is False
    assert result["historical_coverage_state"] == "not_requested"


@pytest.mark.parametrize("target", [
    "https://static2.finnhub.io/file/privatedatany2/exchange/USf.json",
    "https://static2.finnhub.io/file/privatedatany2/exchange/USf.json?token=private-test-key",
])
def test_finnhub_authenticated_cdn_redirect_never_forwards_headers_or_query(target):
    request = Request(mod.SPEC_BY_PROVIDER["finnhub"].endpoint, headers={"X-Finnhub-Token": SECRET})
    with pytest.raises(mod.CatalogError, match="redirect_refused"):
        mod._NoRedirect().redirect_request(request, None, 302, "redirect", {}, target)


def finnhub_delivery_client(location, *, maximum_requests=2, second_redirect=None, body=None):
    calls = []
    budget = mod.RequestBudget(maximum_requests)

    def opener(request, timeout):
        calls.append(request)
        if len(calls) == 1:
            raise HTTPError(request.full_url, 302, "delivery", {"Location": location}, io.BytesIO(b""))
        if second_redirect is not None:
            raise HTTPError(request.full_url, 302, "delivery", {"Location": second_redirect}, io.BytesIO(b""))
        return Response(body or payload("finnhub"))

    client = mod.SafeCatalogTransport(mod.SPEC_BY_PROVIDER["finnhub"], budget, maximum_bytes=4096,
                                      secrets=(SECRET,), opener=opener, limiter=Limiter())
    return client, budget, calls


def test_reviewed_finnhub_single_hop_uses_separate_signed_token_no_original_headers():
    token = "opaque-signed-delivery-token"
    client, budget, calls = finnhub_delivery_client(mod.FINNHUB_CDN_CATALOG + "?Authorization=" + token,
                                                   body=json.dumps([{"symbol": "AAPL", "echo": token}]).encode())
    url, headers = mod.request_for(mod.SPEC_BY_PROVIDER["finnhub"], SECRET)
    headers.update({"Cookie": SECRET, "Authorization": SECRET})
    body = client.fetch(url, headers)
    assert budget.used == 2
    assert len(calls) == 2
    assert calls[0].get_header("X-finnhub-token") == SECRET
    assert set(name.lower() for name, value in calls[1].header_items()) == {"user-agent", "accept"}
    assert SECRET not in calls[1].full_url
    assert token in client.secrets
    assert client._approved_location is None
    encoded, metadata = mod.parse_payload(mod.SPEC_BY_PROVIDER["finnhub"], body, client.secrets)
    assert token.encode() not in encoded
    assert metadata["payload_sanitized"] is True


@pytest.mark.parametrize("location", [
    "http://static2.finnhub.io/file/privatedatany2/exchange/USf.json?Authorization=signed",
    "https://evil.invalid/file/privatedatany2/exchange/USf.json?Authorization=signed",
    "https://static2.finnhub.io/other.json?Authorization=signed",
    "https://static2.finnhub.io:443/file/privatedatany2/exchange/USf.json?Authorization=signed",
    "https://user@static2.finnhub.io/file/privatedatany2/exchange/USf.json?Authorization=signed",
    mod.FINNHUB_CDN_CATALOG + "?Authorization=signed&extra=1",
    mod.FINNHUB_CDN_CATALOG + "?Authorization=signed&Authorization=second",
    mod.FINNHUB_CDN_CATALOG + "?authorization=signed",
    mod.FINNHUB_CDN_CATALOG + "?Authorization=",
    mod.FINNHUB_CDN_CATALOG + "?Authorization=signed#fragment",
    mod.FINNHUB_CDN_CATALOG + "?Authorization=" + quote(SECRET, safe=""),
])
def test_unreviewed_or_original_key_redirect_never_reaches_second_request(location):
    client, budget, calls = finnhub_delivery_client(location)
    with pytest.raises(mod.CatalogError, match="^redirect_refused$"):
        client.fetch(*mod.request_for(mod.SPEC_BY_PROVIDER["finnhub"], SECRET))
    assert len(calls) == budget.used == 1


def test_finnhub_second_redirect_is_not_followed():
    target = mod.FINNHUB_CDN_CATALOG + "?Authorization=signed-delivery"
    client, budget, calls = finnhub_delivery_client(target, second_redirect=target)
    with pytest.raises(mod.CatalogError, match="^redirect_refused$"):
        client.fetch(*mod.request_for(mod.SPEC_BY_PROVIDER["finnhub"], SECRET))
    assert len(calls) == budget.used == 2


def test_finnhub_redirect_cannot_exceed_global_request_budget():
    client, budget, calls = finnhub_delivery_client(mod.FINNHUB_CDN_CATALOG + "?Authorization=signed-delivery",
                                                   maximum_requests=1)
    with pytest.raises(mod.CatalogError, match="^request_budget_exhausted$"):
        client.fetch(*mod.request_for(mod.SPEC_BY_PROVIDER["finnhub"], SECRET))
    assert len(calls) == budget.used == 1


def test_summary_does_not_add_different_grains_as_total_observations(tmp_path):
    specs = [mod.SPEC_BY_PROVIDER[name] for name in ["census", "cwa", "moenv"]]
    result = mod.execute(tmp_path, specs, CREDS, minimum_free_bytes=0, maximum_bytes=4096,
                         transport_factory=factory_for([]))
    assert result["rows"] is None
    assert result["catalog_entries"] == 1
    assert result["snapshot_observation_rows"] == 2
    assert result["provider_row_counts"] == {"census": 1, "cwa": 1, "moenv": 1}


def test_persistent_cooldown_survives_new_instance_and_force(tmp_path):
    calls = []
    spec = mod.SPEC_BY_PROVIDER["bea"]
    kwargs = dict(minimum_free_bytes=0, maximum_bytes=4096,
                  transport_factory=factory_for(calls, failure=mod.CatalogError("http_429")))
    first = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    second = mod.execute(tmp_path, [spec], CREDS, force=True, **kwargs)
    assert first["providers"][0]["status"] == "http_429"
    assert second["providers"][0]["status"] == "provider_cooldown"
    assert second["requests"] == 0
    assert calls == ["bea"]


def test_shared_limiter_cooldown_is_host_persistent(tmp_path):
    first = SharedRateLimiter(1, name="bea_public", state_dir=tmp_path)
    first.defer(60)
    second = SharedRateLimiter(1, name="bea_public", state_dir=tmp_path)
    granted, wait = second._claim_process_shared()
    assert not granted and wait > 55


def test_bea_application_throttle_preserves_retry_and_continues_other_provider(tmp_path):
    calls = []
    base = factory_for(calls)

    class Transport(base):
        def fetch(self, url, headers):
            if self.spec.provider != "bea":
                return super().fetch(url, headers)
            self.budget.claim("bea")
            calls.append("bea")
            return json.dumps({"BEAAPI": {"Results": {"Error": {
                "APIErrorCode": "7", "APIErrorDescription": SECRET}}}}).encode()

    result = mod.execute(tmp_path, [mod.SPEC_BY_PROVIDER[name] for name in ["bea", "census"]], CREDS,
                         minimum_free_bytes=0, maximum_bytes=4096, transport_factory=Transport)
    assert [row["status"] for row in result["providers"]] == ["provider_throttled", "acquired"]
    assert "retry_at_utc" in result["providers"][0]
    assert SECRET not in json.dumps(result)
    assert calls == ["bea", "census"]


@pytest.mark.parametrize("code,reason", [("1", "credential_rejected"), ("4", "credential_activation_required")])
def test_bea_known_credential_error_returns_fixed_actionable_reason(code, reason):
    body = json.dumps({"BEAAPI": {"Results": {"Error": {
        "APIErrorCode": code, "APIErrorDescription": SECRET}}}}).encode()
    with pytest.raises(mod.CatalogError, match=f"^{reason}$"):
        mod.parse_payload(mod.SPEC_BY_PROVIDER["bea"], body, (SECRET,))


def test_refresh_failure_does_not_discard_last_good_receipt(tmp_path):
    spec = mod.SPEC_BY_PROVIDER["bea"]
    mod.execute(tmp_path, [spec], CREDS, minimum_free_bytes=0, maximum_bytes=4096,
                transport_factory=factory_for([]))
    receipt = (tmp_path / "receipts" / "bea.json").read_bytes()
    result = mod.execute(tmp_path, [spec], CREDS, minimum_free_bytes=0, maximum_bytes=4096,
                         force=True, transport_factory=factory_for([], failure=mod.CatalogError("http_403")))
    assert result["providers"][0]["status"] == "http_403"
    assert (tmp_path / "receipts" / "bea.json").read_bytes() == receipt


def test_disk_reserve_rechecked_after_download(tmp_path, monkeypatch):
    calls = []
    free = iter([100000, 0])
    monkeypatch.setattr(mod.shutil, "disk_usage", lambda _: SimpleNamespace(free=next(free)))
    result = mod.execute(tmp_path, [mod.SPEC_BY_PROVIDER["census"]], {}, minimum_free_bytes=10,
                         maximum_bytes=4096, transport_factory=factory_for(calls))
    assert result["providers"][0]["status"] == "disk_reserve_reached"
    assert not (tmp_path / "objects").exists()


def test_lock_prevents_second_writer(tmp_path):
    with (tmp_path / ".download.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            mod.execute(tmp_path, [], {}, max_requests=0)


def test_default_cli_is_zero_request_plan_without_workspace_writes(tmp_path, monkeypatch, capsys):
    def never(*args, **kwargs):
        pytest.fail("default plan may not call execute")
    monkeypatch.setattr(mod, "execute", never)
    monkeypatch.setenv("BEA_API_KEY", SECRET)
    root = tmp_path / "not-created"
    assert mod.main(["--env-file", str(tmp_path / "absent.env"), "--output-root", str(root), "--providers", "bea", "census"]) == 0
    output = capsys.readouterr().out
    assert SECRET not in output
    result = json.loads(output)
    assert result["network_requests"] == 0
    assert result["global_history_enabled"] is False
    assert result["history_size_estimate_bytes"] is None
    assert [row["credential_state"] for row in result["providers"]] == ["configured", "not_required"]
    assert not root.exists()


def test_hourly_snapshot_becomes_due_at_release_boundary_not_capture_plus_hour(tmp_path, monkeypatch):
    current = [datetime(2026, 9, 27, 10, 59, tzinfo=UTC)]
    monkeypatch.setattr(mod, "_now", lambda: current[0])
    spec = mod.SPEC_BY_PROVIDER["cwa"]
    calls = []
    kwargs = dict(minimum_free_bytes=0, maximum_bytes=4096, transport_factory=factory_for(calls))
    first = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    assert first["providers"][0]["next_check_at_utc"] == "2026-09-27T11:00:00+00:00"
    current[0] = datetime(2026, 9, 27, 11, 0, tzinfo=UTC)
    second = mod.execute(tmp_path, [spec], CREDS, **kwargs)
    assert second["providers"][0]["status"] == "acquired"
    assert calls == ["cwa", "cwa"]


def test_error_reason_codes_cannot_echo_unknown_text():
    assert str(mod.CatalogError(SECRET)) == "local_processing_failed"


@pytest.mark.parametrize("mutate", [
    lambda receipt: receipt.update(observed_at_utc=(datetime.now(UTC) + timedelta(days=1)).isoformat()),
    lambda receipt: receipt.update(observed_at_utc=(datetime.now(UTC) - timedelta(days=2)).isoformat()),
    lambda receipt: receipt.update(object_path="../../outside.json"),
    lambda receipt: receipt.update(object_sha256="0" * 64),
])
def test_cache_rejects_future_stale_unsafe_or_hash_mismatch(tmp_path, mutate):
    spec = mod.SPEC_BY_PROVIDER["census"]
    mod.execute(tmp_path, [spec], {}, minimum_free_bytes=0, maximum_bytes=4096, transport_factory=factory_for([]))
    receipt_path = tmp_path / "receipts" / "census.json"
    receipt = json.loads(receipt_path.read_text())
    mutate(receipt)
    receipt_path.write_text(json.dumps(receipt))
    assert mod._cached(tmp_path, spec, now=datetime.now(UTC), maximum_bytes=4096) is None
