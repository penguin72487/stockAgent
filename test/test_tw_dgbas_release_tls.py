"""Correct official-host chains and access denial, never insecure TLS fallback."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader import download_tw_dgbas_release_archive as archive


class Response:
    def __init__(self, status=200, body=b"original", location=None):
        self.status_code = status
        self.content = body
        self.headers = {"Location": location} if location else {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def raise_for_status(self):
        assert self.status_code < 400

    def iter_content(self, **_kwargs):
        yield self.content


def test_pinned_ca_is_fetched_once_with_verified_https_and_original_trust(tmp_path, monkeypatch):
    der = b"reviewed intermediate DER"
    pin = hashlib.sha256(der).hexdigest()
    trust = tmp_path / "original-trust.pem"
    trust.write_bytes(b"ORIGINAL ROOTS")
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(body=der)

    monkeypatch.setattr(archive, "_CA_BUNDLES", {})
    monkeypatch.setattr(archive, "_CA_TEMPS", {})
    monkeypatch.setattr(archive.requests, "get", get)
    monkeypatch.setattr(archive.requests.certs, "where", lambda: str(trust))
    monkeypatch.setattr(archive.ssl, "DER_cert_to_PEM_cert", lambda value: "PINNED PEM" if value == der else "wrong")
    with ThreadPoolExecutor(max_workers=8) as pool:
        paths = list(pool.map(lambda _: archive._pinned_intermediate_bundle("https://issuer/cert", pin), range(16)))
    assert len(set(paths)) == 1 and len(calls) == 1
    assert calls[0][1] == {"timeout": 15, "verify": True, "allow_redirects": False}
    assert Path(paths[0]).read_bytes() == b"ORIGINAL ROOTS\nPINNED PEM"
    assert trust.read_bytes() == b"ORIGINAL ROOTS"


@pytest.mark.parametrize("status,body", [(200, b"unreviewed"), (302, b"certificate"), (200, b"x" * 64_001)])
def test_bad_pin_redirect_or_oversized_certificate_cannot_create_trust(status, body, monkeypatch):
    monkeypatch.setattr(archive, "_CA_BUNDLES", {})
    monkeypatch.setattr(archive, "_CA_TEMPS", {})
    monkeypatch.setattr(archive.requests, "get", lambda *a, **k: Response(status, body))

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid issuer content created a trusted bundle")

    monkeypatch.setattr(archive.tempfile, "TemporaryDirectory", forbidden)
    with pytest.raises(RuntimeError, match="certificate"):
        archive._pinned_intermediate_bundle("https://issuer/cert", "reviewed")
    assert not archive._CA_BUNDLES and not archive._CA_TEMPS


def test_main_and_attachment_host_redirects_choose_distinct_verified_chains(monkeypatch):
    calls = []
    targets = [Response(302, location="https://ws.dgbas.gov.tw/original.pdf"), Response(body=b"%PDF original")]

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return targets.pop(0)

    monkeypatch.setattr(archive, "_session", lambda: SimpleNamespace(get=get))
    monkeypatch.setattr(archive, "_stat_ca_bundle", lambda: "stat-pinned-ca")
    monkeypatch.setattr(archive, "_ws_dgbas_ca_bundle", lambda: "ws-pinned-ca")
    limiter = SimpleNamespace(wait=lambda: None, defer=lambda *args: pytest.fail("unexpected retry"))
    assert archive._fetch("https://www.stat.gov.tw/News.aspx", limiter, max_bytes=100) == b"%PDF original"
    assert [kwargs["verify"] for _, kwargs in calls] == ["stat-pinned-ca", "ws-pinned-ca"]
    assert all(kwargs["allow_redirects"] is False for _, kwargs in calls)


@pytest.mark.parametrize("host", ["stat.gov.tw", "www.stat.gov.tw", "ws.dgbas.gov.tw"])
@pytest.mark.parametrize("status", [401, 403])
def test_authorization_or_access_denial_is_not_retried_or_treated_as_data(host, status, monkeypatch):
    calls = []
    monkeypatch.setattr(archive, "_stat_ca_bundle", lambda: "stat-pinned-ca")
    monkeypatch.setattr(archive, "_ws_dgbas_ca_bundle", lambda: "ws-pinned-ca")

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(status)

    monkeypatch.setattr(archive, "_session", lambda: SimpleNamespace(get=get))
    limiter = SimpleNamespace(wait=lambda: None, defer=lambda *args: pytest.fail("denial wasted more calls"))
    with pytest.raises(archive.SourceAccessBlocked, match=f"HTTP {status}"):
        archive._fetch(f"https://{host}/original", limiter, max_bytes=100)
    assert len(calls) == 1 and calls[0][1]["verify"] is not False


def test_redirect_to_unregistered_host_never_fetches_another_certificate_or_page(monkeypatch):
    calls = []
    monkeypatch.setattr(archive, "_stat_ca_bundle", lambda: "stat-pinned-ca")
    monkeypatch.setattr(archive, "_ws_dgbas_ca_bundle", lambda: pytest.fail("out-of-scope chain"))

    def get(url, **kwargs):
        calls.append(url)
        return Response(302, location="https://unregistered.test/secret")

    monkeypatch.setattr(archive, "_session", lambda: SimpleNamespace(get=get))
    with pytest.raises(ValueError, match="out-of-scope"):
        archive._fetch("https://www.stat.gov.tw/original", SimpleNamespace(wait=lambda: None), max_bytes=100)
    assert calls == ["https://www.stat.gov.tw/original"]
