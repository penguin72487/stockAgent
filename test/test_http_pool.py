from concurrent.futures import ThreadPoolExecutor
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request

import pytest

from downloader import http_pool


@pytest.fixture
def api_server():
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            with self.server.guard:
                self.server.paths.append(self.path)
                self.server.clients.add(self.client_address)
            body = gzip.compress(b'{"observed": -17}', mtime=0) if self.path == "/gzip" else b'{"observed": -17}'
            status = 429 if self.path == "/quota" else 302 if self.path == "/redirect" else 200
            self.send_response(status)
            self.send_header("Content-Length", str(len(body) + (100 if self.path == "/broken" else 0)))
            if self.path == "/gzip":
                self.send_header("Content-Encoding", "gzip")
            if self.path == "/quota":
                self.send_header("Retry-After", "60")
            if self.path == "/redirect":
                self.send_header("Location", "/target")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            if self.path == "/broken":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.guard = threading.Lock()
    server.paths = []
    server.clients = set()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server, f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join(2)


@pytest.fixture
def opener(monkeypatch):
    monkeypatch.setattr(http_pool, "getproxies", lambda: {})
    pool = http_pool.PooledUrlOpener(origins=2, connections_per_origin=2)
    yield pool
    pool.close()


def test_complete_responses_reuse_one_connection_and_preserve_raw_bytes(api_server, opener):
    server, base = api_server
    for _ in range(4):
        with opener(Request(base + "/gzip"), timeout=2) as response:
            assert response.status == response.getcode() == 200
            assert response.geturl() == base + "/gzip"
            assert response.read() == gzip.compress(b'{"observed": -17}', mtime=0)
            assert response.headers["Content-Encoding"] == "gzip"
    assert len(server.clients) == 1
    assert len(server.paths) == 4


@pytest.mark.parametrize("path,status", [("/quota", 429), ("/redirect", 302)])
def test_no_hidden_retries_or_redirects_and_error_body_is_readable(api_server, opener, path, status):
    server, base = api_server
    with pytest.raises(HTTPError) as caught:
        opener(base + path, timeout=2)
    with caught.value as error:
        assert error.code == status
        assert error.read() == b'{"observed": -17}'
    assert server.paths == [path]
    with opener(base + "/good", timeout=2) as response:
        assert response.read() == b'{"observed": -17}'
    assert len(server.clients) == 1


def test_partial_response_is_not_reused_or_retried(api_server, opener):
    server, base = api_server
    with pytest.raises(URLError):
        opener(base + "/broken", timeout=2)
    with opener(base + "/good", timeout=2) as response:
        assert response.read() == b'{"observed": -17}'
    assert server.paths == ["/broken", "/good"]
    assert len(server.clients) == 2


def test_parallel_callers_respect_connection_bound(api_server, opener):
    server, base = api_server

    def fetch(_index):
        with opener(base + "/good", timeout=2) as response:
            return response.read()

    with ThreadPoolExecutor(max_workers=8) as executor:
        assert list(executor.map(fetch, range(20))) == [b'{"observed": -17}'] * 20
    assert len(server.paths) == 20
    assert len(server.clients) <= 2


@pytest.mark.parametrize("data,proxy", [(b"signed body", {}), (None, {"https": "https://proxy.invalid"})])
def test_writes_and_proxy_requests_keep_original_transport(monkeypatch, data, proxy):
    monkeypatch.setattr(http_pool, "getproxies", lambda: proxy)
    monkeypatch.setattr(http_pool, "proxy_bypass", lambda _host: False)
    calls = []
    monkeypatch.setattr(http_pool, "urlopen", lambda req, **kw: calls.append((req, kw)))
    with_pool = http_pool.PooledUrlOpener()
    req = Request("https://example.test/api", data=data)
    assert with_pool(req, timeout=7) is None
    assert calls == [(req, {"timeout": 7})]
    assert with_pool._pool is None


def test_fork_identity_discards_parent_pool_before_using_any_inherited_lock(monkeypatch, opener):
    previous = object()
    opener._pool = previous
    opener._pid = -1
    opener._after_fork()
    assert opener._pool is None
    assert opener._pid == http_pool.os.getpid()


def test_invalid_pool_bounds_are_rejected():
    with pytest.raises(ValueError):
        http_pool.PooledUrlOpener(connections_per_origin=0)
