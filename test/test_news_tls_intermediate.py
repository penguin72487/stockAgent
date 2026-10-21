"""Synthetic PKI + loopback TLS: a missing intermediate is not a new root.

No source access, publisher article fixtures, permanent keys or external trust
mutation. OpenSSL is already part of the canonical runtime used for diagnosis.
"""

from contextlib import closing, contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import subprocess
import threading

import pytest
import requests

from downloader.news_fulltext_connection_observation import ConnectionObservingAdapter
from downloader.news_fulltext_connections import OriginConnectionSession
from downloader.news_fulltext_policy import NewsError
from downloader import news_fulltext_tls as tls


def openssl(*args):
    return subprocess.run(['openssl', *map(str, args)], capture_output=True, check=True, timeout=10).stdout


@pytest.fixture(scope='module')
def pki(tmp_path_factory):
    root = tmp_path_factory.mktemp('news-synthetic-pki')

    def certificate(name, *, issuer=None, days=2, hostname='localhost', ca=False):
        key, csr, pem, ext = [root / (name + suffix) for suffix in ('.key', '.csr', '.pem', '.ext')]
        openssl('ecparam', '-name', 'prime256v1', '-genkey', '-noout', '-out', key)
        openssl('req', '-new', '-key', key, '-subj', '/CN=' + name, '-out', csr)
        ext.write_text('basicConstraints=critical,CA:' + ('TRUE' if ca else 'FALSE') + '\n'
            + ('keyUsage=critical,keyCertSign,cRLSign\n' if ca else
               'keyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:' + hostname + '\n')
            + 'subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n')
        sign = ['-signkey', key] if issuer is None else ['-CA', issuer['pem'], '-CAkey', issuer['key'], '-CAcreateserial']
        validity = (['-not_before', '20000101000000Z', '-not_after', '20000102000000Z']
                    if days < 0 else ['-days', days])
        openssl('x509', '-req', '-in', csr, *sign, *validity, '-extfile', ext, '-out', pem)
        return {'pem': pem, 'key': key}

    anchor = certificate('trusted-root', ca=True)
    foreign = certificate('untrusted-root', ca=True)
    intermediate = certificate('intermediate', issuer=anchor, ca=True)
    leaf = certificate('leaf', issuer=intermediate)
    mismatch = certificate('wrong-host', issuer=intermediate, hostname='different.invalid')
    expired = certificate('expired-leaf', issuer=intermediate, days=-1)
    expired_ca = certificate('expired-intermediate', issuer=anchor, days=-1, ca=True)
    bad_chain = certificate('expired-chain-leaf', issuer=expired_ca)
    return dict(anchor=anchor, foreign=foreign, intermediate=intermediate, leaf=leaf,
                mismatch=mismatch, expired=expired, expired_ca=expired_ca, bad_chain=bad_chain)


@contextmanager
def tls_server(leaf):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.server.paths.append(self.path)
            data = b'native synthetic bytes'
            self.send_response(200)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.paths = []
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # The server sends the leaf alone, reproducing the missing-chain defect.
    context.load_cert_chain(leaf['pem'], leaf['key'])
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        yield server, f'https://localhost:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def bound_adapter(origin, context):
    adapter = ConnectionObservingAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
    adapter.bind_verified_tls_context(origin, context)
    return adapter


def fetch_with_chain(origin, context, path='/original'):
    with requests.Session() as session:
        session.trust_env = False
        adapter = bound_adapter(origin, context)
        session.mount('https://', adapter)
        response = session.get(origin + path, timeout=2)
        assert response.content == b'native synthetic bytes'
        observation = adapter.finish_connection_observation()
        assert observation['connect_attempts'] == observation['connect_completed'] == 1
        assert adapter.max_retries.total == 0
        return response


def test_missing_intermediate_fails_normally_then_chain_works_without_disabling_tls(pki):
    with tls_server(pki['leaf']) as (server, origin):
        with requests.Session() as native:
            native.trust_env = False
            with pytest.raises(requests.exceptions.SSLError):
                native.get(origin + '/must-not-send', verify=str(pki['anchor']['pem']), timeout=2)
        context = tls._chain_context(pki['intermediate']['pem'].read_text(), cafile=str(pki['anchor']['pem']))
        assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
        assert not context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
        fetch_with_chain(origin, context)
        assert server.paths == ['/original']


@pytest.mark.parametrize('leaf,intermediate,anchor', [
    ('leaf', 'intermediate', 'foreign'),  # Intermediate cannot be its own anchor.
    ('mismatch', 'intermediate', 'anchor'),
    ('expired', 'intermediate', 'anchor'),
    ('bad_chain', 'expired_ca', 'anchor'),
    ('leaf', 'expired_ca', 'anchor'),
])
def test_invalid_root_hostname_expiry_or_chain_still_fails_before_article_http(pki, leaf, intermediate, anchor):
    with tls_server(pki[leaf]) as (server, origin):
        context = tls._chain_context(pki[intermediate]['pem'].read_text(), cafile=str(pki[anchor]['pem']))
        with pytest.raises(requests.exceptions.SSLError):
            fetch_with_chain(origin, context)
        assert server.paths == []


@pytest.mark.parametrize('origin', [
    'http://iknow.stpi.narl.org.tw', 'https://iknow.stpi.narl.org.tw:8443',
    'https://iknow.stpi.niar.org.tw', 'https://www.stpi.narl.org.tw',
    'https://iknow.stpi.narl.org.tw.attacker.invalid', 'https://publisher.test',
])
def test_only_exact_reviewed_origin_can_load_chain(monkeypatch, origin):
    class ForbiddenRead:
        def open(self, *args):
            pytest.fail('unrelated origin must not read the reviewed certificate')
    monkeypatch.setattr(tls, 'INTERMEDIATE_FILE', ForbiddenRead())
    assert tls.reviewed_origin_context(origin) is None
    with OriginConnectionSession() as session:
        if origin.endswith(':8443'):
            # Keep the canonical publisher URL grammar; no port widening for a
            # synthetic TLS fixture or a repair of a different exact origin.
            with pytest.raises(NewsError, match='invalid_port'):
                session.get_adapter(origin + '/known')
            return
        adapter = session.get_adapter(origin + '/known')
        assert adapter._news_tls_context is None
        assert session.get_adapter(origin + '/other') is adapter


def test_installed_public_intermediate_digest_and_native_roots_are_bound_without_network():
    pem = tls.INTERMEDIATE_FILE.read_text()
    assert hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest() == tls.INTERMEDIATE_DER_SHA256
    with OriginConnectionSession() as session:
        adapter = session.get_adapter(tls.REVIEWED_ORIGIN + '/robots.txt')
        context = adapter._news_tls_context
        assert context is not None and context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
        assert not context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
        assert context.cert_store_stats()['x509_ca'] > 1
        assert session.get_adapter(tls.REVIEWED_ORIGIN + '/Post/Read.aspx?PostID=3250') is adapter
        assert adapter.max_retries.total == 0


@pytest.mark.parametrize('kind', ['oversize', 'wrong-digest', 'two-certificates', 'private-key', 'missing'])
def test_corrupt_or_missing_chain_fails_closed_and_does_not_cache_adapter(tmp_path, monkeypatch, kind):
    path = tmp_path / 'certificate.pem'
    pem = tls.INTERMEDIATE_FILE.read_bytes()
    data = {'oversize': b'A' * 8193, 'wrong-digest': pem.replace(b'AQELBQAD', b'AQELBQAE'),
            'two-certificates': pem + pem, 'private-key': b'-----BEGIN PRIVATE KEY-----\nx\n'}
    if kind != 'missing':
        path.write_bytes(data[kind])
    monkeypatch.setattr(tls, 'INTERMEDIATE_FILE', path)
    with OriginConnectionSession() as session:
        good = session.get_adapter('https://publisher.test/healthy')
        with pytest.raises(requests.exceptions.SSLError, match='configuration_invalid'):
            session.get_adapter(tls.REVIEWED_ORIGIN + '/robots.txt')
        assert tls.REVIEWED_ORIGIN not in session.origin_adapters
        assert session.get_adapter('https://publisher.test/healthy') is good


@pytest.mark.parametrize('mutation', ['partial-chain', 'no-hostname', 'no-certificate'])
def test_context_cannot_be_weakened_after_binding(pki, mutation):
    context = tls._chain_context(pki['intermediate']['pem'].read_text(), cafile=str(pki['anchor']['pem']))
    with closing(bound_adapter(tls.REVIEWED_ORIGIN, context)) as adapter:
        if mutation == 'partial-chain':
            context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
        else:
            context.check_hostname = False
            if mutation == 'no-certificate':
                context.verify_mode = ssl.CERT_NONE
        with pytest.raises(requests.exceptions.SSLError, match='existing_root_and_hostname'):
            adapter.build_connection_pool_key_attributes(requests.Request('GET', tls.REVIEWED_ORIGIN).prepare(), True)


@pytest.mark.parametrize('url,verify,cert', [
    (tls.REVIEWED_ORIGIN, False, None), (tls.REVIEWED_ORIGIN, '/other/trust.pem', None),
    (tls.REVIEWED_ORIGIN, True, 'client-cert.pem'), ('https://publisher.test/', True, None),
    ('http://iknow.stpi.narl.org.tw/', True, None),
])
def test_bound_adapter_rejects_scope_or_verification_override(pki, url, verify, cert):
    context = tls._chain_context(pki['intermediate']['pem'].read_text(), cafile=str(pki['anchor']['pem']))
    with closing(bound_adapter(tls.REVIEWED_ORIGIN, context)) as adapter:
        with pytest.raises(requests.exceptions.SSLError, match='scope_mismatch'):
            adapter.build_connection_pool_key_attributes(requests.Request('GET', url).prepare(), verify, cert)


def test_reset_is_origin_local_and_reconstructs_verified_context():
    with OriginConnectionSession() as session:
        healthy = session.get_adapter('https://publisher.test/healthy')
        old = session.get_adapter(tls.REVIEWED_ORIGIN + '/robots.txt')
        assert session.reset_origin(tls.REVIEWED_ORIGIN)
        new = session.get_adapter(tls.REVIEWED_ORIGIN + '/robots.txt')
        assert new is not old and new._news_tls_context is not old._news_tls_context
        assert new._news_tls_context.check_hostname
        assert session.get_adapter('https://publisher.test/healthy') is healthy
