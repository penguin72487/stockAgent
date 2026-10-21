"""Reviewed, exact-origin intermediate chains; never new trust anchors.

The STPI server omitted its intermediate on 2026-10-20. The leaf's AIA named
this TWCA certificate, fetched over normally verified HTTPS and independently
verified against the existing Requests roots. Supplying that public chain is
not permission to request a source: NewsHTTP still owns robots and deadlines.
No AIA fetching, leaf pinning, global trust-store mutation or TLS retry occurs.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import ssl

import requests


REVIEWED_ORIGIN = 'https://iknow.stpi.narl.org.tw'
INTERMEDIATE_URL = 'https://sslserver.twca.com.tw/cacert/secure_sha2_2023G3.crt'
INTERMEDIATE_DER_SHA256 = '1a2c75fd096e0499e9ff6ac74e526f61eaae3edfc8c2ea4436fee0c24d8b7d0e'
INTERMEDIATE_FILE = Path(__file__).resolve().parents[1] / 'configs/news/tls/twca_secure_ssl_2023g3.crt'


def _chain_context(intermediate_pem: str, *, cafile=None) -> ssl.SSLContext:
    """Add chain material, still requiring an existing self-signed root.

    Python 3.13+ may default to PARTIAL_CHAIN, which would instead promote an
    intermediate into an anchor. Explicitly prohibit that flag; keep all other
    native validation, including stricter validation on newer runtimes.
    """
    context = ssl.create_default_context(cafile=cafile or requests.utils.DEFAULT_CA_BUNDLE_PATH)
    context.verify_flags &= ~ssl.VERIFY_X509_PARTIAL_CHAIN
    context.load_verify_locations(cadata=intermediate_pem)
    return context


def reviewed_origin_context(origin: str) -> ssl.SSLContext | None:
    """Only the reviewed HTTPS authority, not suffixes, ports or new aliases."""
    if origin != REVIEWED_ORIGIN:
        return None
    with INTERMEDIATE_FILE.open('rb') as stream:
        raw = stream.read(8193)
    if len(raw) > 8192 or not re.fullmatch(
            rb'-----BEGIN CERTIFICATE-----\n[A-Za-z0-9+/=\n]+-----END CERTIFICATE-----\n', raw):
        raise ssl.SSLError('reviewed_intermediate_invalid_pem')
    pem = raw.decode('ascii')
    if hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest() != INTERMEDIATE_DER_SHA256:
        raise ssl.SSLError('reviewed_intermediate_digest_mismatch')
    return _chain_context(pem)
