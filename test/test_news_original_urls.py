"""Native legacy metadata plus bounded malformed/security cases; no HTTP."""

import base64

import pytest

from stockagent.data.news_source_urls import resolve_google_literal_url


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result) + bytes([value])


def field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def redirect(payload):
    return "https://news.google.com/rss/articles/" + base64.urlsafe_b64encode(payload).decode().rstrip("=") + "?oc=5"


def test_native_original_and_amp_are_distinct_evidence():
    url = "https://news.google.com/rss/articles/CBMiKWh0dHBzOi8vZWMubHRuLmNvbS50dy9hcnRpY2xlL3BhcGVyLzEzODE20gEtaHR0cHM6Ly9lYy5sdG4uY29tLnR3L2FtcC9hcnRpY2xlL3BhcGVyLzEzODE2?oc=5"
    result = resolve_google_literal_url(url)
    assert result.outcome == "literal_original"
    assert result.original_url == "https://ec.ltn.com.tw/article/paper/13816"
    assert result.alternate_url == "https://ec.ltn.com.tw/amp/article/paper/13816"
    assert result.envelope_sha256 and result.contract_version == 1


def test_native_empty_alternate():
    url = "https://news.google.com/rss/articles/CBMiO2h0dHBzOi8vd3d3LmNoZWVycy5jb20udHcvYXJ0aWNsZS9hcnRpY2xlLmFjdGlvbj9pZD01MDI1MDMx0gEA?oc=5"
    result = resolve_google_literal_url(url)
    assert result.original_url == "https://www.cheers.com.tw/article/article.action?id=5025031"
    assert result.alternate_url is None


def test_native_internal_rss_path_has_the_same_literal_envelope():
    # Decoding retained metadata, NOT requesting Google's internal route.
    # The source metadata title/stock association is not certified.
    result = resolve_google_literal_url("https://news.google.com/__i/rss/rd/articles/CBMiJ2h0dHBzOi8vdWRuLmNvbS9uZXdzL3N0b3J5LzcyNTQvNjY2MjA4MNIBK2h0dHBzOi8vdWRuLmNvbS9uZXdzL2FtcC9zdG9yeS83MjU0LzY2NjIwODA?oc=5")
    assert result.original_url == "https://udn.com/news/story/7254/6662080"
    assert result.alternate_url == "https://udn.com/news/amp/story/7254/6662080"


@pytest.mark.parametrize("payload,error", [
    (b"\x08\x13" + field(4, b"AU_yqLopaqueToken"), "opaque_original_url"),
    (b"\x08\x13" + field(4, b"https://publisher.test/a") + field(4, b"https://publisher.test/b"), "duplicate_envelope_field"),
    (b"\x08\x13" + field(4, b"https://publisher.test/a") + field(26, b"") + field(26, b""), "duplicate_envelope_field"),
    (b"\x08\x13" + field(4, b"https://publisher.test/a") + field(5, b"unreviewed"), "unreviewed_envelope_fields"),
    (b"\x08\x12" + field(4, b"https://publisher.test/a"), "unreviewed_envelope_fields"),
    (field(4, b"https://publisher.test/a"), "unreviewed_envelope_fields"),
    (b"\x08\x13\x22\x7fshort", "truncated_length_field"),
    (b"\x08\x13\x22\x80", "truncated_varint"),
    (b"\x08\x13\x22" + b"\xff" * 10, "overflow_varint"),
    (b"\x08\x13\x23", "unreviewed_envelope_fields"),
])
def test_opaque_and_unreviewed_envelopes_do_not_guess(payload, error):
    result = resolve_google_literal_url(redirect(payload))
    assert result.error_code == error and result.original_url is None
    assert result.envelope_sha256


@pytest.mark.parametrize("literal", [
    b"https://user:secret@publisher.test/a", b"http://127.0.0.1/a", b"http://8.8.8.8/a",
    b"http://localhost/a", b"http://stock.internal/a", b"https://publisher.test:8443/a",
    b"https://pub%20lisher.test/a", b"https://publisher.test/a\n", b" https://publisher.test/a",
    b"https://news.google.com/rss/articles/abc", b"https://t.co/abc",
    b"https://publisher.test/\xff", b"https://publisher.test/a b",
])
def test_unsafe_literal_is_not_routed(literal):
    result = resolve_google_literal_url(redirect(b"\x08\x13" + field(4, literal)))
    assert result.outcome == "unresolved" and result.original_url is None


@pytest.mark.parametrize("url", [
    "https://news.google.com.evil.test/rss/articles/CBMiAA", "https://user@news.google.com/rss/articles/abc",
    "https://news.google.com/rss/search?q=stock", "https://news.google.com/read/opaque",
    "https://news.google.com/rss/articles/a", "https://news.google.com/rss/articles/CBMi*",
    "\nhttps://news.google.com/rss/articles/CBMiAA",
    "https://news.google.com/rss/articles/" + "a" * 9000, None,
])
def test_invalid_redirects_are_not_decoded(url):
    result = resolve_google_literal_url(url)
    assert result.outcome == "unresolved" and result.original_url is None


def test_canonical_url_normalization_preserves_identity_query():
    result = resolve_google_literal_url(redirect(b"\x08\x13" + field(4, b"https://Publisher.test/a?b=2&a=1&utm_source=rss#fragment")))
    assert result.original_url == "https://publisher.test/a?b=2&a=1"


def test_invalid_alternate_cannot_turn_into_a_fallback():
    result = resolve_google_literal_url(redirect(b"\x08\x13" + field(4, b"https://publisher.test/a") + field(26, b"https://user:secret@publisher.test/a")))
    assert result.original_url is None and result.alternate_url is None


def test_noncanonical_base64_is_rejected():
    url = redirect(b"\x08\x13" + field(4, b"https://publisher.test/a"))
    token = url.split("/articles/")[1].split("?")[0]
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    # This payload has two insignificant trailing bits. Altering those bits
    # must not create a second metadata envelope identity for the same bytes.
    assert len(token) % 4 in {2, 3}
    changed = token[:-1] + alphabet[alphabet.index(token[-1]) + 1]
    result = resolve_google_literal_url("https://news.google.com/rss/articles/" + changed)
    assert result.error_code == "invalid_base64_envelope"
