"""Bounded streaming decoding for gzip sitemap metadata, never article bodies."""
from __future__ import annotations

import gzip
import io
import zlib

from lxml import etree

from downloader.news_fulltext_policy import NewsError


GZIP_SITEMAP_CONTRACT_VERSION = 1
# https://www.sitemaps.org/protocol.html: these limits apply after decompression.
MAX_SITEMAP_DECODED_BYTES = 50 << 20
MAX_SITEMAP_ENTRIES = 50_000
SITEMAP_NAMESPACE = 'http://www.sitemaps.org/schemas/sitemap/0.9'


class _BoundedReader:
    """Limit total expansion across gzip members without materializing the XML."""

    def __init__(self, stream):
        self.stream = stream
        self.decoded_bytes = 0

    def read(self, size):
        remaining = MAX_SITEMAP_DECODED_BYTES - self.decoded_bytes
        payload = self.stream.read(min(size, 64 << 10, remaining + 1))
        self.decoded_bytes += len(payload)
        if self.decoded_bytes > MAX_SITEMAP_DECODED_BYTES:
            raise NewsError('sitemap_decoded_limit_exceeded')
        return payload


def gzip_sitemap_links(payload: bytes) -> list[tuple[str, str, str]]:
    """Validate the complete gzip/XML envelope before returning any identities.

    Direct sitemap entries preserve source order, duplicates and literal lastmod.
    Extension metadata is not a publication clock or an article body. Clearing
    each finished entry bounds the XML tree; the returned URL list remains O(n).
    """
    links = []
    depth, entries, root_tag = 0, 0, None
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
            reader = _BoundedReader(stream)
            parser = etree.iterparse(reader, events=('start', 'end'), resolve_entities=False,
                load_dtd=False, no_network=True, huge_tree=False, recover=False,
                remove_comments=True, remove_pis=True)
            for event, element in parser:
                if event == 'start':
                    depth += 1
                    if depth == 1:
                        if element.getroottree().docinfo.doctype:
                            raise NewsError('sitemap_doctype_not_allowed')
                        name = etree.QName(element)
                        if name.namespace not in {None, SITEMAP_NAMESPACE} or name.localname not in {'urlset', 'sitemapindex'}:
                            raise NewsError('invalid_sitemap')
                        root_tag = name.localname
                        prefix = '{' + name.namespace + '}' if name.namespace else ''
                    continue
                if depth == 2:
                    expected = prefix + ('sitemap' if root_tag == 'sitemapindex' else 'url')
                    if element.tag == expected:
                        entries += 1
                        if entries > MAX_SITEMAP_ENTRIES:
                            raise NewsError('sitemap_entry_limit_exceeded')
                        location = element.find(prefix + 'loc')
                        if location is not None:
                            url = ''.join(part.strip() for part in location.itertext())
                            modified = element.find(prefix + 'lastmod')
                            date = ''.join(modified.itertext()) if modified is not None else ''
                            links.append((url, 'sitemap', '') if root_tag == 'sitemapindex' else (url, '', date))
                    element.clear()
                    while element.getprevious() is not None:
                        del element.getparent()[0]
                depth -= 1
            # iterparse consumes through EOF: gzip CRC/truncation and trailing
            # XML are validated before the caller can register even one URL.
    except (OSError, EOFError, zlib.error):
        raise NewsError('invalid_sitemap_gzip') from None
    except etree.XMLSyntaxError:
        raise NewsError('invalid_discovery_xml') from None
    return links
