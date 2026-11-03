"""Exact reviewed TAIFEX market-closure notices, not broker-account evidence.

Seven originals were inspected at their exact uncompressed SHA-256. The 2011
notice contains extractable text; the subsequent tables were visually reviewed.
Each notice gives the last and next market sessions. Nothing here
infers a broker valuation, cash event, executable price, or historical receipt.
"""
from __future__ import annotations

from datetime import date
import gzip
import hashlib
from pathlib import Path

from downloader.artifact_io import sha256_file


# Publication day, last market session, reopening session, reviewed original.
REVIEWED = (
    (date(2010, 12, 16), date(2011, 1, 28), date(2011, 2, 8),
     "6d22d15520a7456e14845f7e493b2c19026c8828bcc58ce0897657894bd1c2fa"),
    (date(2011, 12, 2), date(2012, 1, 18), date(2012, 1, 30),
     "df9937f008ab3fc5037d7b68cde9a8b1d89e7fd7108d045205896a11f320266b"),
    (date(2013, 1, 25), date(2013, 2, 6), date(2013, 2, 18),
     "f8ecdfaecd66b510772caaa4a20be313239dbc13696be2f22b25cae57da7e8c0"),
    (date(2014, 1, 9), date(2014, 1, 27), date(2014, 2, 5),
     "a0da401cfd53157b518dc3872d64ee9e5cc3e103d106498f5c27198a7baecdb4"),
    (date(2015, 1, 14), date(2015, 2, 13), date(2015, 2, 24),
     "020cd365c8b1c03e701dccf7efe1b921494aff20d5d38134da9e9e3ab33c7e0d"),
    (date(2016, 1, 8), date(2016, 2, 3), date(2016, 2, 15),
     "0ee9702e367ea0cd8f9f9f1a5d0b8b1603c127a837a3eaf01d430c6285b52f51"),
    (date(2017, 1, 3), date(2017, 1, 24), date(2017, 2, 2),
     "cfcaedb2ec9204ed4e5f2bb18d67e06b1c9722215e7620ea4dac7922a48843b8"),
)


def early_holiday_closures(rules_root: Path) -> tuple[dict[date, date], list[dict]]:
    """Bind the reviewed reopenings to retained official originals."""
    root = rules_root.resolve()
    dates: dict[date, date] = {}
    pins: list[dict] = []
    last_reopening = None
    for published, previous, reopening, content_sha in REVIEWED:
        path = root / "raw" / content_sha[:2] / f"{content_sha}.body.gz"
        if path.is_symlink() or not path.is_file():
            raise ValueError("reviewed TAIFEX holiday original is missing")
        before = sha256_file(path)
        if (hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != content_sha
                or sha256_file(path) != before):
            raise ValueError("reviewed TAIFEX holiday original changed")
        if (not (published < previous < reopening < date(2017, 5, 15))
                or reopening in dates or (last_reopening is not None and previous <= last_reopening)):
            raise ValueError("reviewed holiday intervals overlap or are not pre-night")
        dates[reopening] = previous
        last_reopening = reopening
        pins.append(dict(path=str(path), sha256=before, content_sha256=content_sha,
                         published_date=str(published), last_session=str(previous),
                         reopening_session=str(reopening)))
    return dates, pins
