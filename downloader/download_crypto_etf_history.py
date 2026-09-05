from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
from html.parser import HTMLParser
from io import BytesIO, StringIO
import json
import math
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import polars as pl

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    PersistentProgress,
    SharedRateLimiter,
    load_env_file,
    provider_rate_limit,
    retry_delay_seconds,
)
from artifact_io import (  # noqa: E402
    atomic_write_bytes as _atomic_bytes,
    atomic_write_json as _atomic_json,
    atomic_write_parquet as _atomic_parquet,
    sha256_bytes as _sha256,
)


SCHEMA_VERSION = 1


@dataclass(slots=True)
class SourceResult:
    source_id: str
    provider: str
    status: str
    rows: int
    requests: int
    output_paths: list[str]
    message: str | None = None


@dataclass(slots=True)
class IssuerFrames:
    result: SourceResult
    daily_metrics: list[dict[str, Any]]
    holdings: list[dict[str, Any]]
    reserves: list[dict[str, Any]]


class SecComplianceError(RuntimeError):
    pass


class HttpStatusError(RuntimeError):
    def __init__(self, code: int, url: str, detail: str) -> None:
        super().__init__(f"HTTP {code} for {url}: {detail}")
        self.code = int(code)
        self.url = url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill registered crypto ETP SEC filings/company facts and "
            "versioned official issuer NAV, holdings and asset-per-share history."
        )
    )
    parser.add_argument(
        "--config", type=Path, default=Path("configs/crypto_etf_sources.json")
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_crypto_etf"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--sec-workers", type=int, default=10)
    parser.add_argument("--issuer-workers", type=int, default=4)
    parser.add_argument("--max-sec-entities", type=int, default=0)
    parser.add_argument("--max-primary-documents-per-entity", type=int, default=0)
    parser.add_argument(
        "--primary-documents",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--skip-sec", action="store_true")
    parser.add_argument("--skip-issuers", action="store_true")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-base", type=float, default=1.0)
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    if value in {None, ""} or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", "").replace("$", "").replace("%", ""))
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _http_date(value: str | None) -> str | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


_CONTACT_EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+"
)


def _sec_user_agent(raw_user_agent: str, contact_email: str = "") -> str:
    """Return a compliant ASCII SEC identity without exposing its value.

    Python's HTTP client encodes header values as latin-1.  Names or
    organisations written in CJK therefore fail before a request is sent.
    SEC needs a descriptive identity and contact address, so preserve the
    ASCII portions and email while replacing an otherwise empty label with a
    stable application identity.
    """

    raw = " ".join(str(raw_user_agent or "").split())
    contact = " ".join(str(contact_email or "").split())
    email_match = _CONTACT_EMAIL_PATTERN.search(raw) or _CONTACT_EMAIL_PATTERN.search(
        contact
    )
    if email_match is None:
        return ""
    email = email_match.group(0)
    ascii_identity = raw.encode("ascii", errors="ignore").decode("ascii")
    ascii_identity = " ".join(ascii_identity.split())
    if not ascii_identity or ascii_identity.casefold() == email.casefold():
        ascii_identity = "stockAgent research"
    if email.casefold() not in ascii_identity.casefold():
        ascii_identity = f"{ascii_identity} {email}"
    # Keep a bounded, printable value for urllib/http.client header encoding.
    return ascii_identity[:512]


class HttpClient:
    def __init__(self, *, max_retries: int, retry_base: float) -> None:
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))
        self._limiters: dict[str, SharedRateLimiter] = {}
        self._lock = threading.Lock()

    def _limiter(self, profile_name: str) -> SharedRateLimiter:
        with self._lock:
            limiter = self._limiters.get(profile_name)
            if limiter is None:
                profile = provider_rate_limit(profile_name)
                limiter = SharedRateLimiter(
                    profile.interval_seconds, name=profile.provider
                )
                self._limiters[profile_name] = limiter
            return limiter

    def get(
        self,
        url: str,
        *,
        profile_name: str,
        user_agent: str,
        accept: str = "application/json,text/plain,text/html,*/*",
    ) -> tuple[bytes, dict[str, str]]:
        limiter = self._limiter(profile_name)
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            request = Request(
                url,
                headers={
                    "User-Agent": user_agent,
                    "Accept": accept,
                    "Accept-Encoding": "identity",
                },
            )
            try:
                with urlopen(request, timeout=120) as response:
                    return response.read(), {
                        str(key).lower(): str(value)
                        for key, value in response.headers.items()
                    }
            except HTTPError as exc:
                last_error = exc
                if (
                    exc.code in {408, 429, 500, 502, 503, 504}
                    and attempt < self.max_retries
                ):
                    limiter.defer(
                        retry_delay_seconds(
                            attempt,
                            base=self.retry_base,
                            retry_after=exc.headers.get("Retry-After"),
                        )
                    )
                    continue
                detail = exc.read().decode("utf-8", errors="replace")[:400]
                raise HttpStatusError(exc.code, url, detail) from exc
            except (URLError, TimeoutError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    limiter.defer(retry_delay_seconds(attempt, base=self.retry_base))
                    continue
                raise
        raise last_error or RuntimeError(f"request failed for {url}")


def _persist_raw(
    output_dir: Path, category: str, source_id: str, payload: bytes, suffix: str
) -> tuple[Path, str]:
    digest = _sha256(payload)
    path = output_dir / "raw" / category / source_id / f"{digest}.{suffix.lstrip('.')}"
    if not path.is_file():
        stored = (
            gzip.compress(payload, compresslevel=6, mtime=0)
            if path.suffix == ".gz"
            else payload
        )
        _atomic_bytes(path, stored)
    return path, digest


def _json(payload: bytes) -> dict[str, Any]:
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise RuntimeError("expected a JSON object")
    return decoded


def _filing_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    recent = (
        payload.get("filings", {}).get("recent")
        if isinstance(payload.get("filings"), dict)
        else payload
    )
    if not isinstance(recent, dict):
        return []
    columns = {
        str(key): value for key, value in recent.items() if isinstance(value, list)
    }
    length = max((len(value) for value in columns.values()), default=0)
    return [
        {
            key: values[index] if index < len(values) else None
            for key, values in columns.items()
        }
        for index in range(length)
    ]


def _filing_available_at(row: dict[str, Any]) -> str:
    accepted = str(row.get("acceptanceDateTime") or "").strip()
    if accepted:
        try:
            parsed = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(UTC).isoformat()
        except ValueError:
            pass
    filed = str(row.get("filingDate") or "").strip()
    return f"{filed}T23:59:59+00:00" if filed else datetime.now(UTC).isoformat()


def _companyfact_rows(
    payload: dict[str, Any],
    *,
    cik: str,
    tickers: list[str],
    title: str,
    retrieved_at: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    facts = payload.get("facts") or {}
    if not isinstance(facts, dict):
        return output
    for taxonomy, taxonomy_facts in facts.items():
        if not isinstance(taxonomy_facts, dict):
            continue
        for tag, definition in taxonomy_facts.items():
            if not isinstance(definition, dict):
                continue
            label = definition.get("label")
            description = definition.get("description")
            units = definition.get("units") or {}
            if not isinstance(units, dict):
                continue
            for unit, observations in units.items():
                if not isinstance(observations, list):
                    continue
                for item in observations:
                    if not isinstance(item, dict):
                        continue
                    value_number = _safe_float(item.get("val"))
                    output.append(
                        {
                            "cik": cik,
                            "registered_tickers": ",".join(tickers),
                            "entity_name": title,
                            "taxonomy": str(taxonomy),
                            "tag": str(tag),
                            "label": str(label or ""),
                            "description": str(description or ""),
                            "unit": str(unit),
                            "value_float": value_number,
                            "value_text": None
                            if value_number is not None
                            else str(item.get("val") or ""),
                            "period_start": item.get("start"),
                            "period_end": item.get("end"),
                            "filed_date": item.get("filed"),
                            "form": item.get("form"),
                            "fiscal_year": item.get("fy"),
                            "fiscal_period": item.get("fp"),
                            "frame": item.get("frame"),
                            "accession_number": item.get("accn"),
                            "available_at_utc": f"{item.get('filed')}T23:59:59+00:00"
                            if item.get("filed")
                            else retrieved_at,
                            "retrieved_at_utc": retrieved_at,
                        }
                    )
    return output


def _sec_entity(
    client: HttpClient,
    output_dir: Path,
    config: dict[str, Any],
    *,
    cik: int,
    tickers: list[str],
    assets: list[str],
    issuers: list[str],
    title: str,
    user_agent: str,
    download_documents: bool,
    max_documents: int,
) -> SourceResult:
    cik10 = f"{cik:010d}"
    requests = 0
    output_paths: list[str] = []
    retrieved_at = datetime.now(UTC).isoformat()
    submissions_url = str(config["submissions_url_template"]).format(cik10=cik10)
    raw, _ = client.get(
        submissions_url, profile_name="sec_edgar", user_agent=user_agent
    )
    requests += 1
    path, submissions_sha = _persist_raw(
        output_dir, "sec/submissions", cik10, raw, "json.gz"
    )
    output_paths.append(str(path))
    submissions = _json(raw)
    filings = _filing_rows(submissions)
    for file_spec in (submissions.get("filings") or {}).get("files", []):
        name = str(file_spec.get("name") or "") if isinstance(file_spec, dict) else ""
        if not name:
            continue
        shard_url = str(config["submission_shard_url_template"]).format(name=name)
        shard_raw, _ = client.get(
            shard_url, profile_name="sec_edgar", user_agent=user_agent
        )
        requests += 1
        shard_path, _ = _persist_raw(
            output_dir, "sec/submission_shards", cik10, shard_raw, "json.gz"
        )
        output_paths.append(str(shard_path))
        filings.extend(_filing_rows(_json(shard_raw)))
    normalized_filings: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in filings:
        accession = str(row.get("accessionNumber") or "").strip()
        document = str(row.get("primaryDocument") or "").strip()
        key = (accession, document)
        if not accession or key in seen:
            continue
        seen.add(key)
        normalized_filings.append(
            {
                "cik": cik10,
                "registered_tickers": ",".join(sorted(tickers)),
                "registered_assets": ",".join(sorted(set(assets))),
                "registered_issuers": ",".join(sorted(set(issuers))),
                "entity_name": str(submissions.get("name") or title),
                "accession_number": accession,
                "filing_date": row.get("filingDate"),
                "report_date": row.get("reportDate"),
                "acceptance_datetime": row.get("acceptanceDateTime"),
                "form": row.get("form"),
                "file_number": row.get("fileNumber"),
                "film_number": row.get("filmNumber"),
                "primary_document": document,
                "primary_doc_description": row.get("primaryDocDescription"),
                "available_at_utc": _filing_available_at(row),
                "retrieved_at_utc": retrieved_at,
                "submissions_raw_sha256": submissions_sha,
            }
        )
    filings_frame = (
        pl.from_dicts(normalized_filings, infer_schema_length=None, strict=False)
        if normalized_filings
        else pl.DataFrame()
    )
    filings_path = output_dir / "normalized" / "sec" / cik10 / "filings.parquet"
    _atomic_parquet(filings_path, filings_frame)
    output_paths.append(str(filings_path))

    facts_url = str(config["companyfacts_url_template"]).format(cik10=cik10)
    try:
        facts_raw, _ = client.get(
            facts_url, profile_name="sec_edgar", user_agent=user_agent
        )
        requests += 1
        facts_raw_path, facts_sha = _persist_raw(
            output_dir, "sec/companyfacts", cik10, facts_raw, "json.gz"
        )
        output_paths.append(str(facts_raw_path))
        fact_rows = _companyfact_rows(
            _json(facts_raw),
            cik=cik10,
            tickers=sorted(tickers),
            title=title,
            retrieved_at=retrieved_at,
        )
    except HttpStatusError as exc:
        if exc.code != 404:
            raise
        facts_sha = None
        fact_rows = []
    facts_frame = (
        pl.from_dicts(fact_rows, infer_schema_length=None, strict=False)
        if fact_rows
        else pl.DataFrame()
    )
    facts_path = output_dir / "normalized" / "sec" / cik10 / "companyfacts.parquet"
    _atomic_parquet(facts_path, facts_frame)
    output_paths.append(str(facts_path))

    documents_downloaded = 0
    document_receipts: list[dict[str, Any]] = []
    allowed_forms = {str(value) for value in config.get("forms", [])}
    document_failures: list[dict[str, str]] = []
    if download_documents:
        candidates = [
            row
            for row in normalized_filings
            if row.get("form") in allowed_forms and row.get("primary_document")
        ]
        if max_documents > 0:
            candidates = candidates[:max_documents]
        for row in candidates:
            accession = str(row["accession_number"])
            primary_document = Path(str(row["primary_document"])).name
            accession_compact = accession.replace("-", "")
            target = (
                output_dir
                / "raw"
                / "sec"
                / "primary_documents"
                / cik10
                / accession
                / f"{primary_document}.gz"
            )
            fallback_template = str(
                config.get("complete_submission_url_template") or ""
            ).strip()
            fallback_url = (
                fallback_template.format(
                    cik=cik,
                    accession=accession,
                    accession_compact=accession_compact,
                )
                if fallback_template
                else ""
            )
            fallback_target = (
                output_dir
                / "raw"
                / "sec"
                / "complete_submissions"
                / cik10
                / accession
                / f"{accession}.txt.gz"
            )
            if target.is_file():
                try:
                    cached_raw = gzip.decompress(target.read_bytes())
                    document_receipts.append(
                        {
                            "accession_number": accession,
                            "primary_document": primary_document,
                            "status": "cached",
                            "raw_path": str(target),
                            "raw_sha256": _sha256(cached_raw),
                            "raw_bytes": len(cached_raw),
                        }
                    )
                except (OSError, gzip.BadGzipFile):
                    quarantine = target.with_name(
                        target.name
                        + ".corrupt-"
                        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                    )
                    try:
                        target.rename(quarantine)
                    except OSError:
                        document_failures.append(
                            {
                                "accession_number": accession,
                                "primary_document": primary_document,
                                "error": "cached primary document is unreadable and could not be quarantined",
                            }
                        )
                        continue
                else:
                    continue
            if fallback_url and fallback_target.is_file():
                try:
                    cached_fallback_raw = gzip.decompress(fallback_target.read_bytes())
                    document_receipts.append(
                        {
                            "accession_number": accession,
                            "primary_document": primary_document,
                            "status": "cached_complete_submission",
                            "url": fallback_url,
                            "raw_path": str(fallback_target),
                            "raw_sha256": _sha256(cached_fallback_raw),
                            "raw_bytes": len(cached_fallback_raw),
                        }
                    )
                except (OSError, gzip.BadGzipFile):
                    quarantine = fallback_target.with_name(
                        fallback_target.name
                        + ".corrupt-"
                        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                    )
                    try:
                        fallback_target.rename(quarantine)
                    except OSError:
                        document_failures.append(
                            {
                                "accession_number": accession,
                                "primary_document": primary_document,
                                "error": (
                                    "cached complete submission is unreadable and "
                                    "could not be quarantined"
                                ),
                            }
                        )
                        continue
                else:
                    continue
            document_url = str(config["primary_document_url_template"]).format(
                cik=cik,
                accession_compact=accession_compact,
                primary_document=quote(primary_document, safe="._-"),
            )
            try:
                document_raw, _ = client.get(
                    document_url,
                    profile_name="sec_edgar",
                    user_agent=user_agent,
                    accept="text/html,application/xml,text/plain,*/*",
                )
                requests += 1
                _atomic_bytes(
                    target, gzip.compress(document_raw, compresslevel=6, mtime=0)
                )
                documents_downloaded += 1
                document_receipts.append(
                    {
                        "accession_number": accession,
                        "primary_document": primary_document,
                        "status": "downloaded",
                        "url": document_url,
                        "raw_path": str(target),
                        "raw_sha256": _sha256(document_raw),
                        "raw_bytes": len(document_raw),
                    }
                )
            except Exception as primary_exc:
                if not fallback_url:
                    document_failures.append(
                        {
                            "accession_number": accession,
                            "primary_document": primary_document,
                            "error": f"{type(primary_exc).__name__}: {primary_exc}",
                        }
                    )
                    continue
                try:
                    fallback_raw, _ = client.get(
                        fallback_url,
                        profile_name="sec_edgar",
                        user_agent=user_agent,
                        accept="text/plain,*/*",
                    )
                    requests += 1
                    _atomic_bytes(
                        fallback_target,
                        gzip.compress(fallback_raw, compresslevel=6, mtime=0),
                    )
                    documents_downloaded += 1
                    document_receipts.append(
                        {
                            "accession_number": accession,
                            "primary_document": primary_document,
                            "status": "fallback_complete_submission",
                            "primary_url": document_url,
                            "primary_error": (
                                f"{type(primary_exc).__name__}: {primary_exc}"
                            ),
                            "url": fallback_url,
                            "raw_path": str(fallback_target),
                            "raw_sha256": _sha256(fallback_raw),
                            "raw_bytes": len(fallback_raw),
                        }
                    )
                except Exception as fallback_exc:
                    document_failures.append(
                        {
                            "accession_number": accession,
                            "primary_document": primary_document,
                            "error": (
                                f"primary={type(primary_exc).__name__}: {primary_exc}; "
                                f"complete_submission={type(fallback_exc).__name__}: "
                                f"{fallback_exc}"
                            ),
                        }
                    )
    manifest_path = output_dir / "normalized" / "sec" / cik10 / "manifest.json"
    _atomic_json(
        manifest_path,
        {
            "schema_version": SCHEMA_VERSION,
            "cik": cik10,
            "tickers": sorted(tickers),
            "assets": sorted(set(assets)),
            "issuers": sorted(set(issuers)),
            "entity_name": str(submissions.get("name") or title),
            "filing_rows": filings_frame.height,
            "companyfact_rows": facts_frame.height,
            "documents_downloaded_this_run": documents_downloaded,
            "primary_documents": document_receipts,
            "document_failures": document_failures,
            "submissions_raw_sha256": submissions_sha,
            "companyfacts_raw_sha256": facts_sha,
            "completed_at_utc": datetime.now(UTC).isoformat(),
        },
    )
    output_paths.append(str(manifest_path))
    return SourceResult(
        f"sec_cik_{cik10}",
        "SEC EDGAR",
        "degraded" if document_failures else "complete",
        filings_frame.height + facts_frame.height,
        requests,
        output_paths,
        f"{len(document_failures)} primary documents failed and will be retried"
        if document_failures
        else None,
    )


class _NextDataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.capture = False
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script" and dict(attrs).get("id") == "__NEXT_DATA__":
            self.capture = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self.capture:
            self.capture = False

    def handle_data(self, data: str) -> None:
        if self.capture:
            self.parts.append(data)


def _issuer_context(
    spec: dict[str, Any], observed_at: str, raw_sha: str
) -> dict[str, Any]:
    return {
        "source_id": str(spec["id"]),
        "provider": str(spec["provider"]),
        "ticker": str(spec["ticker"]),
        "asset": str(spec["asset"]),
        "observed_at_utc": observed_at,
        "available_at_utc": observed_at,
        "point_in_time_state": "historical_first_observed_at_retrieval",
        "raw_sha256": raw_sha,
    }


def _parse_ishares_xls(
    spec: dict[str, Any], raw: bytes, observed_at: str, raw_sha: str
) -> IssuerFrames:
    book = pd.ExcelFile(BytesIO(raw), engine="xlrd")
    sheet = next((name for name in book.sheet_names if "Gross Proceeds" in name), None)
    if sheet is None:
        raise RuntimeError(f"{spec['id']}: Gross Proceeds sheet is missing")
    table = pd.read_excel(BytesIO(raw), sheet_name=sheet, header=None, engine="xlrd")
    if table.shape[1] < 5:
        raise RuntimeError(
            f"{spec['id']}: unexpected Gross Proceeds schema {table.shape}"
        )
    context = _issuer_context(spec, observed_at, raw_sha)
    rows: list[dict[str, Any]] = []
    metric_columns = (
        (2, "asset_units_per_share", f"{spec['asset']}/share"),
        (3, "asset_units_sold_for_expenses_per_share", f"{spec['asset']}/share"),
        (4, "expense_sale_proceeds_per_share", "USD/share"),
    )
    for _, values in table.iloc[2:].iterrows():
        parsed_date = pd.to_datetime(values.iloc[1], errors="coerce")
        if pd.isna(parsed_date):
            continue
        for index, metric, unit in metric_columns:
            value = _safe_float(values.iloc[index])
            rows.append(
                {
                    **context,
                    "event_date": parsed_date.date().isoformat(),
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                }
            )
    return IssuerFrames(
        SourceResult(
            str(spec["id"]), str(spec["provider"]), "complete", len(rows), 1, []
        ),
        rows,
        [],
        [],
    )


def _parse_ishares_holdings(
    spec: dict[str, Any], raw: bytes, observed_at: str, raw_sha: str
) -> IssuerFrames:
    text = raw.decode("utf-8-sig")
    lines = text.splitlines()
    header_index = next(
        (index for index, line in enumerate(lines) if line.startswith("Ticker,Name,")),
        None,
    )
    if header_index is None:
        raise RuntimeError(f"{spec['id']}: holdings CSV header is missing")
    metadata: dict[str, str] = {}
    for line in lines[1:header_index]:
        parsed = next(csv.reader([line]), [])
        if len(parsed) >= 2:
            metadata[parsed[0].strip()] = parsed[1].strip()
    as_of = pd.to_datetime(metadata.get("Fund Holdings as of"), errors="coerce")
    if pd.isna(as_of):
        raise RuntimeError(f"{spec['id']}: holdings as-of date is missing")
    context = _issuer_context(spec, observed_at, raw_sha)
    holdings: list[dict[str, Any]] = []
    for row in csv.DictReader(StringIO("\n".join(lines[header_index:]))):
        holdings.append(
            {
                **context,
                "as_of_date": as_of.date().isoformat(),
                "holding_ticker": row.get("Ticker"),
                "holding_name": row.get("Name"),
                "asset_class": row.get("Asset Class"),
                "market_value_usd": _safe_float(row.get("Market Value")),
                "weight_percent": _safe_float(row.get("Weight (%)")),
                "notional_value_usd": _safe_float(row.get("Notional Value")),
                "quantity": _safe_float(row.get("Quantity")),
                "currency": row.get("Market Currency"),
            }
        )
    metrics = [
        {
            **context,
            "event_date": as_of.date().isoformat(),
            "metric": "shares_outstanding",
            "value": _safe_float(metadata.get("Shares Outstanding")),
            "unit": "shares",
        }
    ]
    return IssuerFrames(
        SourceResult(
            str(spec["id"]),
            str(spec["provider"]),
            "complete",
            len(holdings) + len(metrics),
            1,
            [],
        ),
        metrics,
        holdings,
        [],
    )


def _parse_bitwise(
    spec: dict[str, Any], raw: bytes, observed_at: str, raw_sha: str
) -> IssuerFrames:
    parser = _NextDataParser()
    parser.feed(raw.decode("utf-8", errors="replace"))
    if not parser.parts:
        raise RuntimeError(f"{spec['id']}: __NEXT_DATA__ is missing")
    payload = json.loads("".join(parser.parts))
    page_props = payload["props"]["pageProps"]
    data = page_props["fundData"]["data"]
    context = _issuer_context(spec, observed_at, raw_sha)
    metrics: list[dict[str, Any]] = []
    chart = (data.get("navAndMarketPrice") or {}).get("chart") or {}
    for field, metric in (
        ("nav", "nav_per_share"),
        ("marketPrice", "market_price_per_share"),
    ):
        for timestamp_ms, value in chart.get(field, []):
            event_date = (
                datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=UTC)
                .date()
                .isoformat()
            )
            metrics.append(
                {
                    **context,
                    "event_date": event_date,
                    "metric": metric,
                    "value": _safe_float(value),
                    "unit": "USD/share",
                }
            )
    details = data.get("fundDetails") or {}
    as_of = str(details.get("asOfDate") or "")
    for field, metric, unit in (
        ("netAssets", "net_assets", "USD"),
        ("sharesOutstanding", "shares_outstanding", "shares"),
        ("expenseRatio", "expense_ratio", "ratio"),
    ):
        if as_of and details.get(field) is not None:
            metrics.append(
                {
                    **context,
                    "event_date": as_of,
                    "metric": metric,
                    "value": _safe_float(details.get(field)),
                    "unit": unit,
                }
            )
    holdings: list[dict[str, Any]] = []
    holding_payload = data.get("holdings") or {}
    holding_as_of = str(holding_payload.get("asOfDate") or as_of)
    for row in holding_payload.get("basket") or []:
        holdings.append(
            {
                **context,
                "as_of_date": holding_as_of,
                "holding_ticker": row.get("ticker") or spec.get("asset"),
                "holding_name": row.get("companyName"),
                "asset_class": "Digital Asset",
                "market_value_usd": _safe_float(row.get("marketValue")),
                "weight_percent": (_safe_float(row.get("weight")) or 0.0) * 100.0,
                "notional_value_usd": _safe_float(row.get("notionalValue")),
                "quantity": _safe_float(row.get("shares")),
                "currency": spec.get("asset"),
            }
        )
    reserves: list[dict[str, Any]] = []
    reserve = page_props.get("proofOfReservesSnapshotData") or {}
    if reserve.get("timestamp"):
        reserves.append(
            {
                **context,
                "snapshot_at_utc": reserve.get("timestamp"),
                "reserve_asset_units": _safe_float(reserve.get("totalReserve")),
                "nav_asset_units": _safe_float(reserve.get("totalNAV")),
                "ripcord": bool(reserve.get("ripcord")),
                "definition": "Issuer-published proof-of-reserves snapshot; not independently asserted as solvency.",
            }
        )
    result = SourceResult(
        str(spec["id"]),
        str(spec["provider"]),
        "complete",
        len(metrics) + len(holdings) + len(reserves),
        1,
        [],
    )
    return IssuerFrames(result, metrics, holdings, reserves)


def _issuer_source(
    client: HttpClient, output_dir: Path, spec: dict[str, Any]
) -> IssuerFrames:
    source_id = str(spec["id"])
    adapter = str(spec["adapter"])
    observed_at = datetime.now(UTC).isoformat()
    receipt_path = output_dir / "receipts" / "issuers" / f"{source_id}.json"
    raw: bytes | None = None
    raw_path: Path | None = None
    raw_sha: str | None = None
    requests = 0
    if spec.get("immutable") and receipt_path.is_file():
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            cached_path = Path(str(receipt["raw_path"]))
            if cached_path.is_file():
                raw = cached_path.read_bytes()
                if cached_path.suffix == ".gz":
                    raw = gzip.decompress(raw)
                raw_path = cached_path
                raw_sha = str(receipt["raw_sha256"])
                observed_at = str(receipt["observed_at_utc"])
        except (OSError, KeyError, ValueError, json.JSONDecodeError, gzip.BadGzipFile):
            raw = None
    if raw is None:
        profile = (
            "ishares_public" if spec.get("provider") == "iShares" else "bitwise_public"
        )
        raw, headers = client.get(
            str(spec["url"]), profile_name=profile, user_agent="stockAgent-crypto-etf/1"
        )
        requests = 1
        extension = (
            "xls"
            if adapter.endswith("xls")
            else ("csv.gz" if adapter.endswith("csv") else "html.gz")
        )
        raw_path, raw_sha = _persist_raw(
            output_dir, "issuers", source_id, raw, extension
        )
        _atomic_json(
            receipt_path,
            {
                "schema_version": SCHEMA_VERSION,
                "source_id": source_id,
                "url": spec["url"],
                "raw_path": str(raw_path),
                "raw_sha256": raw_sha,
                "http_last_modified_utc": _http_date(headers.get("last-modified")),
                "observed_at_utc": observed_at,
            },
        )
    assert raw_sha is not None and raw_path is not None
    adapters = {
        "ishares_gross_proceeds_xls": _parse_ishares_xls,
        "ishares_holdings_csv": _parse_ishares_holdings,
        "bitwise_next_data": _parse_bitwise,
    }
    if adapter not in adapters:
        raise RuntimeError(f"unsupported issuer adapter: {adapter}")
    parsed = adapters[adapter](spec, raw, observed_at, raw_sha)
    parsed.result.requests = requests
    parsed.result.output_paths = [str(raw_path), str(receipt_path)]
    return parsed


def _upsert_rows(path: Path, rows: list[dict[str, Any]], keys: list[str]) -> int:
    if not rows:
        return 0
    incoming = pl.from_dicts(rows, infer_schema_length=None, strict=False)
    if path.is_file():
        existing = pl.read_parquet(path)
        all_columns = list(dict.fromkeys([*existing.columns, *incoming.columns]))
        existing = existing.select(
            [
                pl.col(name) if name in existing.columns else pl.lit(None).alias(name)
                for name in all_columns
            ]
        )
        incoming = incoming.select(
            [
                pl.col(name) if name in incoming.columns else pl.lit(None).alias(name)
                for name in all_columns
            ]
        )
        frame = pl.concat([existing, incoming], how="vertical_relaxed")
    else:
        frame = incoming
    frame = frame.sort("observed_at_utc").unique(
        subset=keys, keep="last", maintain_order=True
    )
    _atomic_parquet(path, frame)
    return frame.height


def _resolve_sec_entities(
    client: HttpClient,
    output_dir: Path,
    sec_config: dict[str, Any],
    funds: list[dict[str, Any]],
    user_agent: str,
) -> tuple[list[dict[str, Any]], list[str], int]:
    raw, _ = client.get(
        str(sec_config["company_tickers_url"]),
        profile_name="sec_edgar",
        user_agent=user_agent,
    )
    raw_path, digest = _persist_raw(
        output_dir, "sec", "company_tickers", raw, "json.gz"
    )
    mapping_payload = _json(raw)
    mapping = {
        str(item.get("ticker") or "").upper(): item
        for item in mapping_payload.values()
        if isinstance(item, dict)
    }
    entities: dict[int, dict[str, Any]] = {}
    missing: list[str] = []
    for fund in funds:
        ticker = str(fund["ticker"]).upper()
        item = mapping.get(ticker)
        if item is None:
            missing.append(ticker)
            continue
        cik = int(item["cik_str"])
        entity = entities.setdefault(
            cik,
            {
                "cik": cik,
                "tickers": [],
                "assets": [],
                "issuers": [],
                "title": str(item.get("title") or ""),
            },
        )
        entity["tickers"].append(ticker)
        entity["assets"].append(str(fund["asset"]))
        entity["issuers"].append(str(fund["issuer"]))
    _atomic_json(
        output_dir / "receipts" / "sec" / "company_tickers.json",
        {
            "schema_version": SCHEMA_VERSION,
            "raw_path": str(raw_path),
            "raw_sha256": digest,
            "resolved_entities": len(entities),
            "missing_tickers": sorted(missing),
            "observed_at_utc": datetime.now(UTC).isoformat(),
        },
    )
    return list(entities.values()), sorted(missing), 1


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    config_path = args.config if args.config.is_absolute() else repo_root / args.config
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else repo_root / args.output_dir
    )
    env_file = (
        args.env_file if args.env_file.is_absolute() else repo_root / args.env_file
    )
    load_env_file(
        env_file, allowed_names={"SEC_USER_AGENT", "STOCKAGENT_CONTACT_EMAIL"}
    )
    config = json.loads(config_path.read_text(encoding="utf-8"))
    selected = {value.upper() for value in args.tickers} if args.tickers else None
    funds = [
        item
        for item in config["sec"]["funds"]
        if selected is None or str(item["ticker"]).upper() in selected
    ]
    if selected:
        unknown = selected - {str(item["ticker"]).upper() for item in funds}
        if unknown:
            raise SystemExit(
                "unregistered crypto ETF tickers: " + ", ".join(sorted(unknown))
            )
    issuer_specs = [
        item
        for item in config.get("issuer_sources", [])
        if selected is None or str(item["ticker"]).upper() in selected
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (output_dir / ".download.lock").open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(
            f"[crypto-etf] another updater owns {output_dir / '.download.lock'}; skip",
            flush=True,
        )
        return 0
    client = HttpClient(max_retries=args.max_retries, retry_base=args.retry_base)
    results: list[SourceResult] = []
    sec_entities: list[dict[str, Any]] = []
    sec_user_agent = _sec_user_agent(
        os.getenv("SEC_USER_AGENT", ""),
        os.getenv("STOCKAGENT_CONTACT_EMAIL", ""),
    )
    initial_requests = 0
    missing_sec_tickers: list[str] = []
    if not args.skip_sec and sec_user_agent:
        sec_entities, missing_sec_tickers, initial_requests = _resolve_sec_entities(
            client, output_dir, config["sec"], funds, sec_user_agent
        )
        if args.max_sec_entities > 0:
            sec_entities = sec_entities[: args.max_sec_entities]
    progress_total = (
        0 if args.skip_sec else max(1, len(sec_entities) + len(missing_sec_tickers))
    ) + (0 if args.skip_issuers else len(issuer_specs))
    progress = PersistentProgress(
        output_dir / "progress.json",
        label="SEC and crypto ETF issuer history",
        total=progress_total,
        unit="sources",
        basis="ETA uses completed SEC entities and issuer sources; filing-document count and remote publication latency vary.",
    )
    if not args.skip_sec and not sec_user_agent:
        result = SourceResult(
            "sec_edgar",
            "SEC EDGAR",
            "blocked_configuration",
            0,
            0,
            [],
            "Set SEC_USER_AGENT='name organization email' with an ASCII contact email for SEC fair-access identification.",
        )
        results.append(result)
        progress.update("SEC_USER_AGENT", result.status)
    elif not args.skip_sec:
        for ticker in missing_sec_tickers:
            result = SourceResult(
                f"sec_ticker_{ticker}",
                "SEC EDGAR",
                "unavailable_mapping",
                0,
                0,
                [],
                "Ticker is registered locally but absent from the current SEC company_tickers.json mapping.",
            )
            results.append(result)
            progress.update(result.source_id, result.status)
        with ThreadPoolExecutor(max_workers=max(1, args.sec_workers)) as executor:
            futures = {
                executor.submit(
                    _sec_entity,
                    client,
                    output_dir,
                    config["sec"],
                    cik=entity["cik"],
                    tickers=entity["tickers"],
                    assets=entity["assets"],
                    issuers=entity["issuers"],
                    title=entity["title"],
                    user_agent=sec_user_agent,
                    download_documents=args.primary_documents,
                    max_documents=max(0, args.max_primary_documents_per_entity),
                ): entity
                for entity in sec_entities
            }
            for future in as_completed(futures):
                entity = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = SourceResult(
                        f"sec_cik_{int(entity['cik']):010d}",
                        "SEC EDGAR",
                        "failed",
                        0,
                        0,
                        [],
                        f"{type(exc).__name__}: {exc}",
                    )
                results.append(result)
                progress.update(result.source_id, result.status)
        if initial_requests:
            results.append(
                SourceResult(
                    "sec_company_tickers",
                    "SEC EDGAR",
                    "complete",
                    len(sec_entities),
                    initial_requests,
                    [],
                )
            )

    issuer_frames: list[IssuerFrames] = []
    if not args.skip_issuers:
        with ThreadPoolExecutor(max_workers=max(1, args.issuer_workers)) as executor:
            futures = {
                executor.submit(_issuer_source, client, output_dir, spec): spec
                for spec in issuer_specs
            }
            for future in as_completed(futures):
                spec = futures[future]
                try:
                    parsed = future.result()
                except Exception as exc:
                    parsed = IssuerFrames(
                        SourceResult(
                            str(spec["id"]),
                            str(spec["provider"]),
                            "failed",
                            0,
                            0,
                            [],
                            f"{type(exc).__name__}: {exc}",
                        ),
                        [],
                        [],
                        [],
                    )
                issuer_frames.append(parsed)
                results.append(parsed.result)
                progress.update(parsed.result.source_id, parsed.result.status)
        daily_rows = [row for parsed in issuer_frames for row in parsed.daily_metrics]
        holding_rows = [row for parsed in issuer_frames for row in parsed.holdings]
        reserve_rows = [row for parsed in issuer_frames for row in parsed.reserves]
        daily_total = _upsert_rows(
            output_dir / "normalized" / "issuer_daily_fund_metrics.parquet",
            daily_rows,
            ["source_id", "ticker", "event_date", "metric"],
        )
        holdings_total = _upsert_rows(
            output_dir / "normalized" / "issuer_holdings_snapshots.parquet",
            holding_rows,
            ["source_id", "ticker", "as_of_date", "holding_ticker"],
        )
        reserves_total = _upsert_rows(
            output_dir / "normalized" / "issuer_reserve_snapshots.parquet",
            reserve_rows,
            ["source_id", "ticker", "snapshot_at_utc"],
        )
    else:
        daily_total = holdings_total = reserves_total = 0

    failed = any(
        result.status
        in {"failed", "degraded", "blocked_configuration", "unavailable_mapping"}
        for result in results
    )
    progress.finish(failed=failed)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "state": "failed" if failed else "complete",
        "selected_funds": len(funds),
        "sec_entities": len(sec_entities),
        "issuer_sources": len(issuer_specs),
        "complete_sources": sum(result.status == "complete" for result in results),
        "failed_sources": sum(result.status == "failed" for result in results),
        "degraded_sources": sum(result.status == "degraded" for result in results),
        "blocked_sources": sum(
            result.status == "blocked_configuration" for result in results
        ),
        "unavailable_mapping_sources": sum(
            result.status == "unavailable_mapping" for result in results
        ),
        "rows_this_run": sum(result.rows for result in results),
        "requests": sum(result.requests for result in results),
        "issuer_daily_metric_rows_total": daily_total,
        "issuer_holdings_rows_total": holdings_total,
        "issuer_reserve_rows_total": reserves_total,
        "results": [
            asdict(result)
            for result in sorted(results, key=lambda item: item.source_id)
        ],
    }
    _atomic_json(output_dir / "download_summary.json", summary)
    print(
        json.dumps(
            {
                key: summary[key]
                for key in (
                    "state",
                    "selected_funds",
                    "sec_entities",
                    "issuer_sources",
                    "complete_sources",
                    "failed_sources",
                    "blocked_sources",
                    "rows_this_run",
                    "requests",
                )
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
