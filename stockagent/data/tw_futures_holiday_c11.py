"""Source-bound holiday marks from original exchange C11/CB1 and P08/PA8.

Layout reference: TAIFEX file transfer format v1.14.1, sections 1.5/1.10.
These are account valuations, never trades or opening references. Historical
format compatibility and acquisition authority must be reviewed separately;
decoding a file does not establish historical availability to the policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_portfolio_daily import _OFFICIAL_PRODUCT_OVERRIDES

HOLIDAY_C11_ABI = "taifex_original_c11_holiday_account_valuation_v1"
_KEYS = ["date", "product", "contract"]
_LAYOUT = "taifex_file_transfer_1_14_1"


def _records(body: bytes, width: int) -> list[bytes]:
    """Accept fixed records, LF or CRLF; never strip fixed-width padding."""
    if not body or any(byte > 127 for byte in body):
        raise ValueError("empty or non-ASCII TAIFEX account file")
    if b"\n" in body or b"\r" in body:
        rows = body.split(b"\n")
        if rows[-1] == b"":
            rows.pop()
        rows = [row[:-1] if row.endswith(b"\r") else row for row in rows]
    else:
        rows = [body[i:i + width] for i in range(0, len(body), width)]
    if not rows or any(len(row) != width or b"\r" in row for row in rows):
        raise ValueError("TAIFEX account record width or framing differs")
    return rows


def _integer(raw: bytes) -> int:
    if not raw.isdigit():
        raise ValueError("TAIFEX account numeric field is not unsigned decimal")
    return int(raw)


def _day(raw: bytes) -> date:
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError("invalid TAIFEX account date")
    try:
        return datetime.strptime(raw.decode("ascii"), "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError("invalid TAIFEX account date") from exc


def decode_c11(body: bytes, *, file_code: str) -> list[dict]:
    """Keep integer price fields until joined to a dated product definition.

    The long CB1 identifier adds ten bytes. Option underlying-price appendices
    have different semantics and are deliberately rejected by this futures
    account decoder rather than interpreted as futures settlement records.
    """
    if file_code not in {"C11", "CB1"}:
        raise ValueError("expected C11 or CB1 account file")
    n = 10 if file_code == "C11" else 20
    result = []
    seen = set()
    for number, row in enumerate(_records(body, n + 54), 1):
        identifier = row[:n].decode("ascii").rstrip(" ")
        if (not re.fullmatch(r"[A-Z0-9]+", identifier) or row[n + 25:n + 26] not in
                (b"N", b"E", b"P", b"Z") or row[n + 26:] != b" " * 28):
            raise ValueError("invalid C11 futures identity, status or padding")
        record = dict(prod_id=identifier, encoded_price=_integer(row[n:n + 9]),
                      open_interest=_integer(row[n + 9:n + 17]),
                      date=_day(row[n + 17:n + 25]), status=row[n + 25:n + 26].decode(),
                      source_row=number)
        key = (record["date"], identifier)
        if key in seen:
            raise ValueError("duplicate C11 account date/product identity")
        seen.add(key)
        result.append(record)
    return result


def decode_p08_futures(body: bytes, *, file_code: str) -> dict[str, dict]:
    """Read the dated month, lifetime and price decimals, without year guessing.

    Supports the documented 160/170-byte layout only. Other historical P08
    layouts require a separate reviewed decoder; they cannot be padded here.
    """
    if file_code not in {"P08", "PA8"}:
        raise ValueError("expected P08 or PA8 product definition")
    n = 10 if file_code == "P08" else 20
    result = {}
    for row in _records(body, n + 150):
        identifier = row[:n].decode("ascii").rstrip(" ")
        if not re.fullmatch(r"[A-Z0-9]+", identifier) or identifier in result:
            raise ValueError("invalid or duplicate P08 product identity")
        # Options cannot enter the futures mark table. Their appendices and
        # price-zero rules belong to the options accounting contract.
        if row[n + 15:n + 16] != b" ":
            raise ValueError("options product in a futures P08 account source")
        if (_integer(row[n + 6:n + 15]) != 0 or row[n + 63:n + 64] != b" "
                or row[n + 64:n + 150] != b" " * 86):
            raise ValueError("unsupported P08 futures fields or padding")
        month = row[n:n + 6].decode("ascii")
        if not re.fullmatch(r"\d{6}", month) or not 1 <= int(month[4:]) <= 12:
            raise ValueError("invalid P08 delivery month")
        if row[n + 41:n + 42] not in (b"I", b"R", b"B", b"C", b"S", b"E"):
            raise ValueError("unknown P08 product class")
        begin, end = _day(row[n + 16:n + 24]), _day(row[n + 24:n + 32])
        if begin > end or begin.year < 1900 or end.year < 1900:
            raise ValueError("invalid P08 product lifetime")
        root = identifier[:3]
        result[identifier] = dict(product=_OFFICIAL_PRODUCT_OVERRIDES.get(root, root),
                                  contract=month, begin=begin, end=end,
                                  decimals=_integer(row[n + 43:n + 44]))
    return result


def _rows_digest(rows: pl.DataFrame) -> str:
    payload = json.dumps(rows.sort(_KEYS).to_dicts(), sort_keys=True, default=str,
                         allow_nan=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class HolidayC11Source:
    rows: pl.DataFrame
    fingerprint: str
    financial_daily_sha256: str | None
    pinned_files: tuple[tuple[Path, str], ...]
    rows_sha256: str

    def verify(self) -> None:
        for path, digest in self.pinned_files:
            if sha256_file(path) != digest:
                raise ValueError("holiday C11 source input changed")
        if _rows_digest(self.rows) != self.rows_sha256:
            raise ValueError("holiday C11 decoded rows changed")


def load_holiday_c11(source_dir: Path, *, workspace: Path) -> HolidayC11Source:
    """Verify originals, dated catalog snapshots and every decoded valuation.

    A checksum or manifest flag alone cannot authorize a substituted mark: the
    value, date and month must reconstruct from the original fixed records.
    Acquisition receipts retain source URLs and actual retrieval timestamps.
    This loader makes no PIT-capture or execution-price certification.
    """
    workspace = workspace.resolve()
    source_dir = source_dir.resolve()
    if not source_dir.is_relative_to(workspace):
        raise ValueError("holiday C11 evidence must reside in its workspace")
    path = source_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    financial_sha = manifest.get("financial_daily_sha256")
    if (manifest.get("contract") != HOLIDAY_C11_ABI or manifest.get("layout") != _LAYOUT
            or (financial_sha is not None and (not isinstance(financial_sha, str)
                or not re.fullmatch(r"[0-9a-f]{64}", financial_sha)))
            or not manifest.get("batches")):
        raise ValueError("invalid holiday C11 source contract")
    pins = {path: sha256_file(path)}

    def original(item, expected_day):
        original_path = (workspace / item["path"]).resolve()
        receipt_path = (workspace / item["receipt_path"]).resolve()
        for source, key in ((original_path, "sha256"), (receipt_path, "receipt_sha256")):
            if (not source.is_relative_to(workspace)
                    or sha256_file(source) != item[key]):
                raise ValueError("holiday C11 original or receipt hash/path differs")
            pins[source] = item[key]
        receipt = json.loads(receipt_path.read_text())
        captured = datetime.fromisoformat(receipt["retrieved_at_utc"])
        if (captured.tzinfo is None or receipt.get("content_sha256") != item["sha256"]
                or receipt.get("data_date") != str(expected_day)
                or not isinstance(receipt.get("source_url"), str)
                or not receipt["source_url"].startswith(("https://", "sftp://", "ftps://"))):
            raise ValueError("holiday C11 acquisition receipt lacks bound source provenance")
        return original_path.read_bytes()

    rows = []
    for batch in manifest["batches"]:
        day = date.fromisoformat(batch["account_date"])
        snapshot_day = date.fromisoformat(batch["catalog_date"])
        if snapshot_day > day:
            raise ValueError("future product catalog cannot establish a holiday identity")
        catalogs = decode_p08_futures(original(batch["catalog"], snapshot_day), file_code=batch["catalog"]["file_code"])
        marks = decode_c11(original(batch["marks"], day), file_code=batch["marks"]["file_code"])
        for mark in marks:
            if mark["date"] != day or mark["open_interest"] != 0:
                raise ValueError("holiday C11 date or zero OI does not match")
            # A complete file can include retired products or missing values.
            # Keep their original bytes; they cannot value a nonterminal book.
            # Required identities still fail the downstream coverage check.
            if mark["status"] in {"E", "Z"} or mark["encoded_price"] == 0:
                continue
            definition = catalogs.get(mark["prod_id"])
            if (definition is None
                    or not definition["begin"] <= snapshot_day <= day <= definition["end"]):
                raise ValueError("holiday C11 date, zero OI, price or dated catalog does not match")
            price = Decimal(mark["encoded_price"]).scaleb(-definition["decimals"])
            rows.append(dict(date=day, product=definition["product"], contract=definition["contract"],
                             valuation_price=float(price), source_prod_id=mark["prod_id"],
                             source_row=mark["source_row"], source_sha256=batch["marks"]["sha256"],
                             catalog_sha256=batch["catalog"]["sha256"]))
    frame = pl.DataFrame(rows)
    if frame.is_empty() or frame.select(_KEYS).is_duplicated().any():
        raise ValueError("holiday C11 canonical identity is empty or ambiguous")
    source = HolidayC11Source(frame, pins[path], financial_sha,
                              tuple(sorted(pins.items())), _rows_digest(frame))
    source.verify()
    return source


def require_complete_holiday_c11(
    financial_daily: Path, scope: pl.DataFrame, source: HolidayC11Source,
) -> None:
    """Admit an already compiled release only if every held identity matches.

    No mark is patched at training time. Changed valuations require canonical
    preparation and a fresh financial release. Expiry cash is independently
    admitted by the final-settlement contract, not by this nonterminal gate.
    """
    if not isinstance(source, HolidayC11Source):
        raise ValueError("holiday valuation requires a verified C11 source")
    source.verify()
    if (source.financial_daily_sha256 is not None
            and sha256_file(financial_daily) != source.financial_daily_sha256):
        raise ValueError("holiday C11 source belongs to another financial release")
    required = {*_KEYS, "physical_instance", "valuation_price"}
    if not required <= set(scope.columns) or scope.select(_KEYS).is_duplicated().any():
        raise ValueError("holiday financial scope lacks unambiguous physical identities")
    if scope.select(list(required)).null_count().sum_horizontal().item():
        raise ValueError("holiday financial scope has missing identities or marks")
    joined = scope.join(source.rows.select(*_KEYS, pl.col("valuation_price").alias("_c11_mark")),
                        on=_KEYS, how="left", validate="1:1")
    failures = joined.filter(pl.col("_c11_mark").is_null()
                             | ~pl.col("valuation_price").is_finite()
                             | (pl.col("valuation_price") != pl.col("_c11_mark")))
    if failures.height:
        examples = failures.select(*_KEYS, "physical_instance", "valuation_price", "_c11_mark").head(5).to_dicts()
        raise ValueError(f"holiday C11 valuation coverage/match incomplete: {failures.height} rows; {examples}")
    source.verify()


def apply_holiday_c11_marks(physical: pl.DataFrame, source: HolidayC11Source) -> pl.DataFrame:
    """Canonical preparation only: change account marks, never market prices.

    The source may cover more products/dates than this preparation. Every
    physical nonterminal row on each covered account date must have a mark.
    Other dates remain unadmitted until their own evidence arrives. Calendar,
    final cash, margin and publication retain their independent owners.
    """
    if not isinstance(source, HolidayC11Source):
        raise ValueError("holiday valuation requires a verified C11 source")
    source.verify()
    required = {*_KEYS, "physical_instance", "valuation_price", "accounting_only_date", "cash_settlement"}
    if (not required <= set(physical.columns) or physical.select(_KEYS).is_duplicated().any()
            or physical.select(list(required - {"valuation_price"})).null_count().sum_horizontal().item()):
        raise ValueError("holiday physical calendar lacks unambiguous classifications or identities")
    covered_dates = source.rows["date"].unique()
    if physical.filter(pl.col("date").is_in(covered_dates.implode()) & ~pl.col("accounting_only_date")).height:
        raise ValueError("C11 holiday mark conflicts with a market session")
    selected = (pl.col("date").is_in(covered_dates.implode()) & pl.col("accounting_only_date")
                & ~pl.col("cash_settlement"))
    targets = physical.filter(selected)
    if targets.is_empty():
        raise ValueError("C11 source has no matching nonterminal account dates in this preparation")
    marks = source.rows.select(*_KEYS, pl.col("valuation_price").alias("_holiday_c11_mark"))
    if targets.select(_KEYS).join(marks.select(_KEYS), on=_KEYS, how="anti").height:
        raise ValueError("C11 source is incomplete for the selected physical account dates")
    result = physical.join(marks, on=_KEYS, how="left", validate="1:1", maintain_order="left")
    result = result.with_columns(
        pl.when(selected).then(pl.col("_holiday_c11_mark")).otherwise(pl.col("valuation_price"))
        .alias("valuation_price"),
        pl.when(selected).then(pl.lit(HOLIDAY_C11_ABI)).otherwise(None).alias("holiday_valuation_contract"),
        pl.when(selected).then(pl.lit(source.fingerprint)).otherwise(None)
        .alias("holiday_valuation_source_fingerprint"),
    ).drop("_holiday_c11_mark")
    source.verify()
    return result
