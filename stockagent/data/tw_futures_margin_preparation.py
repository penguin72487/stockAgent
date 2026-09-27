"""Build a bounded TX/MTX rule release from the canonical official archive.

No downloading, guessed historical values, or alternative training pipeline.
All dates refer to the regular session. Publication-day end is a conservative
availability bound; a close boundary labels the post-close account phase.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import unicodedata

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.taifex_rule_parsing import margin_changes
from scripts.download_taifex_rule_history import read_verified_raw

TAIPEI = timezone(timedelta(hours=8))
PRODUCTS = ("TX", "MTX")
ROC_DATE = r"(\d{2,3})年(\d{1,2})月(\d{1,2})日"


def compact(text: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", text))


def roc_date(match) -> date:
    return date(int(match[1]) + 1911, int(match[2]), int(match[3]))


def timestamp(day: date, clock: str) -> str:
    return datetime.combine(day, time.fromisoformat(clock), TAIPEI).isoformat()


def margin_effective(text: str) -> str:
    matches = list(re.finditer(r"自" + ROC_DATE + r"(一般)?交易時段結束後", compact(text)))
    if any(not m[4] and roc_date(m) >= date(2017, 5, 15) for m in matches):
        raise ValueError("post-night-launch notice must identify the regular close")
    values = {timestamp(roc_date(m), "13:45:00") for m in matches}
    if len(values) != 1:
        raise ValueError(f"ambiguous/missing margin effective close: {values}")
    return values.pop()


def check_publication(text: str, published: str) -> None:
    """Bind reusable attachment names to their historical issuing notice."""
    text = compact(text)
    m = re.search(r"發文日期:中華民國" + ROC_DATE, text)
    if m is None:
        raise ValueError("margin/position attachment lacks issuing date")
    lag = (date.fromisoformat(published) - roc_date(m)).days
    if not 0 <= lag <= 7:
        raise ValueError("historical attachment issuing date disagrees with index")


def margin_pdf_values(text: str, product: str) -> tuple[int, ...]:
    # The layout is product-local: never take a unit from another table.
    text = unicodedata.normalize("NFKC", text)
    blocks = re.split(r"單位\s*[:：]\s*", text)
    found = []
    for block in blocks[1:]:
        if not re.match(r"新[臺台]幣元\s+" + re.escape(product) + r"(?:\s|$)", block):
            continue
        dense = compact(block)
        header = "調整後保證金金額調整前保證金金額"
        if header not in dense:
            raise ValueError("unverified PDF before/after column order")
        columns = "原始保證金金額維持保證金金額結算保證金金額" * 2
        if columns not in dense:
            raise ValueError("unverified PDF margin column semantics")
        tail = re.split(r"保證金\s+(?=[\d,]+\s)", block)[-1]
        values = re.findall(r"(?<!\d)\d[\d,]*(?!\d)", tail)
        if len(values) < 6:
            raise ValueError("missing six PDF margin amounts")
        found.append(tuple(int(x.replace(",", "")) for x in values[:6]))
    if len(set(found)) != 1:
        raise ValueError(f"missing/ambiguous {product} TWD PDF table")
    return found[0]


def verified_margin_values(rows: list[dict], pdf_text: str, product: str) -> tuple[int, ...]:
    expected = margin_pdf_values(pdf_text, product)
    facts = [f for f in margin_changes(rows, unit_hint="TWD") if f["contract_code"] == product]
    values = {(f["phase"], f["margin_kind"]): float(f["normalized_value"]) for f in facts}
    keys = [(phase, kind) for phase in ("after", "before")
            for kind in ("initial", "maintenance", "clearing")]
    if len(facts) != 6 or set(values) != set(keys):
        raise ValueError(f"duplicate/incomplete {product} CSV cells")
    actual = tuple(values[k] for k in keys)
    if actual != expected or not all(x > 0 for x in actual):
        raise ValueError(f"{product} CSV/PDF margin disagreement")
    return expected


def position_change(text: str) -> dict | None:
    text = compact(text)
    try:
        clause = text.split("二、", 1)[1].split("三、", 1)[0]
    except IndexError as error:
        raise ValueError("position notice clause structure changed") from error
    if not clause.startswith("「臺股期貨」"):
        raise ValueError("position notice does not isolate TX")
    tx = clause.split("「臺指選擇權」", 1)[0]
    if "自然人" not in tx:
        if "法人部位限制數由" in tx:
            return None
        raise ValueError("unrecognized TX position amendment")
    number = r"([\d,]+)(?:個契約|口)"
    combined = re.search(r"自然人與法人部位限制數分別由" + number + "與" + number
                         + r"調[升高降]至" + number + "與" + number, tx)
    separate = re.search(r"自然人部位限制數由" + number + r"調[升高降]至" + number, tx)
    if combined:
        before, after = combined[1], combined[3]
    elif separate:
        before, after = separate[1], separate[2]
    else:
        raise ValueError("unrecognized natural-person position amounts")
    # First effective date in clause II applies to TX, including amendments
    # where TX and TXO share a trailing date. Never take a date from clause III.
    m = re.search("自" + ROC_DATE + "一般交易時段起生效", clause)
    if m is None:
        raise ValueError("missing TX position effective opening")
    return dict(before=int(before.replace(",", "")), after=int(after.replace(",", "")),
                effective_at=timestamp(roc_date(m), "08:45:00"))


def validate_event_chain(events: list[dict], fields: tuple[str, ...]) -> list[dict]:
    unique = {}
    for event in events:
        key = event["effective_at"]
        if event["known_at"] > key:
            raise ValueError("event was not conservatively public before taking effect")
        if key in unique:
            if any(unique[key][name] != event[name] for name in fields):
                raise ValueError("conflicting simultaneous rule events")
        else:
            unique[key] = event
    ordered = sorted(unique.values(), key=lambda e: e["effective_at"])
    if not ordered:
        raise ValueError("empty rule event chain")
    for left, right in zip(ordered, ordered[1:]):
        if right.get("absolute_level_notice") and right["before"] is None:
            # Old official notices sometimes state a complete replacement level
            # without quoting the previous amount. Do not invent a quoted cell.
            continue
        if left["after"] != right["before"]:
            raise ValueError(f"missing rule transition: {left['effective_at']} -> {right['effective_at']}")
    return ordered


class RuleArchive:
    def __init__(self, root: Path, bundle: Path, reviews: Path | None = None):
        self.root, self.bundle = root, bundle
        self.conn = sqlite3.connect(f"file:{root / 'state/queue.sqlite3'}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("BEGIN")
        self.sources: dict[str, dict] = {}
        self.cache: dict[str, dict] = {}
        self.reviews = {}
        self.legacy = None
        if reviews is not None:
            review_data = json.loads(reviews.read_text())
            if review_data["schema_version"] != 1:
                raise ValueError("unsupported visual review schema")
            self.reviews = {r["url"]: r for r in review_data["reviews"]}
            self.legacy = review_data.get("legacy")
            self.copy(reviews, sha256_file(reviews), url="", kind="visual_transcription_review")
            for source in review_data.get("supplemental_sources", []):
                path = Path(source["path"])
                if hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != source["content_sha256"]:
                    raise ValueError("supplemental official content hash mismatch")
                self.copy(path, source["sha256"], url=source["url"], kind="supplemental_official_raw_gzip")
            self.supplemental = {s["url"]: s for s in review_data.get("supplemental_sources", [])}

    def copy(self, path: Path, digest: str, *, url: str, kind: str) -> str:
        if sha256_file(path) != digest:
            raise ValueError(f"archive source hash mismatch: {path}")
        relative = Path("sources") / (digest + "".join(path.suffixes))
        dest = self.bundle / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            shutil.copyfile(path, dest)
        if sha256_file(dest) != digest:
            raise ValueError("copied source hash mismatch")
        self.sources[str(relative)] = dict(path=str(relative), sha256=digest, url=url, kind=kind)
        return str(relative)

    def document(self, url: str) -> dict:
        if url in self.cache:
            return self.cache[url]
        row = self.conn.execute("SELECT * FROM documents WHERE url=?", (url,)).fetchone()
        if row is None or not row["parsed_path"]:
            raise ValueError(f"uncaptured rule document: {url}")
        row = dict(row)
        raw = read_verified_raw(self.root, row)
        version = self.conn.execute(
            "SELECT * FROM parse_versions WHERE url=? AND content_sha256=? AND parser_version=?",
            (url, row["content_sha256"], row["parser_version"])).fetchone()
        if version is None:
            raise ValueError("missing immutable parse version")
        path = self.root / version["parsed_path"]
        if sha256_file(path) != version["parsed_sha256"]:
            raise ValueError("parsed rule evidence hash mismatch")
        parsed = json.loads(path.read_text())
        if parsed["content_sha256"] != hashlib.sha256(raw).hexdigest():
            raise ValueError("parsed/raw document mismatch")
        if url in self.reviews:
            review = self.reviews[url]
            if review["content_sha256"] != parsed["content_sha256"]:
                raise ValueError("visual transcription applies to different PDF bytes")
            parsed = dict(parsed, text=parsed["text"] + "\n" + review["clock_text"])
        # Preserve exact raw and interpreted source bytes in the small rule bundle.
        self.copy(self.root / row["raw_path"], row["raw_sha256"], url=url, kind="raw_gzip")
        self.copy(path, version["parsed_sha256"], url=url, kind="parsed_v2")
        self.cache[url] = parsed
        return parsed

    def children(self, url: str) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT child FROM links WHERE parent=?", (url,))]


def reviewed_legacy_events(archive: RuleArchive, start: date) -> tuple[dict, list, dict]:
    """Accept explicit, byte-bound transcriptions, never unchecked OCR output."""
    margins, positions, notices = {p: [] for p in PRODUCTS}, [], {}
    scope = archive.legacy
    if scope is None:
        return margins, positions, notices
    if start < date.fromisoformat(scope["verified_start"]):
        raise ValueError("requested start predates the reviewed historical rule scope")
    for review in scope["notices"]:
        ann = archive.conn.execute("SELECT * FROM announcements WHERE url=? AND published_date=?",
            (review["announcement_url"], review["published_date"])).fetchone()
        if ann is None or review["url"] not in [ann["url"], *archive.children(ann["url"])]:
            raise ValueError("legacy source is not bound to its announcement")
        doc = archive.document(review["url"])
        if doc["content_sha256"] != review["content_sha256"] or not review["pages_one_based"]:
            raise ValueError("legacy transcription applies to different source bytes or lacks pages")
        if review["announcement_url"] in notices:
            raise ValueError("duplicate legacy notice review")
        notices[review["announcement_url"]] = review
        if review["kind"] == "excluded":
            if not review["reason"]:
                raise ValueError("legacy exclusion lacks a reviewed reason")
            continue
        published = date.fromisoformat(review["published_date"])
        issued = date.fromisoformat(review["issued_date"])
        if not 0 <= (published-issued).days <= 7:
            raise ValueError("legacy issuing date disagrees with index")
        known = timestamp(published, "23:59:59")
        proof = review.get("publication_date_confirmation")
        if proof:
            source = archive.supplemental.get(proof["url"])
            if source is None or source["content_sha256"] != proof["content_sha256"] or not proof["pages_one_based"]:
                raise ValueError("missing independent official publication-date confirmation")
            known = timestamp(issued, "23:59:59")
        event = dict(known_at=known, published_date=str(published), source_url=review["url"],
                     content_sha256=review["content_sha256"], pages_one_based=review["pages_one_based"])
        effective_day = date.fromisoformat(review["effective_date"])
        if review["kind"] == "margin":
            if review["currency"] != "TWD" or review["column_order"] != ["after", "before"]:
                raise ValueError("unverified legacy currency or before/after order")
            if set(review["values"]) != set(PRODUCTS):
                raise ValueError("TX and MTX must each have independently transcribed cells")
            for product, six in review["values"].items():
                if len(six) != 6 or any(type(v) is not int or v <= 0 for v in six):
                    raise ValueError("legacy margin requires six positive integer cells")
                if not (six[0] >= six[1] >= six[2] and six[3] >= six[4] >= six[5]):
                    raise ValueError("legacy margin column order is inconsistent")
                margins[product].append(dict(event, product=product, after=six[:3], before=six[3:],
                    effective_at=timestamp(effective_day, "13:45:00")))
        elif review["kind"] == "position":
            after = review["after"]
            if type(after) is not int or after <= 0:
                raise ValueError("invalid reviewed position limit")
            exchange = timestamp(effective_day, "00:00:00" if review["immediate"] else "08:45:00")
            positions.append(dict(event, before=review.get("before"), after=after, absolute_level_notice=True,
                exchange_effective_at=exchange, effective_at=max(exchange, known),
                delayed_relaxation=known > exchange))
        else:
            raise ValueError("unknown reviewed legacy notice kind")
    positions.sort(key=lambda e: e["effective_at"])
    for i, event in enumerate(positions):
        if event["delayed_relaxation"] and (i == 0 or event["after"] < positions[i-1]["after"]):
            raise ValueError("late-known position tightening cannot be deferred")
    return margins, positions, notices


def build_rule_events(archive: RuleArchive, start: date, end: date) -> tuple[dict, list, list]:
    margins, positions, reviewed = reviewed_legacy_events(archive, start)
    audit = []
    floor = archive.legacy["index_start"] if archive.legacy else "2021-04-01"
    if start < date(2021, 5, 20) and not archive.legacy:
        raise ValueError("earlier scope requires byte-bound legacy source reviews")
    announcements = [dict(r) for r in archive.conn.execute(
        "SELECT * FROM announcements WHERE published_date>=? AND published_date<=? ORDER BY published_date,url",
        (floor, str(end)))]
    # Index captures establish the bounded announcement inventory, independently
    # of the presence of numeric tables or current queue completeness.
    for row in archive.conn.execute("SELECT * FROM index_windows WHERE end>=? AND start<=?",
                                    (floor, str(end))):
        archive.copy(archive.root / row["raw_path"], row["raw_sha256"],
                     url="https://www.taifex.com.tw/cht/11/hisNews", kind="announcement_index")
    for ann in announcements:
        title, url, pub = ann["title"], ann["url"], ann["published_date"]
        if url in reviewed:
            review = reviewed[url]
            audit.append(dict(url=url, published_date=pub, kind=review["kind"],
                status="hash_bound_visual_review", reason=review.get("reason")))
            continue
        is_position = "部位限制" in title and "臺股" in title
        is_margin = "保證金" in title and "抵繳" not in title
        if not (is_position or is_margin):
            continue
        relevant_title = any(x in compact(title) for x in ("臺股期貨", "台股期貨", "小型臺指", "小型台指", "(TX)", "(MTX)"))
        children = archive.children(url)
        csv_urls = [u for u in children if u.lower().endswith(".csv")]
        if not relevant_title and not csv_urls and not is_position:
            continue
        if archive.legacy and pub < archive.legacy["until_exclusive"]:
            if relevant_title or is_position:
                # A separately indexed press release may duplicate a bound
                # primary notice, but it may not silently introduce a new date.
                same = [r for r in reviewed.values() if r["published_date"] == pub
                        and r["kind"] == ("position" if is_position else "margin")]
                if not same:
                    raise ValueError(f"unreviewed historical TX/MTX notice: {url}")
                audit.append(dict(url=url, published_date=pub, kind="position" if is_position else "margin",
                                  status="same_day_notice_duplicate_not_used"))
            continue
        known = timestamp(date.fromisoformat(pub), "23:59:59")
        if is_position:
            urls = [url] if url.lower().endswith(".pdf") else [u for u in children if u.lower().endswith(".pdf")]
            candidates = []
            for u in urls:
                doc = archive.document(u)
                if "「臺股期貨」" in compact(doc["text"]) and "發文日期" in compact(doc["text"]):
                    candidates.append((u, doc))
            if len(candidates) != 1:
                raise ValueError(f"position attachment not uniquely identified: {url}")
            u, doc = candidates[0]
            check_publication(doc["text"], pub)
            change = position_change(doc["text"])
            audit.append(dict(url=url, published_date=pub, kind="position", status="natural_change" if change else "legal_person_only"))
            if change:
                positions.append(dict(**change, known_at=known, published_date=pub, source_url=u))
            continue
        found = False
        for csv_url in csv_urls:
            state = archive.conn.execute("SELECT parsed_path FROM documents WHERE url=?", (csv_url,)).fetchone()
            if (state is None or not state[0]) and not relevant_title:
                audit.append(dict(url=url, published_date=pub, kind="margin",
                                  status="out_of_scope_notice_missing_attachment"))
                continue
            csv_doc = archive.document(csv_url)
            rows = [r for table in csv_doc["tables"] for r in table["rows"]]
            products = [p for p in PRODUCTS if any(r.get("契約代碼") == p for r in rows)]
            if not products:
                continue
            html = archive.document(url)
            effective = margin_effective(html["text"])
            # First explicit new level, never extend the 'before' value backwards
            # beyond an observed announcement to lengthen the sample.
            if not archive.legacy and effective < timestamp(start - timedelta(days=1), "00:00:00"):
                continue
            pdfs = [u for u in children if u.lower().endswith(".pdf")]
            matched = []
            errors = []
            for pdf_url in pdfs:
                doc = archive.document(pdf_url)
                try:
                    check_publication(doc["text"], pub)
                    if margin_effective(doc["text"]) != effective:
                        raise ValueError("HTML/PDF effective date mismatch")
                    values = {p: verified_margin_values(rows, doc["text"], p) for p in products}
                except ValueError as error:
                    errors.append(f"{pdf_url}: {error}")
                    continue
                matched.append((pdf_url, values))
            if len(matched) != 1:
                if not relevant_title and not matched:
                    audit.append(dict(url=url, published_date=pub, kind="margin",
                                      status="out_of_scope_csv_not_bound_to_notice"))
                    continue
                raise ValueError(f"CSV/PDF binding failed: {url}; {errors}")
            pdf_url, values = matched[0]
            for p, six in values.items():
                margins[p].append(dict(product=p, after=list(six[:3]), before=list(six[3:]),
                    effective_at=effective, known_at=known, published_date=pub,
                    source_url=url, csv_url=csv_url, pdf_url=pdf_url))
            found = True
        if relevant_title and timestamp(date.fromisoformat(pub), "23:59:59") >= timestamp(start - timedelta(days=2), "00:00:00"):
            audit.append(dict(url=url, published_date=pub, kind="margin", status="bound_csv_pdf" if found else "requires_duplicate_check"))
    for p in PRODUCTS:
        margins[p] = validate_event_chain(margins[p], ("after", "before"))
    positions = validate_event_chain(positions, ("after", "before"))
    covered_dates = {e["published_date"] for events in margins.values() for e in events}
    for row in audit:
        if row["status"] == "requires_duplicate_check":
            if row["published_date"] not in covered_dates:
                raise ValueError(f"unresolved TX/MTX announcement: {row['url']}")
            # A press-release duplicate is not authority for a numeric event.
            row["status"] = "same_day_notice_duplicate_not_used"
    return margins, positions, audit


def prepare_daily(daily_path: Path, evidence_path: Path, final_path: Path,
                  output: Path, start: date, end: date) -> tuple[pl.DataFrame, dict]:
    """Repair marks by physical identity, keeping the existing fixed-slot ABI."""
    original_manifest = json.loads(daily_path.with_name("manifest.json").read_text())
    parent_sha = sha256_file(daily_path)
    if original_manifest["outputs"]["continuous_daily"]["sha256"] != parent_sha:
        raise ValueError("daily parent release hash mismatch")
    proof_manifest = json.loads(evidence_path.with_name("official_evidence_manifest.json").read_text())
    if proof_manifest["sha256"] != sha256_file(evidence_path):
        raise ValueError("official daily proof hash mismatch")
    # The canonical evidence loader re-verifies the original archive sources.
    from stockagent.data.tw_stock_futures_carry import _load_carry_official_evidence
    evidence = _load_carry_official_evidence(evidence_path)
    hashes = {item["sha256"] for item in proof_manifest["sources"]}
    if not set(evidence["official_source_sha256"].drop_nulls()) <= hashes:
        raise ValueError("daily settlement row references an unverified official archive")
    final_manifest = json.loads(final_path.with_name("manifest.json").read_text())
    if final_manifest["outputs"]["futures_final_settlement_history"]["sha256"] != sha256_file(final_path):
        raise ValueError("final settlement source hash mismatch")
    raw = pl.read_parquet(daily_path)
    original_columns = raw.columns
    frame = raw.filter(pl.col("product").is_in(PRODUCTS) & pl.col("contract").str.contains(r"^\d{6}$")
                       & (pl.col("date") >= evidence["date"].min()) & (pl.col("date") <= end))
    frame = frame.join(evidence.select("date", "physical_contract", "official_settlement", "official_source_sha256"),
                       on=["date", "physical_contract"], how="left", validate="1:1")
    final = pl.read_parquet(final_path).select(
        pl.col("settlement_date").alias("date"), "product", "contract",
        pl.col("final_settlement_price").alias("_official_final"))
    frame = frame.join(final, on=["date", "product", "contract"], how="left", validate="m:1")
    frame = frame.with_columns(
        pl.when(pl.col("liquidation_reason") == "last_trade_date")
        .then(pl.col("_official_final")).otherwise(pl.col("official_settlement").cast(pl.Float64, strict=False))
        .alias("_verified_settlement"))
    # A file boundary is not a broker liquidation instruction. Keep the open
    # account marked at its final observed official settlement, with residual
    # contracts auditable. Otherwise a capacity-limited, solvent position is
    # spuriously assigned the executor's absorbing failure return at EOF.
    boundary = pl.col("liquidation_reason") == "dataset_terminal"
    legal_expiry = pl.col("date") == pl.col("resolved_last_trade_date")
    if frame.filter(boundary & (pl.col("date") != end)).height:
        raise ValueError("source terminal boundary does not match selected end")
    frame = frame.with_columns(
        pl.when(boundary & legal_expiry).then(pl.lit("last_trade_date"))
        .when(boundary).then(pl.lit("dataset_boundary_mark_only"))
        .otherwise(pl.col("liquidation_reason")).alias("liquidation_reason"),
        pl.when(boundary).then(legal_expiry).otherwise(pl.col("must_liquidate")).alias("must_liquidate"),
        pl.when(boundary & legal_expiry).then(pl.col("_official_final"))
        .otherwise(pl.col("_verified_settlement")).alias("_verified_settlement"))
    if frame.filter(pl.col("_verified_settlement").is_null() | ~pl.col("_verified_settlement").is_finite()
                    | (pl.col("_verified_settlement") <= 0)).height:
        raise ValueError("missing same-day official daily/final settlement")
    repaired = frame.filter(pl.col("settlement") != pl.col("_verified_settlement")).height
    frame = frame.with_columns(pl.col("_verified_settlement").alias("settlement")).sort("physical_contract", "date")
    frame = frame.with_columns(*[
        pl.col(column).shift(1).over("physical_contract").alias(name)
        for column, name in (("settlement", "previous_settlement"), ("date", "previous_symbol_date"),
                             ("product", "previous_product"), ("contract", "previous_contract"),
                             ("volume", "previous_volume"), ("open_interest", "previous_open_interest"))])
    same = (pl.col("previous_symbol_date") == pl.col("previous_market_date")).fill_null(False)
    # On a proven no-print day the previous official settlement is an opening
    # valuation only, with executable=False and zero capacity. Never a fill.
    frame = frame.with_columns(
        pl.when(~pl.col("source_row_observed")).then(pl.col("previous_settlement"))
        .otherwise(pl.col("open")).alias("open"),
        pl.when(~pl.col("source_row_observed")).then(pl.col("settlement"))
        .otherwise(pl.col("close")).alias("close"),
        same.alias("same_contract_as_previous_session"), (~same).alias("lifecycle_reset"),
        pl.when(same).then((pl.col("settlement") / pl.col("previous_settlement")).log())
        .otherwise(None).alias("taifex_settlement_logret_1d"))
    frame = frame.with_columns(
        pl.col("open").shift(-1).over("physical_contract").alias("next_open"),
        pl.col("settlement").alias("valuation_settlement"), pl.col("open").alias("valuation_open"))
    frame = frame.with_columns(pl.when(pl.col("can_hold_overnight"))
        .then((pl.col("next_open") / pl.col("open")).log())
        .otherwise((pl.col("close") / pl.col("open")).log()).alias("holding_log_return"))
    scoped = frame.filter(pl.col("date") >= start)
    excluded = scoped.filter(~pl.col("same_contract_as_previous_session"))
    # Only first physical observations may be excluded. An intermediate hole
    # would strand held positions and must fail rather than shorten a trajectory.
    if excluded.filter(pl.col("previous_symbol_date").is_not_null()).height:
        raise ValueError("intermediate physical-contract calendar gap")
    scoped = scoped.filter(pl.col("same_contract_as_previous_session"))
    if scoped.filter((~pl.col("source_row_observed")) &
                     (pl.col("executable") | (pl.col("volume") > 0))).height:
        raise ValueError("a no-print valuation became an executable quote")
    if scoped.select("date", "symbol").is_duplicated().any():
        raise ValueError("physical scope collides on a fixed output slot")
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(output / "excluded_first_observations.parquet",
                         excluded.select("date", "physical_contract", "previous_symbol_date"))
    outpath = output / "continuous_daily.parquet"
    scoped = scoped.select(original_columns).sort("date", "symbol")
    atomic_write_parquet(outpath, scoped)
    summary = dict(products=list(PRODUCTS), contract_series="monthly_only", start=str(scoped["date"].min()),
        end=str(scoped["date"].max()), rows=scoped.height, physical_contracts=scoped["physical_contract"].n_unique(),
        trading_dates=scoped["date"].n_unique(), excluded_first_observations=excluded.height,
        official_settlement_replacements=repaired, source_daily_sha256=parent_sha,
        no_print_valuation_days=scoped.filter(~pl.col("source_row_observed")).height,
        snapshot_boundary_mark_only_rows=scoped.filter(pl.col("liquidation_reason") == "dataset_boundary_mark_only").height,
        selection_reason="verified_historical_rules_and_prior_same_physical_official_settlement_not_performance")
    manifest = dict(original_manifest, verified_margin_scope=summary,
        outputs={"continuous_daily": {"path": str(outpath), "sha256": sha256_file(outpath), "rows": scoped.height}},
        parent_daily_sha256=parent_sha, official_daily_evidence_sha256=sha256_file(evidence_path),
        official_final_settlement_sha256=sha256_file(final_path))
    manifest.update(scope="verified_TX_MTX_monthly_margin_research", rows=scoped.height,
        products=2, fixed_fee_research_products=2, date_start=summary["start"], date_end=summary["end"],
        observed_transaction_rows=scoped.filter(pl.col("source_row_observed")).height,
        carry_forward_valuation_rows=summary["no_print_valuation_days"], weekly_observed_rows=0,
        logical_symbols=scoped["symbol"].n_unique(),
        maximum_simultaneous_active_contracts=scoped.group_by("date").len()["len"].max(),
        maximum_simultaneous_executable_contracts=scoped.filter(pl.col("executable")).group_by("date").len()["len"].max(),
        execution_contract="prior_completed_features_regular_open_adjustments_official_daily_and_final_settlement_marks",
        fixed_fee_contract="whole_contract_margin_account_fee_40_TWD_per_side_plus_dated_transaction_tax")
    manifest["mandatory_liquidation_boundary"] = "legal_expiry_and_account_risk_only_dataset_EOF_is_official_mark_with_open_positions"
    atomic_write_json(output / "manifest.json", manifest)
    exclusions = raw.group_by("product").agg(pl.len().alias("available_contract_days"),
        pl.col("date").min().alias("available_start"), pl.col("date").max().alias("available_end"))
    exclusions = exclusions.join(scoped.group_by("product").agg(pl.len().alias("selected_contract_days")),
                                  on="product", how="left").with_columns(pl.col("selected_contract_days").fill_null(0))
    exclusions.sort("product").write_csv(output / "all_product_scope.csv")
    return scoped, summary


def event_at(events: list[dict], cutoff: str) -> dict:
    eligible = [e for e in events if e["known_at"] <= cutoff and e["effective_at"] <= cutoff]
    if not eligible:
        raise ValueError(f"no prior public effective rule at {cutoff}")
    return max(eligible, key=lambda e: e["effective_at"])


def align_rules(frame: pl.DataFrame, margins: dict, positions: list) -> pl.DataFrame:
    from stockagent.data.tw_price_rules import taifex_futures_tick_size_numpy, dated_limit_ratio
    prior = frame["previous_settlement"].to_numpy()
    ticks = taifex_futures_tick_size_numpy(prior, frame["date"].to_numpy(),
        product_codes=frame["product"].to_numpy(), asset_classes=frame["asset_class"].to_numpy())
    if not np.isfinite(prior).all() or not (prior > 0).all() or not (ticks == 1).all():
        raise ValueError("unverified prior reference or TX/MTX dated price grid")
    # Use the historical interval, then retain executable grid points inside it.
    upper = np.floor(prior * dated_limit_ratio(1.10, frame["date"].to_numpy(), prior.shape) / ticks + 1e-8) * ticks
    lower = np.ceil(prior * dated_limit_ratio(.90, frame["date"].to_numpy(), prior.shape) / ticks - 1e-8) * ticks
    rows = []
    for i, row in enumerate(frame.select("date", "physical_contract", "product", "liquidation_reason").to_dicts()):
        day, product = row["date"], row["product"]
        opening = event_at(margins[product], timestamp(day, "08:45:00"))
        clock = "13:30:00" if row["liquidation_reason"] == "last_trade_date" else "13:45:00"
        ending = event_at(margins[product], timestamp(day, clock))
        pos = event_at(positions, timestamp(day, "08:45:00"))
        rows.append(dict(date=day, physical_contract=row["physical_contract"], margin_kind="fixed_twd",
            initial=opening["after"][0], maintenance=opening["after"][1],
            settlement_initial=ending["after"][0], settlement_maintenance=ending["after"][1],
            known_at=max(opening["known_at"], pos["known_at"]),
            effective_at=max(opening["effective_at"], pos["effective_at"]),
            settlement_known_at=ending["known_at"], settlement_effective_at=ending["effective_at"],
            settlement_time=clock, position_group="TAIEX_NATURAL_PERSON", position_unit=1. if product=="TX" else .25,
            position_limit=pos["after"], upper_limit=upper[i], lower_limit=lower[i],
            opening_margin_source_url=opening["source_url"], settlement_margin_source_url=ending["source_url"],
            position_source_url=pos["source_url"]))
    return pl.DataFrame(rows).sort("date", "physical_contract")
