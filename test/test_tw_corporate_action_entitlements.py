from __future__ import annotations

from argparse import Namespace
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import polars as pl
import pytest

from downloader import download_tw_corporate_action_entitlements as entitlement_downloader
from downloader.download_tw_corporate_action_entitlements import (
    BulkDividendKey,
    DetailKey,
    ListingKey,
    StockDeliveryDetailKey,
    _collapse_bulk_event_rows,
    _mops_throttle_response,
    _record_raw_receipt_request,
    _requested_listing_keys,
    _reset_raw_receipt_requests,
    _verified_reference_receipt,
    _write_content_addressed_receipt_manifest,
    parse_mops_detail,
    parse_mops_bulk_dividends,
    parse_mops_listing,
    parse_mops_stock_delivery_detail,
    parse_mops_stock_delivery_listing,
    parse_etf_distributions,
    _collapse_etf_event_rows,
    _recover_retained_bulk_rows,
)


def test_bulk_loaded_code_fingerprint_matches_source_pyc_and_other_filenames():
    module = entitlement_downloader
    source = Path(module.__file__).read_bytes()
    expected = module._LOADED_MODULE_SHA256
    for filename in (module.__file__, "other-location.py"):
        assert module._loaded_code_identity(compile(source, filename, "exec", dont_inherit=True)) == expected
    changed = source.replace(b"PARSER_CONTRACT_VERSION = 4", b"PARSER_CONTRACT_VERSION = 999")
    assert changed != source
    assert module._loaded_code_identity(compile(changed, "other.py", "exec", dont_inherit=True)) != expected
    program = "from downloader import download_tw_corporate_action_entitlements as m; print(m._LOADED_MODULE_SHA256)"
    # Two independent real pyc imports, not another call on one code object.
    for _ in range(2):
        observed = subprocess.check_output([sys.executable, "-c", program], text=True, timeout=20).strip()
        assert observed == expected


def test_bulk_parse_cache_preserves_scalar_bits_dates_order_and_fresh_rows(tmp_path, monkeypatch):
    module = entitlement_downloader
    rows = [
        {"date": date(2024, 7, 3), "symbol": "2330", "amount": -0.0,
         "unknown": None, "complete": False, "sequence": 0},
        {"date": date(2024, 6, 3), "symbol": "0001", "amount": 0.1,
         "unknown": None, "complete": True, "sequence": 2},
    ]
    calls = []
    monkeypatch.setattr(module, "parse_mops_bulk_dividends",
                        lambda *_a, **_k: calls.append(1) or [row.copy() for row in rows])
    kwargs = dict(key=BulkDividendKey("twse", 113), cache_root=tmp_path)
    assert module._parse_mops_bulk_dividends_cached(b"same source", **kwargs) == rows
    cached = module._parse_mops_bulk_dividends_cached(b"same source", **kwargs)
    assert cached == rows and calls == [1]
    assert type(cached[0]["date"]) is date
    assert math.copysign(1, cached[0]["amount"]) == -1
    assert cached[1]["amount"].hex() == rows[1]["amount"].hex()
    cached[0]["symbol"] = "poison"
    assert module._parse_mops_bulk_dividends_cached(b"same source", **kwargs) == rows


@pytest.mark.parametrize("change", ["body", "market", "year", "code", "dependency"])
def test_bulk_parse_cache_invalidates_actual_input_and_loaded_implementation(tmp_path, monkeypatch, change):
    module = entitlement_downloader
    calls = []
    monkeypatch.setattr(module, "parse_mops_bulk_dividends", lambda *_a, **_k: calls.append(1) or [])
    kwargs = dict(key=BulkDividendKey("twse", 113), cache_root=tmp_path)
    assert module._parse_mops_bulk_dividends_cached(b"one", **kwargs) == []
    body = b"two" if change == "body" else b"one"
    if change == "market":
        kwargs["key"] = BulkDividendKey("tpex", 113)
    if change == "year":
        kwargs["key"] = BulkDividendKey("twse", 114)
    if change == "code":
        monkeypatch.setattr(module, "_LOADED_MODULE_SHA256", "new loaded implementation")
    if change == "dependency":
        monkeypatch.setattr(module.bs4, "__version__", "new parser dependency")
    assert module._parse_mops_bulk_dividends_cached(body, **kwargs) == []
    assert calls == [1, 1]


@pytest.mark.parametrize("damage", ["payload", "receipt", "schema", "rows", "identity",
                                   "parquet", "missing", "symlink", "fifo", "oversized"])
def test_bulk_parse_cache_corruption_is_a_parse_not_a_false_empty_hit(tmp_path, monkeypatch, damage):
    module = entitlement_downloader
    calls = []
    row = {"date": date(2024, 7, 3), "symbol": "2330"}
    monkeypatch.setattr(module, "parse_mops_bulk_dividends", lambda *_a, **_k: calls.append(1) or [row])
    kwargs = dict(key=BulkDividendKey("twse", 113), cache_root=tmp_path)
    assert module._parse_mops_bulk_dividends_cached(b"source", **kwargs) == [row]
    payload = next(tmp_path.glob("*.parquet"))
    receipt = next(tmp_path.glob("*.json"))
    if damage == "payload":
        payload.write_bytes(b"partial")
    elif damage == "receipt":
        receipt.write_bytes(b"[]")
    elif damage in {"schema", "rows", "identity", "parquet"}:
        info = json.loads(receipt.read_bytes())
        if damage == "schema":
            info["schema"] = []
        elif damage == "rows":
            info["rows"] = 999
        elif damage == "identity":
            info["identity"]["loaded_module_sha256"] = "old code"
        else:
            payload.write_bytes(b"not a parquet file")
            info["bytes"] = payload.stat().st_size
            info["sha256"] = hashlib.sha256(payload.read_bytes()).hexdigest()
        receipt.write_text(json.dumps(info))
    elif damage == "oversized":
        monkeypatch.setattr(module, "_BULK_PARSE_CACHE_MAX_BYTES", 1)
    else:
        original = payload.read_bytes()
        payload.unlink()
        if damage == "symlink":
            target = tmp_path / "unchanged.target"
            target.write_bytes(original)
            payload.symlink_to(target)
        elif damage == "fifo":
            os.mkfifo(payload)
    assert module._parse_mops_bulk_dividends_cached(b"source", **kwargs) == [row]
    assert calls == [1, 1]
    if damage == "symlink":
        assert target.read_bytes() == original


def test_bulk_parse_cache_failure_and_unwritable_cache_do_not_change_source_result(tmp_path, monkeypatch):
    module = entitlement_downloader
    kwargs = dict(key=BulkDividendKey("twse", 113), cache_root=tmp_path)
    for content in (b"\xff", b"Overrun - Too many query requests", b"<html>unknown</html>"):
        with pytest.raises(ValueError):
            module._parse_mops_bulk_dividends_cached(content, **kwargs)
    assert list(tmp_path.iterdir()) == []
    monkeypatch.setattr(module, "atomic_write_bytes", lambda *_a, **_k: (_ for _ in ()).throw(PermissionError()))
    assert module._parse_mops_bulk_dividends_cached("<html>查無資料</html>".encode(), **kwargs) == []


def test_bulk_parse_cache_receipt_replacement_during_read_forces_reparse(tmp_path, monkeypatch):
    module = entitlement_downloader
    calls = []
    monkeypatch.setattr(module, "parse_mops_bulk_dividends", lambda *_a, **_k: calls.append(1) or [])
    kwargs = dict(key=BulkDividendKey("twse", 113), cache_root=tmp_path)
    module._parse_mops_bulk_dividends_cached(b"source", **kwargs)
    original = module._read_regular_cache_bytes
    reads = []
    def replaced(path, **options):
        content = original(path, **options)
        if path.suffix == ".json":
            reads.append(1)
            if len(reads) == 2:
                return b"replacement"
        return content
    monkeypatch.setattr(module, "_read_regular_cache_bytes", replaced)
    assert module._parse_mops_bulk_dividends_cached(b"source", **kwargs) == []
    assert calls == [1, 1]


def test_bulk_fetch_keeps_original_request_receipt_when_parse_cache_hits(tmp_path, monkeypatch):
    module = entitlement_downloader
    requests, parses = [], []
    monkeypatch.setattr(module, "_cached_or_post", lambda *_a, **_k: requests.append(1) or b"body")
    monkeypatch.setattr(module, "parse_mops_bulk_dividends", lambda *_a, **_k: parses.append(1) or [])
    raw = tmp_path / "raw/tw_corporate_action_entitlements"
    args = Namespace(mode="daily", end_date="2026-10-15", timeout=1, retries=0)
    for _ in range(2):
        assert module._fetch_bulk_dividends(raw, BulkDividendKey("twse", 113), args) == []
    assert requests == [1, 1] and parses == [1]


def test_bulk_parse_benchmark_rejects_changes_to_other_existing_functions():
    from scripts.benchmark_corporate_action_bulk_cache import validate_source_pair
    reference = "def parse_mops_bulk_dividends():\n return 1\ndef _fetch_bulk_dividends():\n return 1\n"
    candidate = reference.replace("def _fetch_bulk_dividends():\n return 1", "def _fetch_bulk_dividends():\n return 2")
    validate_source_pair(reference, candidate)
    with pytest.raises(ValueError, match="unrelated parser/workflow change"):
        validate_source_pair(reference, candidate.replace("return 1", "return 3"))


def test_stock_delivery_progress_reports_elapsed_rate_and_eta(capsys) -> None:
    entitlement_downloader._print_stock_delivery_progress(
        "listings", 100, 200, 2, 10.0
    )
    assert capsys.readouterr().out == (
        "[corporate-action] stock delivery listings 100/200 "
        "failures=2 elapsed=10.0s rate=10.00/s eta=10.0s\n"
    )


def test_stock_delivery_progress_handles_zero_elapsed(capsys) -> None:
    entitlement_downloader._print_stock_delivery_progress(
        "details", 0, 0, 0, 0.0
    )
    assert "rate=0.00/s eta=unknown" in capsys.readouterr().out


def test_delivery_disclosures_do_not_query_unobservable_future_year(monkeypatch, tmp_path):
    from datetime import datetime
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat("2026-10-08T14:30:00+08:00")
    monkeypatch.setattr(entitlement_downloader, "datetime", Clock)
    keys = []
    monkeypatch.setattr(entitlement_downloader, "_fetch_stock_delivery_listing_with_market_fallback", lambda _r,key,_a: keys.append(key) or [])
    rows = [{"market":"twse","symbol":"2330","date":date(year,9,1),"stock_dividend_ratio":0.1,"stock_terms_complete":True,"subscription_ratio":0.0} for year in (2025,2026)]
    result, failures, counts, limited = entitlement_downloader._attach_exact_stock_delivery_dates(rows,raw_root=tmp_path,args=Namespace(max_list_requests=0,max_detail_requests=0),workers=1)
    assert {key.roc_year for key in keys} == {114,115}
    assert counts["stock_delivery_disclosure_through_year"] == 2026
    assert all(row["stock_delivery_date"] is None for row in result)
    assert not failures and not limited


def test_entitlement_commit_fence_rejects_reference_or_universe_mutation(monkeypatch, tmp_path):
    reference = tmp_path / "reference.parquet"
    universe = tmp_path / "universe.csv"
    reference.write_bytes(b"reference-v1")
    universe.write_bytes(b"universe-v1")
    ref_receipt = entitlement_downloader._file_receipt(reference)
    uni_receipt = entitlement_downloader._file_receipt(universe)
    monkeypatch.setattr(entitlement_downloader,"_verified_reference_receipt",entitlement_downloader._file_receipt)
    check = entitlement_downloader._entitlement_input_change_errors
    assert check(ref_receipt,uni_receipt) == []
    reference.write_bytes(b"reference-v2")  # equal byte count still changes SHA
    errors = check(ref_receipt,uni_receipt)
    assert len(errors) == 1 and errors[0]["key"] == "reference"
    assert errors[0]["stage"] == "input_dependency_changed"
    reference.write_bytes(b"reference-v1")
    universe.write_bytes(b"universe-v2")
    errors = check(ref_receipt,uni_receipt)
    assert len(errors) == 1 and errors[0]["key"] == "universe"


@pytest.mark.parametrize("changed_input", ["reference", "universe", None])
def test_collector_preserves_accepted_output_if_input_changes_during_build(
    monkeypatch, tmp_path, changed_input,
):
    module = entitlement_downloader
    reference_path = tmp_path / "reference.parquet"
    universe_path = tmp_path / "universe.csv"
    reference_path.write_bytes(b"reference-v1")
    universe_path.write_bytes(b"universe-v1")
    reference = pl.DataFrame({
        "date": [date(2026, 10, 8)], "symbol": ["2330"],
        "market": ["twse"], "event_type": ["息"], "reference_price": [990.0],
    })
    universe = pl.DataFrame({
        "symbol": ["2330"], "security_type": ["stock"], "universe_market": ["twse"],
    })
    reference_receipt = module._file_receipt(reference_path)
    universe_receipt = module._file_receipt(universe_path)
    output = tmp_path / "tw_corporate_action_entitlements.parquet"
    summary_path = output.with_suffix(".summary.json")
    pl.DataFrame({"accepted_generation": [1]}).write_parquet(output)
    summary_path.write_text('{"accepted_generation": 1}')
    before = (output.read_bytes(), summary_path.read_bytes())
    monkeypatch.setattr(module, "_configure_tw_public_rate_limiter", lambda _: 0.0)
    monkeypatch.setattr(module, "_verified_reference_receipt", module._file_receipt)
    monkeypatch.setattr(module, "_load_reference", lambda _: (
        reference, universe, reference_receipt, universe_receipt,
    ))
    monkeypatch.setattr(module, "_requested_bulk_dividend_keys", lambda **_: [])
    monkeypatch.setattr(module, "_recover_retained_bulk_rows", lambda *_, **__: [])
    def finish_source_stage(rows, **_):
        if changed_input is not None:
            target = reference_path if changed_input == "reference" else universe_path
            target.write_bytes(target.read_bytes().replace(b"v1", b"v2"))
        return rows, [], {}, False
    monkeypatch.setattr(module, "_attach_exact_stock_delivery_dates", finish_source_stage)
    args = Namespace(
        output_dir=tmp_path, request_interval=0.0, start_date="2026-10-08",
        end_date="2026-10-08", max_list_requests=0, max_detail_requests=0,
        workers=1, retained_source_dir=[], mode="history",
    )
    if changed_input is not None:
        with pytest.raises(SystemExit) as caught:
            module._run(args)
        assert caught.value.code == 2
        assert (output.read_bytes(), summary_path.read_bytes()) == before
        attempt = json.loads(output.with_suffix(".attempt.summary.json").read_text())
        assert not attempt["coverage_complete"]
        assert attempt["failure_count"] == 1
        assert attempt["failures"][0]["key"] == changed_input
        assert attempt["failures"][0]["stage"] == "input_dependency_changed"
        assert attempt["output_receipt"] is None
    else:
        module._run(args)
        accepted = json.loads(summary_path.read_text())
        assert accepted["coverage_complete"] and accepted["failure_count"] == 0
        assert accepted["reference_receipt"] == reference_receipt
        assert pl.read_parquet(output)["handling"].to_list() == ["avoid"]


def test_stock_delivery_stage_timing_keeps_exact_event(monkeypatch, tmp_path) -> None:
    detail_key = StockDeliveryDetailKey(
        market="twse",
        symbol="2330",
        roc_year=115,
        announcement_date=date(2026, 9, 1),
        sequence=1,
        subject="股票發放公告",
    )
    monkeypatch.setattr(
        entitlement_downloader,
        "_fetch_stock_delivery_listing_with_market_fallback",
        lambda _root, _key, _args: [detail_key],
    )
    monkeypatch.setattr(
        entitlement_downloader,
        "_fetch_stock_delivery_detail",
        lambda _root, _key, _args: {
            "key": detail_key,
            "record_date": date(2026, 9, 2),
            "stock_ratio": 0.1,
            "issue_shares": 1000,
            "delivery_date": date(2026, 9, 20),
        },
    )
    rows, failures, counts, limited = (
        entitlement_downloader._attach_exact_stock_delivery_dates(
            [{
                "market": "twse", "symbol": "2330", "date": date(2026, 9, 10),
                "record_date": date(2026, 9, 2), "stock_dividend_ratio": 0.1,
                "stock_terms_complete": True, "subscription_ratio": 0.0,
            }],
            raw_root=tmp_path,
            args=Namespace(max_list_requests=0, max_detail_requests=0),
            workers=1,
        )
    )
    assert not failures and not limited
    assert rows[0]["stock_delivery_date"] == date(2026, 9, 20)
    assert counts["resolved_stock_delivery_events"] == 1
    assert counts["stock_delivery_listing_elapsed_seconds"] >= 0
    assert counts["stock_delivery_detail_elapsed_seconds"] >= 0


def test_tpex_etf_exact_distribution_not_price_adjustment():
    raw = [{"stockNo": "00755B", "divDate": "115年02月26日", "inDate": "115年03月25日",
            "amount": "0.421", "year": "115", "inBaseDate": "115年03月07日"}]
    rows = parse_etf_distributions(json.dumps(raw).encode(), key=BulkDividendKey("tpex", 115))
    assert rows[0]["cash_dividend_per_share"] == .421
    assert rows[0]["cash_payment_date"] == date(2026, 3, 25)
    assert rows[0]["announcement_date"] is None  # no fabricated publication date


def test_twse_etf_html_source_schema():
    cells = ["00943", "ETF", "115年03月17日", "115年03月23日", "115年04月10日", "0.19", "details", "115"]
    html = ('<table id="myTable"><thead><tr><th>收益分配發放日</th></tr></thead><tbody><tr>'
            + ''.join(f'<td>{v}</td>' for v in cells) + '</tr></tbody></table>')
    row = parse_etf_distributions(html.encode(), key=BulkDividendKey("twse", 115))[0]
    assert row["cash_dividend_per_share"] == .19
    assert row["date"] == date(2026, 3, 17)


@pytest.mark.parametrize("field,value", [("year", "114"), ("amount", "0.1~0.2"),
                                         ("amount", "-1"), ("inDate", "115年01月01日")])
def test_etf_invalid_or_wrong_period_terms_fail_closed(field, value):
    raw = dict(stockNo="00755B", divDate="115年02月26日", inDate="115年03月25日", amount=".421", year="115")
    raw["amount"] = "0.421"
    raw[field] = value
    with pytest.raises(ValueError):
        parse_etf_distributions(json.dumps([raw]).encode(), key=BulkDividendKey("tpex", 115))


def test_etf_unannounced_amount_remains_unknown_and_conflicts_are_rejected():
    raw = dict(stockNo="00755B", divDate="115年02月26日", inDate="115年03月25日", amount="", year="115")
    key = BulkDividendKey("tpex", 115)
    assert parse_etf_distributions(json.dumps([raw]).encode(), key=key)[0]["cash_dividend_per_share"] is None
    for rows in ([raw, raw | {"amount": "0.421"}], [raw | {"amount": "0.421"}, raw]):
        assert parse_etf_distributions(json.dumps(rows).encode(), key=key)[0]["cash_dividend_per_share"] == .421
    with pytest.raises(ValueError, match="conflicting"):
        parse_etf_distributions(json.dumps([raw | {"amount": "0.42"}, raw | {"amount": "0.421"}]).encode(), key=key)


def test_etf_malformed_preliminary_date_remains_unknown_and_audited():
    raw = dict(stockNo="00764B", divDate="190年06月16日", inDate="109年07月14日", amount="", year="109")
    row = parse_etf_distributions(json.dumps([raw]).encode(), key=BulkDividendKey("tpex", 109))[0]
    assert row["date"] == date(2101, 6, 16)  # never guess a corrected date
    assert row["cash_payment_date"] is None
    assert row["source_issue"] == "preliminary_payment_precedes_exdate"


def test_etf_cross_year_final_over_preliminary_and_later_final_revision():
    raw = dict(stockNo="0080", divDate="103年12月12日", inDate="104年01月02日", amount="5.8245", year="103")
    old = parse_etf_distributions(json.dumps([raw]).encode(), key=BulkDividendKey("tpex", 103))[0]
    new = old | {"source_disclosure_year": 104, "cash_dividend_per_share": 6.147}
    for rows in ([old, new], [new, old]):
        assert _collapse_etf_event_rows(rows) == [new]
    assert _collapse_etf_event_rows([new, new | {"source_disclosure_year": 105, "cash_dividend_per_share": None}]) == [new]


def test_disappeared_bulk_event_requires_verified_retained_receipt(tmp_path, monkeypatch):
    from downloader import download_tw_corporate_action_entitlements as module
    root = tmp_path / "raw/tw_corporate_action_entitlements"
    raw = root / "bulk_dividends/tpex-115-asof-20260819-v3.html"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"historical official response")
    module._reset_raw_receipt_requests()
    module._record_raw_receipt_request(raw, url=module.MOPS_BULK_DIVIDEND_URL,
                                      data={"year": "115", "TYPEK": "otc"}, content=raw.read_bytes())
    module._write_content_addressed_receipt_manifest(output_dir=tmp_path, raw_root=root)
    module._reset_raw_receipt_requests()
    row = {"date": date(2026, 6, 17), "symbol": "5371"}
    monkeypatch.setattr(module, "parse_mops_bulk_dividends", lambda *_a, **_k: [row])
    monkeypatch.setattr(module, "_collapse_bulk_event_rows", lambda rows: rows)
    kwargs = dict(output_dir=tmp_path, missing={(row["date"], row["symbol"])}, end=date(2026, 9, 9))
    assert _recover_retained_bulk_rows(root, **kwargs) == [row]
    module._reset_raw_receipt_requests()
    target = tmp_path / "isolated"
    assert _recover_retained_bulk_rows(root, target_output_dir=target, **kwargs) == [row]
    copied = target / raw.relative_to(tmp_path)
    assert copied.read_bytes() == raw.read_bytes()
    proof = module._write_content_addressed_receipt_manifest(
        output_dir=target, raw_root=target / "raw/tw_corporate_action_entitlements")
    assert proof["entries"] == 1
    module._reset_raw_receipt_requests()
    raw.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="receipt changed"):
        _recover_retained_bulk_rows(root, **kwargs)


def test_mops_http_200_throttle_body_is_not_an_official_receipt() -> None:
    assert _mops_throttle_response(
        "Overrun - 查詢過於頻繁,請稍後再試!! Too many query requests".encode(
            "utf-8"
        )
    )
    assert not _mops_throttle_response(b"<html>issuer disclosure</html>")


@pytest.mark.parametrize("denial", [
    b"<html>FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED.</html>",
    "因為安全性考量，您所執行的頁面無法呈現。".encode("utf-8"),
    b"Overrun - Too many query requests",
])
@pytest.mark.parametrize("cached", [False, True])
def test_http_200_denial_is_never_a_successful_receipt_and_can_recover(tmp_path, monkeypatch, denial, cached):
    from types import SimpleNamespace
    from downloader import download_tw_corporate_action_entitlements as module

    path = tmp_path / "raw/response.json"
    path.parent.mkdir()
    if cached:
        path.write_bytes(denial)
    calls, registrations = [], []
    body = [denial]

    class Response:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            pass

        def close(self):
            pass

        @property
        def content(self):
            return body[0]

    def get(*args, **kwargs):
        calls.append(1)
        return Response()

    monkeypatch.setattr(module.requests, "get", get)
    monkeypatch.setattr(module, "_global_tw_public_rate_limiter",
        lambda: SimpleNamespace(wait=lambda: None, defer=lambda _: None))
    monkeypatch.setattr(module, "_record_raw_receipt_request",
        lambda *args, **kwargs: registrations.append(kwargs["content"]))
    kwargs = dict(url="https://www.twse.com.tw/test", data={}, method="GET", timeout=1, retries=0)
    with pytest.raises(RuntimeError, match="throttle/access-denial"):
        module._cached_or_post(path, **kwargs)
    assert len(calls) == 1 and registrations == []
    rejected = path.parent / "rejected" / f"{hashlib.sha256(denial).hexdigest()}.response"
    assert rejected.read_bytes() == denial
    if not cached:
        assert not path.exists()
    body[0] = b'{"stat": "OK", "data": []}'
    assert module._cached_or_post(path, **kwargs) == body[0]
    assert path.read_bytes() == body[0] and registrations == [body[0]]
    assert len(calls) == 2  # a cached denial did not prevent a later valid retry


def test_listing_workload_adds_prior_year_only_for_first_quarter() -> None:
    events = pl.DataFrame(
        {
            "market": ["twse", "twse"],
            "symbol": ["2330", "2330"],
            "date": [date(2024, 1, 3), date(2024, 7, 3)],
        }
    )

    assert _requested_listing_keys(events) == [
        ListingKey(market="twse", symbol="2330", roc_year=112),
        ListingKey(market="twse", symbol="2330", roc_year=113),
    ]


def test_parse_mops_bulk_dividend_extracts_exact_cash_terms(monkeypatch) -> None:
    original_soup = entitlement_downloader.BeautifulSoup

    def assert_decoded_markup(markup, *args, **kwargs):
        assert isinstance(markup, str)
        return original_soup(markup, *args, **kwargs)

    monkeypatch.setattr(entitlement_downloader, "BeautifulSoup", assert_decoded_markup)
    cells = [
        "2330",
        "台積電",
        "112年度",
        "113/07/09",
        "",
        "",
        "",
        "10.0",
        "0.5",
        "",
        "113/07/03",
        "113/07/31",
        "0",
        "0",
        "0",
        "5,000,000,000",
        "113/06/20",
        "15:30:00",
        "新台幣10.0000元",
    ]
    html = (
        "<html><table>"
        "<tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        "<tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=113)
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == date(2024, 7, 3)
    assert row["record_date"] == date(2024, 7, 9)
    assert row["cash_dividend_per_share"] == pytest.approx(10.5)
    assert row["cash_payment_date"] == date(2024, 7, 31)
    assert row["stock_dividend_ratio"] == 0.0
    assert row["subscription_ratio"] == 0.0
    assert row["stop_transfer_start"] is None


def test_parse_mops_bulk_dividend_rejects_invalid_utf8() -> None:
    with pytest.raises(UnicodeDecodeError):
        parse_mops_bulk_dividends(
            b"\xff", key=BulkDividendKey(market="tpex", roc_year=115)
        )


def test_mops_stock_dividend_uses_official_par_not_value_as_ratio() -> None:
    cells = [
        "8941",
        "關中",
        "108年度",
        "109/09/02",
        "1.0",
        "0",
        "109/08/27",
        "0",
        "0",
        "",
        "",
        "",
        "0",
        "0",
        "0",
        "27,645,907",
        "109/08/12",
        "08:30:00",
        "新台幣10.0000元",
    ]
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr><tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="tpex", roc_year=109)
    )

    assert len(rows) == 1
    assert rows[0]["date"] == date(2020, 8, 27)
    assert rows[0]["stock_dividend_value_per_share"] == 1.0
    assert rows[0]["stock_par_value"] == 10.0
    assert rows[0]["stock_dividend_ratio"] == pytest.approx(0.1)


def test_mops_t59_stock_issue_and_delivery_are_independent_exact_receipts() -> None:
    listing_html = """
    <html><table>
      <tr><td>109/08/11</td><td>109年分派108年盈餘轉增資發行新股公告</td>
      <td><input onclick='document.fm.DATE1.value="20200811";document.fm.SKEY.value="1";'></td></tr>
      <tr><td>109/10/08</td><td>109年增資發行新股發放暨上櫃買賣日期公告</td>
      <td><input onclick='document.fm.DATE1.value="20201008";document.fm.SKEY.value="1";'></td></tr>
    </table></html>
    """.encode("utf-8")
    listing_key = ListingKey("tpex", "8941", 109)
    keys = parse_mops_stock_delivery_listing(listing_html, key=listing_key)
    assert len(keys) == 2
    assert all(isinstance(key, StockDeliveryDetailKey) for key in keys)

    issue_html = """
    <html>公司代號 8941 公告內容：計發行新股2,764,590股，每股面額新台幣10元。
    按配股基準日股東名簿所載股東持有股數每仟股無償派發100股。
    普通股增資配股基準日：本公司訂於民國109年09月02日。</html>
    """.encode("utf-8")
    delivery_html = """
    <html>公司代號 8941，本次計發行新股2,764,590股。
    本次增資股票訂於109年10月14日(星期三)起發放並上櫃買賣。</html>
    """.encode("utf-8")
    issue = parse_mops_stock_delivery_detail(issue_html, key=keys[0])
    delivery = parse_mops_stock_delivery_detail(delivery_html, key=keys[1])

    assert issue["issue_shares"] == 2_764_590
    assert issue["stock_ratio"] == pytest.approx(0.1)
    assert issue["record_date"] == date(2020, 9, 2)
    assert delivery["issue_shares"] == 2_764_590
    assert delivery["delivery_date"] == date(2020, 10, 14)


def test_mops_t59_stock_ratio_ignores_sentence_dot_before_share_unit() -> None:
    key = StockDeliveryDetailKey(
        market="twse",
        symbol="4306",
        roc_year=103,
        announcement_date=date(2014, 8, 4),
        sequence=1,
        subject="盈餘轉增資發行新股公告",
    )
    content = """
    <html>公司代號 4306，計發行新股46,290,088股。
    普通股增資配股基準日：本公司訂於民國103年08月24日。
    按配股基準日股東名簿所記載持有股份比例每仟股無償配發99.2436.股。
    </html>
    """.encode("utf-8")

    parsed = parse_mops_stock_delivery_detail(content, key=key)

    assert parsed["issue_shares"] == 46_290_088
    assert parsed["stock_ratio"] == pytest.approx(0.0992436)
    assert parsed["record_date"] == date(2014, 8, 24)


@pytest.mark.parametrize(
    ("cells", "expected_cash", "expected_ex_date", "expected_payment"),
    [
        (
            [
                "2330", "台積電", "93年", "94/07/10", "", "",
                "", "", "", "", "", "2.0", "0.25",
                "94/07/04", "94/08/01", "", "", "", "", "",
                "94/06/20", "09:30:00", "新台幣10.0000元",
            ],
            2.25,
            date(2005, 7, 4),
            date(2005, 8, 1),
        ),
        (
            [
                "2330", "台積電", "104年", "105/07/10", "", "",
                "", "3.0", "0.5", "", "105/07/04", "105/08/01",
                "", "", "", "105/06/20", "09:30:00",
                "新台幣10.0000元",
            ],
            3.5,
            date(2016, 7, 4),
            date(2016, 8, 1),
        ),
    ],
)
def test_parse_mops_bulk_dividend_supports_historical_layouts(
    cells: list[str],
    expected_cash: float,
    expected_ex_date: date,
    expected_payment: date,
) -> None:
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr><tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=94)
    )

    assert len(rows) == 1
    assert rows[0]["date"] == expected_ex_date
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(expected_cash)
    assert rows[0]["cash_payment_date"] == expected_payment


def test_parse_mops_bulk_dividend_keeps_latest_official_correction() -> None:
    base = [
        "7610", "聯友金屬-創", "112年", "113/07/19", "", "", "",
        "0.1", "", "", "113/07/11", "113/08/08", "", "", "",
        "30,000,000", "113/06/28", "10:20:25", "新台幣10.0000元",
    ]
    corrected = list(base)
    corrected[7] = "0.2"
    corrected[17] = "11:46:40"
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        + "".join(
            "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
            for row in (base, corrected)
        )
        + "</table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=113)
    )

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(0.2)


def test_parse_mops_bulk_dividend_ignores_old_preferred_pseudo_symbol() -> None:
    common = [
        "8084", "巨虹", "94年", "95/08/29", "", "", "", "", "", "",
        "0", "0.2", "", "95/08/23", "95/09/20", "", "", "", "", "",
        "95/08/08", "16:13:12", "無面額",
    ]
    preferred = list(common)
    preferred[0] = "8084*"
    preferred[1] = "特別股*"
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        + "".join(
            "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
            for row in (common, preferred)
        )
        + "</table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="tpex", roc_year=95)
    )

    assert len(rows) == 1
    assert rows[0]["symbol"] == "8084"


def test_bulk_cross_year_correction_uses_latest_announcement() -> None:
    original = {
        "date": date(2021, 3, 11),
        "symbol": "3680",
        "market": "tpex",
        "announcement_date": date(2020, 12, 25),
        "record_date": date(2021, 3, 17),
        "cash_dividend_per_share": 1.5,
        "cash_payment_date": date(2021, 4, 9),
        "stock_dividend_ratio": 0.0,
        "subscription_ratio": 0.0,
        "stock_terms_complete": True,
    }
    correction = {
        **original,
        "announcement_date": date(2021, 2, 19),
        "cash_dividend_per_share": 1.36147979,
    }

    rows = _collapse_bulk_event_rows([original, correction])

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(1.36147979)


def test_multiple_action_kinds_on_one_ex_date_fail_closed() -> None:
    cash = {
        "date": date(2010, 7, 1),
        "symbol": "6224",
        "market": "twse",
        "announcement_date": date(2010, 6, 15),
        "record_date": date(2010, 7, 9),
        "cash_dividend_per_share": 2.48643,
        "cash_payment_date": None,
        "stock_dividend_ratio": 0.0,
        "subscription_ratio": 0.0,
        "stock_terms_complete": True,
    }
    subscription = {
        **cash,
        "cash_dividend_per_share": 0.0,
        "subscription_ratio": 0.06196397887,
    }

    rows = _collapse_bulk_event_rows([cash, subscription])

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(2.48643)
    assert rows[0]["subscription_ratio"] > 0.0
    assert rows[0]["stock_terms_complete"] is False


def test_parse_mops_listing_deduplicates_detail_identity() -> None:
    onclick = (
        'DATE1.value="20240620";document.t108sb22_fm1.SEQ_NO.value="1";'
        'document.t108sb22_fm1.COMP.value="2330"'
    )
    content = f"<html><button onclick='{onclick}'>detail</button>{onclick}</html>".encode()

    rows = parse_mops_listing(
        content,
        key=ListingKey(market="twse", symbol="2330", roc_year=113),
    )

    assert rows == [
        DetailKey(
            market="twse",
            symbol="2330",
            announcement_date=date(2024, 6, 20),
            sequence=1,
        )
    ]


def test_parse_mops_detail_extracts_exact_cash_payment_terms() -> None:
    html = """
    <html><body>
      <table>
        <tr><td>公司代號</td><td>2330</td></tr>
        <tr><td>四、股票停止過戶起訖日期：</td><td>113年07月05日至113年07月09日</td></tr>
        <tr><td>（八）權利分派基準日：</td><td>113年07月09日</td></tr>
        <tr><td>除權/除息交易日：</td><td>113年07月03日</td></tr>
        <tr><td>＊現金股利發放日：</td><td>113年07月31日</td></tr>
        <tr><td>※除息--普通股：每壹股配發現金(股利) 10.50000000 元</td></tr>
        <tr><td>※除權--普通股：每壹股配發股票(股利) 0 元</td></tr>
      </table>
    </body></html>
    """.encode("utf-8")
    key = DetailKey(
        market="twse",
        symbol="2330",
        announcement_date=date(2024, 6, 20),
        sequence=1,
    )

    row = parse_mops_detail(html, key=key)

    assert row["date"] == date(2024, 7, 3)
    assert row["record_date"] == date(2024, 7, 9)
    assert row["stop_transfer_start"] == date(2024, 7, 5)
    assert row["stop_transfer_end"] == date(2024, 7, 9)
    assert row["cash_dividend_per_share"] == pytest.approx(10.5)
    assert row["cash_payment_date"] == date(2024, 7, 31)
    assert row["stock_dividend_ratio"] == 0.0
    assert row["subscription_ratio"] == 0.0
    assert row["stock_terms_complete"] is True


def test_parse_mops_detail_keeps_missing_payment_date_for_avoidance() -> None:
    html = """
    <html><body><table>
      <tr><td>公司代號</td><td>2330</td></tr>
      <tr><td>除權/除息交易日：</td><td>113年07月03日</td></tr>
      <tr><td>※除息--普通股：每壹股配發現金(股利) 10 元</td></tr>
    </table></body></html>
    """.encode("utf-8")

    row = parse_mops_detail(
        html,
        key=DetailKey(
            market="twse",
            symbol="2330",
            announcement_date=date(2024, 6, 20),
            sequence=1,
        ),
    )

    assert row["cash_dividend_per_share"] == pytest.approx(10.0)
    assert row["cash_payment_date"] is None


def test_entitlement_input_rejects_tampered_reference(tmp_path) -> None:
    path = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 7, 3)],
            "symbol": ["2330"],
            "reference_price": [990.0],
            "event_type": ["息"],
        }
    ).write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "baseline_established": True,
                "coverage_complete": True,
                "failure_count": 0,
                "schema_version": 3,
                "rows": 1,
                "output_receipt": {
                    "size": path.stat().st_size,
                    "sha256": digest,
                },
            }
        ),
        encoding="utf-8",
    )

    receipt = _verified_reference_receipt(path)
    assert receipt["rows"] == 1

    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="size receipt mismatch"):
        _verified_reference_receipt(path)


def test_raw_receipt_manifest_binds_request_and_response(tmp_path) -> None:
    output_dir = tmp_path / "public"
    raw_root = output_dir / "raw" / "tw_corporate_action_entitlements"
    receipt_path = raw_root / "lists" / "twse-2330-113-v1.html"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(b"<html>official response</html>")
    request_data = {
        "TYPEK": "sii",
        "co_id": "2330",
        "year": "113",
    }
    _reset_raw_receipt_requests()
    _record_raw_receipt_request(
        receipt_path,
        url="https://example.test/mops",
        data=request_data,
        content=receipt_path.read_bytes(),
    )

    receipt = _write_content_addressed_receipt_manifest(
        output_dir=output_dir,
        raw_root=raw_root,
    )

    manifest_path = output_dir / receipt["relative_path"]
    assert manifest_path.stem == receipt["sha256"]
    assert receipt["entries"] == 1
    row = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert row["path"] == (
        "raw/tw_corporate_action_entitlements/lists/twse-2330-113-v1.html"
    )
    assert row["request"]["data"] == request_data
    assert row["response_sha256"] == hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()

    receipt_path.write_bytes(b"<html>tampered after parse</html>")
    with pytest.raises(RuntimeError, match="changed after it was parsed"):
        _write_content_addressed_receipt_manifest(
            output_dir=output_dir,
            raw_root=raw_root,
        )
