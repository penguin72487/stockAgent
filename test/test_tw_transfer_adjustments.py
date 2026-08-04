from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path

import polars as pl
import pytest

from downloader import download_tw_transfer_adjustments as transfer


def _args(tmp_path: Path, **overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "mode": "rebuild",
        "official_input_dir": tmp_path / "official",
        "yahoo_source_dir": tmp_path / "yahoo",
        "output_path": tmp_path / "tw_transfer_adjustment_reference.parquet",
        "start_date": "2000-01-01",
        "end_date": "2004-02-18",
        "overlap_sessions": 6,
        "workers": 2,
        "timeout": 30,
        "retries": 1,
        "request_interval": None,
        "resume": True,
        "verify_ssl": True,
        "overlap_scale_relative_tolerance": 5e-4,
        "price_tolerance": 0.011,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _fms_payload(symbol: str, year: int, rows: list[list[str]]) -> bytes:
    return json.dumps(
        {
            "stat": "OK",
            "date": f"{year}0101",
            "title": f"民國{year - 1911}年 {symbol} 測試公司 月成交資訊",
            "fields": ["月份", "最高價", "最低價"],
            "data": rows,
        },
        ensure_ascii=False,
    ).encode()


def test_parsers_bind_historical_rule_and_fmsrfk_identity() -> None:
    rule = """
    <html><body>20020730 第 59 條 初次上市之有價證券已於櫃檯買賣，
    以終止櫃檯買賣之最近營業日最後一筆成交價格，
    或最近營業日之參考價格辦理。</body></html>
    """.encode()
    assert transfer._parse_historical_rule(rule)["article"] == 59

    parsed = transfer._parse_fmsrfk_payload(
        _fms_payload("4532", 2003, [["8", "20.0", "15.0"]]),
        symbol="4532",
        year=2003,
    )
    assert parsed == {8: {"high": 20.0, "low": 15.0}}
    with pytest.raises(transfer.TransferAdjustmentError, match="identity mismatch"):
        transfer._parse_fmsrfk_payload(
            _fms_payload("9999", 2003, [["8", "20.0", "15.0"]]),
            symbol="4532",
            year=2003,
        )


def test_split_reconstruction_uses_ratio_squared_and_drops_off_calendar_rows(
    tmp_path: Path,
) -> None:
    symbol = "4532"
    transfer_date = date(2003, 8, 4)
    split_date = date(2003, 8, 6)
    overlap_dates = [
        date(2004, 2, 11),
        date(2004, 2, 12),
        date(2004, 2, 13),
        date(2004, 2, 16),
        date(2004, 2, 17),
        date(2004, 2, 18),
    ]
    post_overlap_split_date = date(2004, 2, 20)
    calendar_dates = {
        transfer_date,
        date(2003, 8, 5),
        split_date,
        post_overlap_split_date,
        *overlap_dates,
    }
    overlap_scale = 2.0
    split_ratio = 1.2
    pre_split_scale = overlap_scale * split_ratio**2

    yahoo_rows = [
        {
            "date": transfer_date,
            "open": 18.1 / pre_split_scale,
            "max": 18.5 / pre_split_scale,
            "min": 17.5 / pre_split_scale,
            "close": 18.0 / pre_split_scale,
            "Stock Splits": 0.0,
        },
        {
            "date": date(2003, 8, 5),
            "open": 19.0 / pre_split_scale,
            "max": 20.0 / pre_split_scale,
            "min": 18.0 / pre_split_scale,
            "close": 19.5 / pre_split_scale,
            "Stock Splits": 0.0,
        },
        {
            "date": split_date,
            "open": 16.0 / overlap_scale,
            "max": 17.0 / overlap_scale,
            "min": 15.0 / overlap_scale,
            "close": 16.5 / overlap_scale,
            "Stock Splits": split_ratio,
        },
        # This synthetic Yahoo row is not a TAIEX session and must not affect FMSRFK.
        {
            "date": date(2004, 1, 1),
            "open": 999.0,
            "max": 999.0,
            "min": 999.0,
            "close": 999.0,
            "Stock Splits": 0.0,
        },
    ]
    official_rows: list[dict[str, object]] = []
    for index, value_date in enumerate(overlap_dates):
        raw_open = 10.0 + index
        raw_high = 11.0 + index
        raw_low = 9.0 + index
        raw_close = 10.5 + index
        yahoo_rows.append(
            {
                "date": value_date,
                "open": raw_open,
                "max": raw_high,
                "min": raw_low,
                "close": raw_close,
                "Stock Splits": 0.0,
            }
        )
        official_rows.append(
            {
                "date": value_date,
                "symbol": symbol,
                "official_open": raw_open * overlap_scale,
                "official_high": raw_high * overlap_scale,
                "official_low": raw_low * overlap_scale,
                "official_close": raw_close * overlap_scale,
            }
        )

    # FMSRFK covers the entire month, not only the six overlap sessions.  This
    # later split also proves that the scale is divided by r² after the overlap
    # anchor instead of silently using a single scale for the whole month.
    post_overlap_ratio = 1.1
    post_overlap_scale = overlap_scale / post_overlap_ratio**2
    yahoo_rows.append(
        {
            "date": post_overlap_split_date,
            "open": 30.0 / post_overlap_scale,
            "max": 40.0 / post_overlap_scale,
            "min": 25.0 / post_overlap_scale,
            "close": 35.0 / post_overlap_scale,
            "Stock Splits": post_overlap_ratio,
        }
    )

    yahoo = pl.DataFrame(yahoo_rows).sort("date")
    candidate = transfer.Candidate(
        symbol=symbol,
        company="瑞智",
        transfer_date=transfer_date,
        previous_session_date=date(2003, 8, 1),
        official_reference_price=17.9,
        yahoo_path=tmp_path / "4532_features.parquet",
        yahoo_frame=yahoo,
        yahoo_receipt={
            "path": str(tmp_path / "4532_features.parquet"),
            "size": 1,
            "sha256": "a" * 64,
            "role": "yahoo_source:4532",
        },
    )
    corporate = pl.DataFrame(
        {
            "date": [split_date, post_overlap_split_date],
            "symbol": [symbol, symbol],
            "market": ["twse", "twse"],
            "previous_close": [19.5, 31.0],
            "source_url": [
                "https://wwwc.twse.com.tw/rwd/zh/exRight/TWT49U",
                "https://wwwc.twse.com.tw/rwd/zh/exRight/TWT49U",
            ],
        }
    )
    fms = {
        2003: {8: {"high": 20.0, "low": 15.0}},
        2004: {2: {"high": 40.0, "low": 18.0}},
    }
    row, detail = transfer._verify_candidate(
        candidate,
        official_quotes=pl.DataFrame(official_rows),
        corporate=corporate,
        fmsrfk=fms,
        calendar_dates=calendar_dates,
        overlap_sessions=6,
        overlap_scale_relative_tolerance=5e-4,
        price_tolerance=0.011,
    )

    assert row["split_squared_multiplier"] == pytest.approx(1.44)
    assert row["split_event_count"] == 2
    assert row["post_overlap_split_event_count"] == 1
    assert row["reconstructed_close"] == pytest.approx(18.0)
    assert row["adjustment_factor"] == pytest.approx(18.0 / 17.9)
    assert row["twt49u_max_absolute_error"] == pytest.approx(0.0)
    assert row["off_calendar_row_count"] == 1
    assert detail["off_calendar_rows"] == ["2004-01-01"]
    assert detail["split_events"][0]["squared_scale_multiplier"] == pytest.approx(1.44)


def test_cached_content_addressed_raw_receipt_resumes_without_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_dir = tmp_path / "raw"
    content = _fms_payload("4532", 2003, [["8", "20.0", "15.0"]])
    first = transfer._write_immutable_raw(
        raw_dir,
        logical_key="fmsrfk-4532-2003",
        suffix=".json",
        content=content,
        source_url=transfer.FMSRFK_URL,
        role="twse_fmsrfk:4532:2003",
    )

    def unexpected_network(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("resume should use the validated immutable receipt")

    monkeypatch.setattr(transfer, "_http_get", unexpected_network)
    parsed, resumed = transfer._fetch_validated_raw(
        raw_dir=raw_dir,
        logical_key="fmsrfk-4532-2003",
        suffix=".json",
        source_url=transfer.FMSRFK_URL,
        role="twse_fmsrfk:4532:2003",
        validator=lambda raw: transfer._parse_fmsrfk_payload(
            raw, symbol="4532", year=2003
        ),
        params={"response": "json", "date": "20030101", "stockNo": "4532"},
        timeout=30,
        retries=1,
        verify_ssl=True,
        resume=True,
    )
    assert parsed[8]["high"] == 20.0
    assert resumed["sha256"] == first["sha256"]
    assert resumed["reused"] is True


def test_receipted_official_input_rejects_tampering(tmp_path: Path) -> None:
    output = tmp_path / "official.parquet"
    output.write_bytes(b"original")
    summary = tmp_path / "official.summary.json"
    summary.write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "output_receipt": transfer._file_receipt(output),
            }
        ),
        encoding="utf-8",
    )
    output.write_bytes(b"tampered")
    with pytest.raises(transfer.TransferAdjustmentError, match="output_receipt"):
        transfer._require_summary_output(
            summary,
            output,
            identity=lambda payload: payload.get("coverage_complete") is True,
        )


def test_failed_run_preserves_existing_artifact_and_writes_incomplete_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)
    output = Path(args.output_path)
    output.write_bytes(b"existing-production-artifact")
    before = transfer._file_receipt(output)
    monkeypatch.setattr(
        transfer,
        "_configure_tw_public_rate_limiter",
        lambda _value: 0.1,
    )
    monkeypatch.setattr(
        transfer,
        "_validate_official_inputs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            transfer.TransferAdjustmentError("receipt chain is incomplete")
        ),
    )

    assert transfer._run(args) == 1
    assert transfer._file_receipt(output) == before
    summary = json.loads(
        transfer._summary_path(output).read_text(encoding="utf-8")
    )
    assert summary["coverage_complete"] is False
    assert summary["replacement_promoted"] is False
    assert summary["production_preserved"] is True
    assert summary["previous_output_receipt"] == before


def test_failed_probe_does_not_replace_valid_production_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)
    output = Path(args.output_path)
    output.write_bytes(b"existing-production-artifact")
    output_receipt = transfer._file_receipt(output)
    summary_path = transfer._summary_path(output)
    summary_path.write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "output_receipt": output_receipt,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    summary_before = summary_path.read_bytes()
    monkeypatch.setattr(
        transfer,
        "_configure_tw_public_rate_limiter",
        lambda _value: 0.1,
    )
    monkeypatch.setattr(
        transfer,
        "_validate_official_inputs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            transfer.TransferAdjustmentError("receipt chain is incomplete")
        ),
    )

    assert transfer._run(args) == 1
    assert summary_path.read_bytes() == summary_before
    failed = json.loads(output.with_suffix(".failed.json").read_text(encoding="utf-8"))
    assert failed["coverage_complete"] is False
    assert failed["production_preserved"] is True
    assert failed["previous_output_receipt"] == output_receipt


def test_successful_orchestration_writes_machine_verifiable_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path)
    official_receipt_path = tmp_path / "official-input"
    official_receipt_path.write_bytes(b"official")
    official_receipt = transfer._file_receipt(
        official_receipt_path, role="official_test_input"
    )
    yahoo_receipt_path = tmp_path / "4532_features.parquet"
    yahoo_receipt_path.write_bytes(b"yahoo")
    yahoo_receipt = transfer._file_receipt(
        yahoo_receipt_path, role="yahoo_source:4532"
    )
    candidate = transfer.Candidate(
        symbol="4532",
        company="瑞智",
        transfer_date=date(2003, 8, 4),
        previous_session_date=date(2003, 8, 1),
        official_reference_price=17.9,
        yahoo_path=yahoo_receipt_path,
        yahoo_frame=pl.DataFrame(
            {
                "date": [date(2003, 8, 4)],
                "open": [1.0],
                "max": [1.0],
                "min": [1.0],
                "close": [1.0],
                "Stock Splits": [0.0],
            }
        ),
        yahoo_receipt=yahoo_receipt,
    )
    dummy_paths = {
        "taiex_calendar": tmp_path / "calendar.parquet",
        "twse_ohlcv": tmp_path / "twse.parquet",
        "corporate_reference": tmp_path / "corp.parquet",
    }
    monkeypatch.setattr(transfer, "_configure_tw_public_rate_limiter", lambda _v: 0.1)
    monkeypatch.setattr(
        transfer,
        "_validate_official_inputs",
        lambda *_a, **_k: (dummy_paths, [official_receipt]),
    )
    calendar = pl.DataFrame({"date": [date(1999, 1, 5), date(2003, 8, 4)]})
    monkeypatch.setattr(
        transfer,
        "_load_calendar",
        lambda _path: (calendar, set(calendar["date"].to_list())),
    )
    monkeypatch.setattr(
        transfer,
        "_load_candidates",
        lambda **_kwargs: ([candidate], [yahoo_receipt], [], [], date(2004, 2, 11)),
    )
    raw_path = tmp_path / "raw-receipt"
    raw_path.write_bytes(b"raw")
    raw_receipt = {
        **transfer._file_receipt(raw_path, role="twse_historical_rule59"),
        "logical_key": "test",
        "source_url": transfer.RULE_URL,
        "reused": True,
    }
    monkeypatch.setattr(
        transfer,
        "_fetch_validated_raw",
        lambda **kwargs: (
            ({8: {"high": 1.0, "low": 1.0}} if "fmsrfk" in kwargs["logical_key"] else {}),
            {
                **raw_receipt,
                "role": kwargs["role"],
            },
        ),
    )
    monkeypatch.setattr(transfer, "_load_twse_quotes", lambda *_a, **_k: pl.DataFrame())
    monkeypatch.setattr(
        transfer, "_load_corporate_references", lambda *_a, **_k: pl.DataFrame()
    )
    artifact_row = {
        "date": date(2003, 8, 4),
        "symbol": "4532",
        "company": "瑞智",
        "adjustment_factor": 1.005,
        "official_reference_price": 17.9,
        "previous_official_session": date(2003, 8, 1),
        "reconstructed_open": 18.1,
        "reconstructed_high": 18.5,
        "reconstructed_low": 17.5,
        "reconstructed_close": 18.0,
        "overlap_start": date(2004, 2, 11),
        "overlap_end": date(2004, 2, 18),
        "overlap_sessions": 6,
        "overlap_scale": 2.0,
        "overlap_scale_relative_spread": 0.0,
        "overlap_max_absolute_error": 0.0,
        "split_event_count": 0,
        "split_squared_multiplier": 1.0,
        "twt49u_max_absolute_error": 0.0,
        "fmsrfk_month_count": 2,
        "fmsrfk_max_absolute_error": 0.0,
        "off_calendar_row_count": 0,
        "adjustment_source": "official_transfer_reference+yahoo_split_r2_verified",
        "validation_status": "verified",
        "yahoo_source_sha256": yahoo_receipt["sha256"],
    }
    monkeypatch.setattr(
        transfer,
        "_verify_candidate",
        lambda *_a, **_k: (
            artifact_row.copy(),
            {
                "symbol": "4532",
                "off_calendar_rows": [],
                "yahoo_source_receipt": yahoo_receipt,
            },
        ),
    )

    assert transfer._run(args) == 0
    output = Path(args.output_path)
    summary = json.loads(
        transfer._summary_path(output).read_text(encoding="utf-8")
    )
    assert summary["coverage_complete"] is True
    assert summary["candidate_count"] == summary["required_candidate_count"] == 1
    assert summary["candidate_count"] == summary["rows"] + summary["unresolved_count"]
    assert summary["candidate_keys"] == ["2003-08-04|4532"]
    assert summary["output_receipt"] == transfer._file_receipt(
        output, role="transfer_adjustment_artifact"
    )
    assert summary["input_receipts"]
    artifact = pl.read_parquet(output)
    assert artifact.select("date", "symbol", "adjustment_factor").to_dicts() == [
        {"date": date(2003, 8, 4), "symbol": "4532", "adjustment_factor": 1.005}
    ]
