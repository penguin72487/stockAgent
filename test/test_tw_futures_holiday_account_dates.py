from datetime import date
import gzip
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_futures_holiday_account_dates import (
    HOLIDAY_ACCOUNT_DATES_ABI,
    load_holiday_account_dates,
    supplement_holiday_account_calendar,
)


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source(tmp_path):
    body = b"synthetic-notice"
    raw = tmp_path / "source/raw.gz"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(gzip.compress(body))
    body_hash = hashlib.sha256(body).hexdigest()
    parsed = tmp_path / "source/parsed.json"
    parsed.write_text(json.dumps({"source_url": "https://example.test/notice", "content_sha256": body_hash}))
    script = tmp_path / "artifacts/operations/futures-universe765-repair-20261027-v1/stage-holiday-account-dates-v1.py"
    script.parent.mkdir(parents=True)
    script.write_text("SPECS = " + repr((("2023-12-01", "2024-02-05",
        ("2024-02-06", "2024-02-07"), "2024-02-15", body_hash),)) + "\n")
    data = tmp_path / "data_source"
    data.mkdir()
    period = {"indexed_date": "2023-12-01", "last_session": "2024-02-05",
              "account_dates": ["2024-02-06", "2024-02-07"], "reopening_session": "2024-02-15",
              "extraction": "first_page_visual_review_of_exact_pdf", "source_url": "https://example.test/notice",
              "raw_path": str(raw.relative_to(tmp_path)), "raw_sha256": _sha(raw), "content_sha256": body_hash,
              "parsed_path": str(parsed.relative_to(tmp_path)), "parsed_sha256": _sha(parsed)}
    manifest = {"contract": HOLIDAY_ACCOUNT_DATES_ABI, "auto_adopted_for_training": False,
                "staging_script_sha256": _sha(script), "periods": [period], "account_date_count": 2}
    (data / "manifest.json").write_text(json.dumps(manifest))
    return load_holiday_account_dates(data, workspace=tmp_path), raw, data


def _calendar():
    return pl.DataFrame({
        "date": [date(2024, 2, 5), date(2024, 2, 6), date(2024, 2, 15)],
        "accounting_only_date": [False, True, False],
        "accounting_event_source_sha256s": [[], ["a" * 64], []],
    })


def test_account_date_supplement_preserves_cash_source_and_adds_no_market_event(tmp_path):
    source, _, _ = _source(tmp_path)
    base = _calendar()
    result = supplement_holiday_account_calendar(base, source)
    assert result["date"].to_list() == [date(2024, 2, 5), date(2024, 2, 6),
                                         date(2024, 2, 7), date(2024, 2, 15)]
    assert result["accounting_only_date"].to_list() == [False, True, True, False]
    assert result["accounting_event_source_sha256s"].to_list() == [[], ["a" * 64], [], []]
    assert result["holiday_account_notice_sha256"].null_count() == 2
    assert result["holiday_account_source_fingerprint"].drop_nulls().to_list() == [source.fingerprint] * 2
    assert not {"price", "volume", "executable"} & set(result.columns)
    assert base.equals(_calendar())
    with pytest.raises(ValueError, match="already composed"):
        supplement_holiday_account_calendar(result, source)


def test_account_date_supplement_rejects_market_collision_and_changed_source(tmp_path):
    source, raw, _ = _source(tmp_path)
    conflict = _calendar().with_columns(
        pl.when(pl.col("date") == date(2024, 2, 6)).then(False)
        .otherwise(pl.col("accounting_only_date")).alias("accounting_only_date"))
    with pytest.raises(ValueError, match="conflicts with an observed market"):
        supplement_holiday_account_calendar(conflict, source)
    raw.write_bytes(gzip.compress(b"different"))
    with pytest.raises(ValueError, match="source input changed"):
        supplement_holiday_account_calendar(_calendar(), source)


def test_account_date_source_rejects_modified_notice_and_workspace_escape(tmp_path):
    _, raw, data = _source(tmp_path)
    raw.write_bytes(gzip.compress(b"different"))
    with pytest.raises(ValueError, match="notice source changed"):
        load_holiday_account_dates(data, workspace=tmp_path)
    with pytest.raises(ValueError, match="must reside"):
        load_holiday_account_dates(data, workspace=tmp_path / "source")


def test_account_date_source_rejects_manifest_date_edits_even_if_order_is_valid(tmp_path):
    _, _, data = _source(tmp_path)
    path = data / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["periods"][0]["account_dates"][1] = "2024-02-08"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="dates differ from the reviewed original"):
        load_holiday_account_dates(data, workspace=tmp_path)


def test_account_date_supplement_clips_to_source_horizon(tmp_path):
    source, _, _ = _source(tmp_path)
    early = _calendar().filter(pl.col("date") == date(2024, 2, 5))
    result = supplement_holiday_account_calendar(early, source)
    assert result.height == 1
    assert result["holiday_account_notice_sha256"].null_count() == 1


def test_canonical_accounting_compiler_accepts_source_bound_account_dates(tmp_path, monkeypatch):
    import scripts.prepare_tw_futures_margin_training as builder

    holiday, _, _ = _source(tmp_path)
    source = tmp_path / "compiler_source"
    source.mkdir()
    (source / "position_research_policy.json").write_text("{}")
    market = pl.DataFrame({"date": [date(2024, 2, 5), date(2024, 2, 15)],
                           "product": ["TX", "TX"]})
    monkeypatch.setattr(builder, "read_bound_output", lambda _path: (market, None))
    monkeypatch.setattr(builder, "raw_physical_observations", lambda raw, _universe: raw)
    monkeypatch.setattr(builder, "load_dated_product_specifications", lambda *_args: None)
    monkeypatch.setattr(builder, "load_preparation_final_settlements",
                        lambda _path: pl.DataFrame(schema={"product": pl.String}))

    class CalendarCaptured(Exception):
        pass

    def compile_until_calendar(holiday_source):
        captured = []

        def save(name, frame):
            assert name == "accounting_event_calendar"
            captured.append(frame)
            raise CalendarCaptured

        with pytest.raises(CalendarCaptured):
            builder._compile_accounting(source, pl.DataFrame({"product": ["TX"]}),
                output=tmp_path / "output", end=date(2024, 2, 15), slots=1,
                save=save, log=lambda *_args, **_kwargs: None,
                holiday_account_dates_source=holiday_source)
        return captured[0]

    baseline = compile_until_calendar(None)
    candidate = compile_until_calendar(holiday)
    assert baseline["date"].to_list() == [date(2024, 2, 5), date(2024, 2, 15)]
    assert candidate["date"].to_list() == [date(2024, 2, 5), date(2024, 2, 6),
                                            date(2024, 2, 7), date(2024, 2, 15)]
    assert candidate["accounting_only_date"].to_list() == [False, True, True, False]
    assert candidate.join(baseline.select("date"), on="date", how="semi").select(
        baseline.columns).equals(baseline)


@pytest.mark.parametrize("options", [
    {},
    {"accounting_only": True, "reuse_accounting": True},
    {"accounting_only": True, "parent_accounting": Path("existing")},
    {"accounting_only": True, "previous_sources": Path("existing")},
    {"accounting_only": True, "replay_accounting": Path("existing")},
])
def test_holiday_date_research_never_publishes_or_reuses_old_accounting(tmp_path, options):
    from scripts.prepare_tw_futures_margin_training import build

    with pytest.raises(ValueError, match="fresh, unpublished accounting-only"):
        build(tmp_path / "missing-source", tmp_path / "output", start=date(2011, 1, 1),
              end=date(2026, 9, 24), holiday_account_dates_source=tmp_path / "dates", **options)


@pytest.mark.parametrize("explicit_workspace", [False, True])
def test_frozen_builder_reads_holiday_sources_from_data_workspace(tmp_path, monkeypatch, explicit_workspace):
    import scripts.prepare_tw_futures_margin_training as builder

    _, _, holiday_dir = _source(tmp_path)
    frozen_root = tmp_path / "frozen-code"
    frozen_root.mkdir()
    monkeypatch.setattr(builder, "ROOT", frozen_root)
    monkeypatch.chdir(tmp_path)
    input_dir = tmp_path / "training-sources"
    input_dir.mkdir()
    (input_dir / "source_manifest.json").write_text(json.dumps({"status": "incomplete"}))
    with pytest.raises(ValueError, match="incomplete source evidence"):
        builder.build(input_dir, tmp_path / "output", start=date(2011, 1, 1), end=date(2026, 9, 24),
                      accounting_only=True, holiday_account_dates_source=holiday_dir,
                      holiday_account_workspace=tmp_path if explicit_workspace else None)


def test_holiday_workspace_alone_is_rejected(tmp_path):
    from scripts.prepare_tw_futures_margin_training import build

    with pytest.raises(ValueError, match="requires an explicit holiday account source"):
        build(tmp_path / "missing-source", tmp_path / "output", start=date(2011, 1, 1),
              end=date(2026, 9, 24), holiday_account_workspace=tmp_path)
