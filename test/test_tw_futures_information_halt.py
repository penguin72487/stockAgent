"""Statutory halt accounting must retain the original execution evidence."""
from copy import deepcopy
from datetime import date
import gzip
import hashlib
import inspect
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_information_halt import (
    CONTRACT, HALT_URL, apply_information_halt_values, load_information_halt_review,
)


def case():
    rows = []
    for month, price in (("201704", 36.35), ("201705", 37.25)):
        for day in (18, 19, 20, 21):
            prior = day <= 19
            final = day == 21 and month == "201704"
            rows.append(dict(date=date(2017, 4, day), product="DFF", contract=month,
                physical_instance=f"DFF:{month}#old", valuation_price=price if prior else (35.47 if final else None),
                daily_mark=price if prior else None, cash_settlement=final,
                official_source_sha256="a" * 64 if prior else "",
                official_settlement=str(price) if prior else "",
                outright_volume=0, official_open="", official_close=""))
    frame = pl.DataFrame(rows)
    episode = dict(product="DFF", contract_months=["201704"], halt_start_date="2017-04-19",
        halt_end_date="2017-04-20", resumption_date="2017-04-21",
        rule_known_at="2017-04-19T23:59:59+08:00", law_effective_date=date(2016, 1, 15),
        corporate_input_sha256="b" * 64, source_sha256s=["c" * 64], review_sha256="d" * 64,
        legal_halt_value_contract=CONTRACT)
    corporate = pl.DataFrame(schema={c: pl.String for c in ("product", "from_product", "to_product", "effective_date")})
    return frame, frame.select("date").unique(), episode, corporate


def apply(frame, calendar, episode, corporate):
    return apply_information_halt_values(frame, calendar, episode, corporate=corporate,
        corporate_input_sha256=episode["corporate_input_sha256"])


def test_statutory_mark_changes_only_own_missing_value_and_keeps_final_and_quotes():
    frame, calendar, episode, corporate = case()
    result, values = apply(frame, calendar, episode, corporate)
    assert values.height == 1
    assert values["contract"].to_list() == ["201704"]
    assert values["information_halt_valuation_price"].to_list() == [36.35]
    assert values["information_halt_prior_date"].to_list() == [date(2017, 4, 19)]
    assert values["information_halt_known_at"].to_list() == ["2017-04-19T23:59:59+08:00"]
    assert result.select(frame.columns).drop("valuation_price").equals(frame.drop("valuation_price"))
    assert result.filter(pl.col("cash_settlement"))["valuation_price"].to_list() == [35.47]
    assert result.filter(pl.col("contract") == "201705")["valuation_price"].to_list() == frame.filter(
        pl.col("contract") == "201705")["valuation_price"].to_list()
    again, additions = apply(result, calendar, episode, corporate)
    assert again.equals(result) and additions.is_empty()


def test_unrelated_corporate_repair_keeps_the_same_episode_and_records_new_input():
    frame, calendar, episode, corporate = case()
    corporate = pl.DataFrame(dict(product=["AA1"], from_product=["AAF"], to_product=[None],
        effective_date=["2017-04-20"]), schema_overrides={"to_product": pl.String})
    _, values = apply_information_halt_values(frame, calendar, episode, corporate=corporate,
        corporate_input_sha256="e" * 64)
    assert values["information_halt_valuation_price"].to_list() == [36.35]
    assert values["information_halt_corporate_input_sha256"].to_list() == ["e" * 64]


@pytest.mark.parametrize("problem", ["different_generation", "missing_day", "missing_source", "bad_source",
                                    "missing_mark", "different_literal", "invalid_mark"])
def test_no_borrowing_older_date_other_month_or_generation(problem):
    frame, calendar, episode, corporate = case()
    own = (pl.col("date") == date(2017, 4, 19)) & (pl.col("contract") == "201704")
    if problem == "missing_day":
        frame = frame.filter(~own)
    else:
        field, value = {
            "different_generation": ("physical_instance", "new_generation"),
            "missing_source": ("official_source_sha256", ""),
            "bad_source": ("official_source_sha256", "bad"),
            "missing_mark": ("daily_mark", None),
            "different_literal": ("official_settlement", "36.4"),
            "invalid_mark": ("daily_mark", float("nan")),
        }[problem]
        frame = frame.with_columns(pl.when(own).then(pl.lit(value)).otherwise(pl.col(field)).alias(field))
    result, values = apply(frame, calendar, episode, corporate)
    assert values.is_empty()
    assert result.select(frame.columns).equals(frame)


@pytest.mark.parametrize("problem", ["corporate_action", "volume", "open", "late_notice",
                                    "naive_notice", "future_regime", "duplicate_calendar", "duplicate_day"])
def test_unsupported_or_contradictory_episodes_fail_closed(problem):
    frame, calendar, episode, corporate = case()
    own = (pl.col("date") == date(2017, 4, 20)) & (pl.col("contract") == "201704")
    if problem == "corporate_action":
        corporate = pl.DataFrame(dict(product=["DF1"], from_product=["DFF"], to_product=[None],
            effective_date=["2017-04-20"]), schema_overrides={"to_product": pl.String})
    elif problem in {"volume", "open"}:
        field, value = ("outright_volume", 1) if problem == "volume" else ("official_open", "36.35")
        frame = frame.with_columns(pl.when(own).then(pl.lit(value)).otherwise(pl.col(field)).alias(field))
    elif problem == "late_notice": episode["rule_known_at"] = "2017-04-20T23:59:59+08:00"
    elif problem == "naive_notice": episode["rule_known_at"] = "2017-04-19T23:59:59"
    elif problem == "future_regime": episode["law_effective_date"] = date(2017, 4, 21)
    elif problem == "duplicate_calendar": calendar = pl.concat([calendar, calendar.head(1)])
    elif problem == "duplicate_day": frame = pl.concat([frame, frame.head(1)])
    with pytest.raises(ValueError): apply(frame, calendar, episode, corporate)


def review_fixture(tmp_path):
    urls = ["https://www.taifex.com.tw/rule.pdf", "https://www.taifex.com.tw/commencement",
            "https://www.taifex.com.tw/opening", "https://www.taifex.com.tw/episode", HALT_URL]
    text = ["2015年12月印製 股票期貨暨選擇權契約之標的證券因訊息面暫停交易，"
            "倘當日該股票期貨暨選擇權契約未開盤交易，該契約之每日結算價以當日開盤參考價訂定之。",
            "2016/01/14 訊息面暫停交易自105年1月15日實施",
            "開盤參考價依下列原則決定 前一一般交易時段結算價格 契約調整時另行計算",
            "2017/04/19 契約代號：DF 證券代號：1101 106年4月20日繼續暫停交易 "
            "106年4月份到期契約之最後交易(結算)日順延至106年4月21日",
            "<table><tr><td>9</td><td>DF</td><td>1101</td><td>台泥期貨</td>"
            "<td>台泥選擇權</td><td>2017/4/19</td><td>2017/4/21</td></tr></table>"]
    sources = []
    for i, (url, body) in enumerate(zip(urls, text)):
        raw = tmp_path / f"{i}.gz"; raw.write_bytes(gzip.compress(body.encode(), mtime=0))
        sources.append(dict(url=url, kind="raw_gzip", path=raw.name, sha256=sha256_file(raw)))
        if url != HALT_URL:
            parsed = tmp_path / f"{i}.json"
            parsed.write_text(json.dumps(dict(source_url=url, text=body, format="pdf" if i == 0 else "html",
                content_sha256=hashlib.sha256(body.encode()).hexdigest())))
            sources.append(dict(url=url, kind="parsed_native", path=parsed.name, sha256=sha256_file(parsed)))
    query = tmp_path / "query.json"
    query.write_text(json.dumps(dict(url=HALT_URL, payload=dict(sDate="2016/01/15", eDate="2026/09/04",
        commodity_stock_id=""), content_sha256=hashlib.sha256(text[-1].encode()).hexdigest())))
    review = dict(review_kind=CONTRACT, sources=sources, law_source_url=urls[0], commencement_source_url=urls[1],
        opening_rule_source_url=urls[2], corporate_input_sha256="b" * 64,
        halt_register_query=dict(path=query.name, sha256=sha256_file(query)), episode=dict(product="DFF",
        underlying="1101", contract_months=["201704"], halt_start_date="2017-04-19", halt_end_date="2017-04-20",
        resumption_date="2017-04-21", source_url=urls[3], known_at="2017-04-19T23:59:59+08:00"))
    path = tmp_path / "review.json"; path.write_text(json.dumps(review))
    return path, review


def test_native_rule_and_register_bind_historical_reason_and_publication(tmp_path):
    path, _ = review_fixture(tmp_path)
    episode = load_information_halt_review(path)
    assert episode["rule_known_at"] == "2017-04-19T23:59:59+08:00"
    assert not episode["opening_reference_historical_file_verified"]
    assert episode["value_origin"].startswith("statutory_")


@pytest.mark.parametrize("problem", ["altered_raw", "wrong_underlying", "wrong_halt_reason",
                                    "wrong_resume", "wrong_month", "outside_query", "late_publication"])
def test_corrupted_or_wrong_source_binding_cannot_admit_values(tmp_path, problem):
    path, review = review_fixture(tmp_path)
    if problem == "altered_raw": (tmp_path / "0.gz").write_bytes(b"wrong")
    elif problem == "wrong_underlying": review["episode"]["underlying"] = "2330"
    elif problem == "wrong_halt_reason":
        review["episode"]["halt_start_date"] = "2017-04-18"
    elif problem == "wrong_resume": review["episode"]["resumption_date"] = "2017-04-24"
    elif problem == "wrong_month": review["episode"]["contract_months"] = ["201705"]
    elif problem == "outside_query":
        query = tmp_path / "query.json"; data = json.loads(query.read_text())
        data["payload"]["sDate"] = "2017/04/20"; query.write_text(json.dumps(data))
        review["halt_register_query"]["sha256"] = sha256_file(query)
    elif problem == "late_publication": review["episode"]["known_at"] = "2017-04-20T23:59:59+08:00"
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError): load_information_halt_review(path)


def test_incremental_migration_rejects_other_kernel_changes(tmp_path, monkeypatch):
    import ast
    import scripts.prepare_tw_futures_margin_training as builder
    source = inspect.getsource(builder._compile_accounting)
    node = ast.parse(source).body[0]
    node.args.kwonlyargs.pop(); node.args.kw_defaults.pop()
    node.body = [n for n in node.body if not (isinstance(n, ast.If)
        and isinstance(n.test, ast.Name) and n.test.id == "information_halt_reviews")]
    original = ast.unparse(node) + "\n"
    baseline = tmp_path / "baseline.py"; baseline.write_text(original)
    current = builder._calculation_identity(); previous = deepcopy(current)
    previous["functions"]["_compile_accounting"] = hashlib.sha256(original.encode()).hexdigest()
    receipt = builder._verify_information_halt_kernel_extension(previous, current, (baseline, sha256_file(baseline)))
    assert receipt["unchanged_kernel_ast_verified"]
    changed = deepcopy(current); changed["files"]["stockagent/data/tw_futures_execution_terms.py"] = "different"
    with pytest.raises(ValueError):
        builder._verify_information_halt_kernel_extension(previous, changed, (baseline, sha256_file(baseline)))
    monkeypatch.setattr(builder.inspect, "getsource", lambda _: source.replace("rule_source = source /", "rule_source = other /"))
    with pytest.raises(ValueError):
        builder._verify_information_halt_kernel_extension(previous, current, (baseline, sha256_file(baseline)))


def test_staging_keeps_existing_terminal_inputs_and_all_financial_bytes(tmp_path):
    from scripts.repair_tw_futures_margin_source_intervals import stage_information_halt_repair
    from stockagent.data import tw_futures_information_halt as information
    bundle = tmp_path / "bundle"; bundle.mkdir()
    review_path, review = review_fixture(bundle)
    source = tmp_path / "raw"; source.mkdir()
    (source / "source_manifest.json").write_text('{"identity":"retained"}')
    pending = tmp_path / "pending"; pending.mkdir()
    _, _, _, corporate = case()
    cf_path = pending / "corporate_event_candidates.parquet"; corporate.write_parquet(cf_path)
    review["corporate_input_sha256"] = sha256_file(cf_path); review_path.write_text(json.dumps(review))
    terminal = dict(contract="bound_terminal_operand_source_delta_v1", products=["ES1"], path="terminal", sha256="f" * 64)
    parent = dict(status="pending_source_bound_rule_delta", parent_source_manifest_sha256=sha256_file(source / "source_manifest.json"),
        changed_products=["ES1", "CPF"], sources=[], terminal_source_delta=terminal,
        outputs={cf_path.name: dict(sha256=sha256_file(cf_path))})
    parent_path = pending / "manifest.json"; parent_path.write_text(json.dumps(parent))
    baseline = bundle / "baseline.py"; baseline.write_text("# preserved original\n")
    manifest = dict(status="source_bound_information_halt_extension", parent_source_manifest_sha256=parent["parent_source_manifest_sha256"],
        corporate_input_sha256=sha256_file(cf_path), implementation_sha256=sha256_file(__import__('pathlib').Path(information.__file__)),
        sources=[*review["sources"], dict(review["halt_register_query"], kind="query_receipt")],
        reviews=[dict(path=review_path.name, sha256=sha256_file(review_path))],
        accounting_builder_baseline=dict(path=baseline.name, sha256=sha256_file(baseline)))
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    before = sha256_file(cf_path)
    receipt = tmp_path / "staging.json"
    result = stage_information_halt_repair(source, pending, bundle, receipt)
    after = json.loads(parent_path.read_text())
    assert sha256_file(cf_path) == before
    assert after["terminal_source_delta"] == terminal
    assert after["changed_products"] == ["CPF", "DFF", "ES1"]
    assert after["outputs"] == parent["outputs"]
    assert result["accounting_rows_recompiled"] == 0 and not result["current_training_source_overwritten"]
    with pytest.raises(ValueError):
        stage_information_halt_repair(source, pending, bundle, receipt)
