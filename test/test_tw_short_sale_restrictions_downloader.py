import json
from pathlib import Path

import polars as pl
import pytest

import downloader.download_tw_short_sale_restrictions as downloader
from downloader.download_tw_short_sale_restrictions import (
    _announcement_data_quality,
    _announcement_record,
    _roc_date,
)
from stockagent.data.tw_delisting_rules import (
    classify_delisting_notice,
    extract_announcement_symbols,
    is_relevant_announcement_subject,
)


def test_roc_date_parser() -> None:
    assert _roc_date("公告日期 111年9月29日") == "2022-09-29"
    assert _roc_date("1150709") == "2026-07-09"
    assert _roc_date("本（115）年6月23日") == "2026-06-23"
    assert _roc_date("本年2月24日", default_year=2026) == "2026-02-24"


def test_announcement_parser_extracts_short_ban_and_delisting_dates() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2022-09-29",
        number="test",
        subject="公告股票代號：5820終止上櫃",
        body=(
            "股票代號：5820，自111年9月30日起暫停融資融券交易，惟了結交易不在此限；"
            "自111年11月1日起停止櫃檯買賣，並訂於111年11月11日起終止櫃檯買賣；"
            "應於終止上櫃前第10個營業日前償還或還券了結。"
        ),
        url="https://example.test",
    )
    assert record["symbols"] == "5820"
    assert record["short_open_ban_date"] == "2022-09-30"
    assert record["trading_suspension_date"] == "2022-11-01"
    assert record["delisting_date"] == "2022-11-11"
    assert record["short_cover_lead_trading_days"] == 10
    assert record["closing_only"] is True


def test_announcement_parser_handles_historical_symbol_labels_and_exact_cover_deadline() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2009-10-09",
        number="test-cover",
        subject="公告上櫃股票代號：6141與公司代號:6211終止上櫃",
        body="自98年10月13日起暫停融資融券，且應於98年10月16日前償還或還券了結。",
        url="https://example.test/cover",
    )
    assert record["symbols"] == "6141,6211"
    assert record["short_open_ban_date"] == "2009-10-13"
    assert record["short_cover_deadline"] == "2009-10-16"


def test_unparsed_company_notice_resolves_only_from_unique_same_market_alias() -> None:
    lookup: downloader.CompanySymbolLookup = {}
    downloader._add_company_symbol_aliases(
        lookup,
        market="tpex",
        text="光洋應用材料科技股份有限公司（股票代號：1785）",
        symbols=["1785"],
    )
    record = _announcement_record(
        market="tpex",
        issued_date="2016-05-13",
        number="correction",
        subject=(
            "本中心105年5月13日證櫃監字第10502004811號公告有關"
            "光洋應用材料科技股份有限公司停止買賣公告更正"
        ),
        body="刪除惟了結交易不在此限文字",
        url="https://example.test/correction",
    )
    assert record["symbols"] == ""
    assert downloader._resolve_unparsed_announcement(record, lookup) == "resolved"
    assert record["symbols"] == "1785"

    lookup[("tpex", "光洋應用材料科技")] = {"1785", "9999"}
    record["symbols"] = ""
    assert downloader._resolve_unparsed_announcement(record, lookup) == "unparseable"


def test_symbol_less_expiry_correction_is_classified_non_equity() -> None:
    record = _announcement_record(
        market="twse",
        issued_date="2020-04-16",
        number="warrant-correction",
        subject="更正本公司原到期日、最後交易日及終止上市日，請查照。",
        body="",
        url="https://example.test/warrant-correction",
    )
    assert downloader._resolve_unparsed_announcement(record, {}) == "non_equity"


def test_unresolved_emerging_only_notice_is_explicitly_out_of_universe() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2005-04-22",
        number="emerging-only",
        subject=(
            "公告本中心終止與光威電腦股份有限公司簽訂之興櫃股票"
            "櫃檯買賣契約，並終止該公司普通股在證券商營業處所買賣。"
        ),
        body="",
        url="https://example.test/emerging-only",
    )
    assert record["symbols"] == ""
    assert downloader._resolve_unparsed_announcement(record, {}) == (
        "out_of_universe"
    )


@pytest.mark.parametrize(
    "subject",
    [
        (
            "杜康控股有限公司參與發行之臺灣存託憑證"
            "（公司代號：911616）因終止上市而暫停融資融券交易。"
        ),
        (
            "新光一號不動產投資信託基金受益證券（代號：01003T）"
            "自112年11月29日起終止上市。"
        ),
        (
            "SGBR2X凱基32購01（代號：035204）到期日及終止上市日更正。"
        ),
    ],
)
def test_explicit_unsupported_security_cannot_be_remapped_by_company_alias(
    subject: str,
) -> None:
    lookup: downloader.CompanySymbolLookup = {("twse", "杜康控股"): {"9188"}}
    record = _announcement_record(
        market="twse",
        issued_date="2021-07-21",
        number="unsupported-product",
        subject=subject,
        body="",
        url="https://example.test/unsupported-product",
    )

    assert record["symbols"] == ""
    assert downloader._resolve_unparsed_announcement(record, lookup) == (
        "out_of_universe"
    )
    assert record["symbols"] == ""


def test_four_digit_tdr_style_equity_remains_in_canonical_universe() -> None:
    record = _announcement_record(
        market="twse",
        issued_date="2021-07-21",
        number="supported-tdr",
        subject="某公司臺灣存託憑證（證券代號：9188）終止上市。",
        body="",
        url="https://example.test/supported-tdr",
    )

    assert record["symbols"] == "9188"


def test_cached_unsupported_product_mapping_is_removed_and_not_reused(
    tmp_path: Path,
) -> None:
    subject = (
        "杜康控股有限公司參與發行之臺灣存託憑證"
        "（公司代號：911616）因終止上市而暫停融資融券交易。"
    )
    stale = _announcement_record(
        market="twse",
        issued_date="2021-07-21",
        number="stale-tdr",
        subject=subject,
        body="",
        url="https://example.test/stale-tdr",
    )
    stale["symbols"] = "9188"
    pl.DataFrame([stale], infer_schema_length=None).write_parquet(
        tmp_path / "tw_delisting_short_sale_announcements.parquet"
    )

    assert downloader._repair_announcement_record(stale)["symbols"] == ""
    assert downloader._load_company_symbol_lookup(tmp_path) == {}


def test_announcement_parser_extracts_short_sale_resume_date() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2009-06-10",
        number="test-resume",
        subject="證券代號：1333恢復融資融券",
        body="自98年6月11日起恢復融資融券交易。",
        url="https://example.test/resume",
    )
    assert record["short_open_resume_date"] == "2009-06-11"


def test_announcement_parser_uses_issued_year_for_current_year_dates() -> None:
    record = _announcement_record(
        market="twse",
        issued_date="2026-02-23",
        number="test-current-year",
        subject=(
            "公告股票代號：3454因自本（115）年3月27日起終止上市，"
            "爰自本年2月24日起暫停融資融券交易。"
        ),
        body="應於終止上市前第10個營業日前償還或還券了結。",
        url="https://example.test/current-year",
    )

    assert record["delisting_date"] == "2026-03-27"
    assert record["short_open_ban_date"] == "2026-02-24"
    assert record["short_cover_lead_trading_days"] == 10


def test_article_78_exemption_is_not_parsed_as_affirmative_cover() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2026-07-03",
        number="11502017631",
        subject="公告健亞生物科技股份有限公司（股票代號：4130）終止上櫃",
        body=(
            "健亞公司以股份轉換方式成為易威生醫科技股份有限公司"
            "（股票代號：1799）百分之百持股之子公司；"
            "依第78條第1項第3款規定，證券商無須通知委託人於健亞公司股票"
            "終止上櫃前10個營業日前償還或還券了結。"
        ),
        url="https://example.test/4130",
    )

    assert record["symbols"] == "4130"
    assert record["article_78_exempt"] is True
    assert record["short_cover_lead_trading_days"] is None
    assert record["short_cover_deadline"] is None


def test_shared_rules_distinguish_normal_cover_and_technical_old_share_replacement() -> None:
    affirmative = classify_delisting_notice(
        "通知委託人於該有價證券終止上市前第10個營業日前償還或還券了結。"
    )
    technical = classify_delisting_notice(
        "因變更股票面額，舊股票終止上市並換發新股票，新股票同日繼續上市買賣。"
    )
    cash_reduction = classify_delisting_notice(
        "現金減資換發股票停止過戶、舊股票停止上市買賣暨新股票開始換發及上市買賣日期。"
    )

    assert affirmative.requires_relative_cover is True
    assert affirmative.article_78_exempt is False
    assert technical.technical_share_replacement is True
    assert technical.requires_relative_cover is False
    assert cash_reduction.technical_share_replacement is True


def test_shared_rules_distinguish_short_side_exemption_from_financing_only_exemption() -> None:
    both_sides_exempt = classify_delisting_notice(
        "股票自107年6月13日起終止櫃檯買賣；"
        "該股票於終止櫃檯買賣前，融資融券交易無須提前了結。"
    )
    financing_only_exempt = classify_delisting_notice(
        "股票自109年11月4日起終止櫃檯買賣；"
        "融券餘額應於停止過戶第六個營業日前還券了結，"
        "融資餘額則無須適用終止上櫃前第十個營業日前償還之規定。"
    )
    cancelled = classify_delisting_notice(
        "原公告自107年5月18日起暫停融資融券交易乙案，免予執行。"
    )

    assert both_sides_exempt.article_78_exempt is True
    assert financing_only_exempt.article_78_exempt is False
    assert financing_only_exempt.requires_relative_cover is False
    assert cancelled.restriction_cancelled is True


def test_stop_transfer_six_session_cover_rule_uses_its_own_calendar_anchor() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2020-10-15",
        number="5349-stop-transfer-cover",
        subject="股票代號：5349自109年11月4日起終止櫃檯買賣",
        body=(
            "停止股東名簿記載變更起迄日期：109年10月31日至109年11月4日。"
            "融券餘額應於停止過戶第六個營業日（含）前還券了結，"
            "融資餘額則無須適用終止上櫃前第十個營業日前償還之規定。"
        ),
        url="https://example.test/5349",
    )

    assert record["article_78_exempt"] is False
    assert record["short_cover_lead_trading_days"] is None
    assert record["short_cover_anchor_date"] == "2020-10-31"
    assert record["short_cover_anchor_lead_trading_days"] == 6


def test_cancelled_delisting_keeps_an_explicit_continuing_short_ban() -> None:
    subject = (
        "中福國際股份有限公司（公司代號：1435）上市有價證券原將於"
        "112年4月2日終止上市，已融資融券者，應於終止上市前第10個"
        "營業日前償還或還券了結，惟該有價證券已免除終止上市，故亦"
        "同步免除前開了結事宜，另因財務報告每股淨值仍低於票面，爰"
        "繼續暫停融資融券交易。"
    )
    rules = classify_delisting_notice(subject)
    administrative_rules = classify_delisting_notice(
        "免除中福國際股份有限公司（公司代號：1435）有價證券終止上市之實施，"
        "暨恢復其上市有價證券買賣日期。"
    )
    record = _announcement_record(
        market="twse",
        issued_date="2023-03-16",
        number="1435-cancelled-delisting",
        subject=subject,
        body="",
        url="https://example.test/1435",
    )

    assert rules.delisting_cancelled is True
    assert rules.is_delisting_notice is False
    assert rules.requires_relative_cover is False
    assert rules.continues_short_open_ban is True
    assert administrative_rules.delisting_cancelled is True
    assert record["delisting_cancelled"] is True
    assert record["short_cover_lead_trading_days"] is None
    assert record["short_open_ban_date"] == "2023-03-16"


def test_cancelled_short_restriction_does_not_emit_ban_date() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2018-05-17",
        number="5301-cancelled",
        subject=(
            "股票代號：5301原公告自107年5月18日起暫停融資融券交易乙案，"
            "免予執行。"
        ),
        body="原停止買賣及暫停融資融券交易原因業已消滅。",
        url="https://example.test/5301",
    )

    assert record["restriction_cancelled"] is True
    assert record["short_open_ban_date"] is None


def test_event_clause_symbol_extraction_excludes_newly_listed_counterparty() -> None:
    subjects = [
        (
            "漢磊先進投資控股股份有限公司(公司代號：3707)普通股股票開始買賣，"
            "暨漢磊科技股份有限公司(公司代號：5326)有價證券自同日起"
            "終止在證券商營業處所買賣。",
            ["5326"],
        ),
        (
            "鑫聯大投資控股股份有限公司(證券代號：3709)普通股股票開始買賣，"
            "暨捷元股份有限公司(證券代號：5384)有價證券自同日起"
            "終止在證券商營業處所買賣。",
            ["5384"],
        ),
    ]
    for subject, expected in subjects:
        assert extract_announcement_symbols(subject, "") == expected
        assert is_relevant_announcement_subject(subject) is True


def test_mixed_equity_and_bond_notice_keeps_only_equity_symbol() -> None:
    subject = (
        "樂陞科技股份有限公司普通股股票（股票代號：3662），"
        "公司債（債券代號：36624、36625），暨以其為標的之權證，"
        "自同日起暫停融資融券交易。"
    )

    assert is_relevant_announcement_subject(subject) is True
    assert extract_announcement_symbols(subject, "") == ["3662"]


def test_short_restriction_clause_resolves_named_target_from_body_mapping() -> None:
    subject = (
        "公告調整樺晟電子股份有限公司等6家公司上櫃有價證券之交易方式；"
        "其中樺晟電子股份有限公司之普通股股票並自同日起暫停融資融券交易。"
    )
    body = (
        "新增為變更交易方法之上櫃有價證券簡稱（股票代號）："
        "樺晟（3202）、中茂（5205）；恢復普通交割：藝舍-KY（2724）。"
    )

    assert extract_announcement_symbols(subject, body) == ["3202"]


def test_emerging_to_listed_transition_is_not_a_lifecycle_delisting() -> None:
    subject = (
        "大江生醫股份有限公司初次申請上櫃普通股股票，"
        "以一般類股開始櫃檯買賣日期，並自同日起終止該興櫃股票之櫃檯買賣。"
    )
    rules = classify_delisting_notice(subject)

    assert rules.emerging_to_listed_transition is True
    assert rules.is_delisting_notice is False
    assert is_relevant_announcement_subject(subject) is False


def test_monthly_subject_prefilter_keeps_only_rule_relevant_announcements() -> None:
    assert is_relevant_announcement_subject("公告股票終止上市") is True
    assert is_relevant_announcement_subject("自明日起暫停融資融券交易") is True
    assert is_relevant_announcement_subject("恢復有價證券買賣") is True
    assert is_relevant_announcement_subject("294檔認購（售）權證終止上市") is False
    assert is_relevant_announcement_subject("中央政府建設公債到期終止上市") is False
    assert is_relevant_announcement_subject("臺北市市府債終止上市") is False
    assert is_relevant_announcement_subject("無擔保公司債終止上市") is False
    assert is_relevant_announcement_subject("指數投資證券ETN終止上市") is False
    assert is_relevant_announcement_subject("甲種記名式特別股到期收回暨終止上市") is False
    assert is_relevant_announcement_subject("ETF受益憑證（證券代號：00685L）終止上市") is True
    assert is_relevant_announcement_subject("本公司債務清償完成後股票終止上市") is True
    assert is_relevant_announcement_subject("董事會決議股利分派") is False


def test_unlabeled_parenthesized_etf_code_and_spaced_stock_label_are_extracted() -> None:
    etf = (
        "新光證券投資信託股份有限公司經理之新光標普電動車ETF"
        "證券投資信託基金（00925）受益憑證因終止上市而暫停融資融券交易。"
    )
    spaced_label = (
        "駿吉控股股份有限公司上櫃之有價證券（股票代 號：1591）"
        "自115年6月11日起停止買賣。"
    )

    assert extract_announcement_symbols(etf, "") == ["00925"]
    assert extract_announcement_symbols(spaced_label, "") == ["1591"]


def test_historical_chinese_numeral_stock_code_is_extracted_but_business_id_is_not() -> None:
    subject = (
        "公告本中心終止與普羅強生半導體股份有限公司"
        "（股票代號：三一三三）簽訂之興櫃股票櫃檯買賣契約。"
    )
    body = "公司營利事業統一編號：13173433。"

    assert extract_announcement_symbols(subject, body) == ["3133"]


def test_official_company_and_historical_quote_names_resolve_symbol_less_notice(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "公司代號": ["6184"],
            "公司名稱": ["大豐有線電視股份有限公司"],
            "公司簡稱": ["大豐電"],
        }
    ).write_parquet(tmp_path / "twse_listed_company_basic.parquet")
    pl.DataFrame(
        {
            "date": ["2004-06-01"],
            "代號": ["8010"],
            "名稱": ["益和"],
        }
    ).write_parquet(tmp_path / "tpex_daily_ohlcv.parquet")

    lookup = downloader._load_company_symbol_lookup(tmp_path)
    transfer = _announcement_record(
        market="tpex",
        issued_date="2005-02-04",
        number="transfer",
        subject=(
            "公告大豐有線電視股份有限公司普通股股票，自94年2月15日起，"
            "終止在證券商營業處所買賣。"
        ),
        body="",
        url="https://example.test/transfer",
    )
    historical = _announcement_record(
        market="tpex",
        issued_date="2005-05-19",
        number="historical",
        subject=(
            "公告本中心終止與益和股份有限公司簽訂之股票櫃檯買賣契約，"
            "並終止普通股股票在證券商營業處所買賣。"
        ),
        body="",
        url="https://example.test/historical",
    )

    assert downloader._resolve_unparsed_announcement(transfer, lookup) == "resolved"
    assert transfer["symbols"] == "6184"
    assert downloader._resolve_unparsed_announcement(historical, lookup) == "resolved"
    assert historical["symbols"] == "8010"


def test_historical_quote_mojibake_is_not_identity_evidence(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": ["2004-06-01", "2004-06-01", "2004-06-01", "2004-10-28"],
            "代號": ["8010", "8925", "5351", "5351"],
            "名稱": ["益和", "嚙踝蕭嚙踝蕭", "鈺喉蕭", "鈺創"],
            "_name_decode_status": [
                "",
                "official_receipt_name_bytes_unrecoverable",
                "official_receipt_name_bytes_unrecoverable",
                "",
            ],
        }
    ).write_parquet(tmp_path / "tpex_daily_ohlcv.parquet")

    lookup = downloader._load_company_symbol_lookup(tmp_path)

    assert lookup[("tpex", "益和")] == {"8010"}
    assert lookup[("tpex", "鈺創")] == {"5351"}
    assert ("tpex", "鈺喉蕭") not in lookup
    assert all("嚙" not in alias for _, alias in lookup)


def test_stock_code_label_variant_is_extracted_from_terminal_notice() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2017-07-07",
        number="10602006831",
        subject=(
            "三汰控股(開曼)股份有限公司（股票代碼：4762）普通股股票，"
            "自106年8月1日起終止在證券商營業處所買賣。"
        ),
        body="",
        url="https://example.test/4762",
    )

    assert record["symbols"] == "4762"
    assert record["delisting_date"] == "2017-08-01"
    assert classify_delisting_notice(
        "華特電子普通股自94年11月17日起終止於證券商營業處所買賣。"
    ).is_delisting_notice is True


@pytest.mark.parametrize(
    ("company_name", "company_alias", "symbol", "issued_date", "effective_date"),
    [
        ("普格科技股份有限公司", "普格", "3073", "2013-04-02", "2013-04-08"),
        ("常珵科技股份有限公司", "常珵", "8097", "2014-11-17", "2014-11-19"),
    ],
)
def test_roundup_short_ban_resolves_company_shorthand_and_same_day_date(
    company_name: str,
    company_alias: str,
    symbol: str,
    issued_date: str,
    effective_date: str,
) -> None:
    roc_year = int(effective_date[:4]) - 1911
    month = int(effective_date[5:7])
    day = int(effective_date[8:10])
    subject = (
        f"公告{company_name}等8家公司之上櫃有價證券新增為變更交易方法，"
        f"自{roc_year}年{month}月{day}日起實施；其中{company_alias}公司之普通股"
        "股票並自同日起暫停融資融券交易，惟了結交易不在此限。"
    )
    body = (
        "新增為變更交易方法之上櫃有價證券簡稱（股票代號）："
        f"{company_alias}（{symbol}）、{company_alias}三（{symbol}3）、其他（9999）。"
    )
    record = _announcement_record(
        market="tpex",
        issued_date=issued_date,
        number=f"roundup-{symbol}",
        subject=subject,
        body=body,
        url=f"https://example.test/{symbol}",
    )

    assert is_relevant_announcement_subject(subject) is True
    assert record["symbols"] == symbol
    assert record["short_open_ban_date"] == effective_date


def test_generic_trading_method_roundup_is_not_a_rule_announcement() -> None:
    subject = (
        "公告甲股份有限公司等8家公司之上櫃有價證券新增為變更交易方法、"
        "新增分盤方式交易及恢復普通交割方式交易，自111年8月18日起實施。"
    )

    assert is_relevant_announcement_subject(subject) is False


def test_temporary_reduction_halt_inside_disposition_roundup_is_not_lifecycle_rule() -> None:
    subject = (
        "公告調整甲股份有限公司等7家公司上櫃有價證券之交易方式，其中乙公司"
        "因辦理減資暨換股作業將自111年8月18日至111年8月25日停止買賣，"
        "而延至111年8月26日實施。"
    )

    assert is_relevant_announcement_subject(subject) is False


def test_twse_month_filters_irrelevant_subject_before_fetching_detail(monkeypatch) -> None:
    search_html = b"""
        <form id="form_category"><input name="SYNCHRONIZER_TOKEN" value="token"></form>
    """
    result_html = """
        <table><tbody>
          <tr><td>0</td><td>115/07/01</td><td>證券商</td><td>N1</td><td>董事會決議股利分派</td><td><a href="/irrelevant">detail</a></td></tr>
          <tr><td>0</td><td>115/07/01</td><td>證券商</td><td>N-BOND</td><td>中央政府建設公債到期終止上市</td><td><a href="/bond">detail</a></td></tr>
          <tr><td>0</td><td>115/07/02</td><td>證券商</td><td>N2</td><td>公告股票代號：4130終止上市</td><td><a href="/relevant">detail</a></td></tr>
        </tbody></table>
    """.encode()

    class Response:
        def __init__(self, content: bytes):
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self):
            self.headers = {}
            self.detail_urls: list[str] = []

        def get(self, url: str, timeout: int):
            if url == downloader.TWSE_SEARCH:
                return Response(search_html)
            self.detail_urls.append(url)
            return Response("股票代號：4130，自115年7月28日起終止上市".encode())

        def post(self, *_args, **_kwargs):
            return Response(result_html)

    session = Session()
    monkeypatch.setattr(downloader.requests, "Session", lambda: session)

    batch = downloader._download_twse_month(2026, 7, 5)

    assert len(batch.records) == 1
    assert batch.records[0]["symbols"] == "4130"
    assert batch.filtered_irrelevant == 2
    assert batch.unparseable == 0
    assert session.detail_urls == ["https://dsp.twse.com.tw/relevant"]


def test_twse_legacy_plain_text_archive_is_parsed_and_month_checked(monkeypatch) -> None:
    search_html = b"""
        <form id="form_category"><input name="SYNCHRONIZER_TOKEN" value="token"></form>
    """
    legacy_text = """臺灣證券交易所股份有限公司 函
發文日期：中華民國107年1月31日
發文字號：臺證上一字第1070000001號
主旨：公告甲股份有限公司（公司代號：1234）股票終止上市，請查照。
說明：自107年2月12日起終止上市。
=================================================================
臺灣證券交易所股份有限公司 函
發文日期：中華民國107年1月30日
發文字號：臺證上一字第1070000002號
主旨：中央政府建設公債到期終止上市，請查照。
說明：如主旨。
=================================================================
""".encode()

    class Response:
        def __init__(self, content: bytes, content_type: str):
            self.content = content
            self.headers = {"Content-Type": content_type}

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, *_args, **_kwargs):
            return Response(search_html, "text/html")

        def post(self, *_args, **_kwargs):
            return Response(legacy_text, "text/plain;charset=UTF-8")

    monkeypatch.setattr(downloader.requests, "Session", Session)

    batch = downloader._download_twse_month(2018, 1, 5)
    assert [record["symbols"] for record in batch.records] == ["1234"]
    assert batch.filtered_irrelevant == 1
    assert batch.unparseable == 0

    with pytest.raises(RuntimeError, match="different month"):
        downloader._download_twse_month(2018, 2, 5)


def test_twse_current_month_rejects_mismatched_result_month(monkeypatch) -> None:
    search_html = b"""
        <form id="form_category"><input name="SYNCHRONIZER_TOKEN" value="token"></form>
    """
    result_html = """
        <table><tbody><tr>
          <td>0</td><td>115/06/30</td><td>證券商</td><td>N1</td>
          <td>公告股票代號：4130終止上市</td><td><a href="/detail">detail</a></td>
        </tr></tbody></table>
    """.encode()

    class Response:
        def __init__(self, content: bytes):
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, *_args, **_kwargs):
            return Response(search_html)

        def post(self, *_args, **_kwargs):
            return Response(result_html)

    monkeypatch.setattr(downloader.requests, "Session", Session)

    with pytest.raises(RuntimeError, match="different month"):
        downloader._download_twse_month(2026, 7, 5)


def test_tpex_month_rejects_server_normalized_month(monkeypatch) -> None:
    result_html = """
        <table><tbody><tr>
          <td>0</td><td>91/11/01</td><td>N1</td>
          <td>公告股票代號：6107終止上櫃</td><td><a href="/detail">detail</a></td>
        </tr></tbody></table>
    """.encode()

    class Response:
        content = result_html

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self):
            self.headers = {}

        def post(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setattr(downloader.requests, "Session", Session)

    with pytest.raises(RuntimeError, match="different month"):
        downloader._download_tpex_month(2002, 12, 5)


def test_tpex_blank_subject_fetches_narrow_detail_before_filtering(monkeypatch) -> None:
    result_html = """
        <table><tbody>
          <tr><td>1</td><td>98/10/09</td><td>N1</td><td></td><td><a href="/terminal">detail</a></td></tr>
          <tr><td>2</td><td>98/10/08</td><td>N2</td><td></td><td><a href="/dividend">detail</a></td></tr>
          <tr><td>3</td><td>98/10/07</td><td>N3</td><td></td><td><a href="/empty">detail</a></td></tr>
        </tbody></table>
    """.encode()
    terminal_html = """
        <html><body>
          <nav>公告股票代號：9999終止上櫃</nav>
          <div class="page-wrapper">
            <table><tr><td>發文日期：中華民國98年10月9日</td></tr></table>
            <table><tr><td>主　旨：</td><td>
              公告亞智科技股份有限公司（代號：5492）普通股股票，
              自98年10月30日起終止在證券商營業處所買賣，並自98年10月13日起
              暫停融資融券交易，且應於98年10月16日前償還或還券了結。
            </td></tr></table>
          </div>
        </body></html>
    """.encode()
    dividend_html = """
        <div class="page-wrapper"><table><tr>
          <td>主旨：</td><td>董事會決議股利分派</td>
        </tr></table></div>
    """.encode()
    empty_html = """
        <div class="page-wrapper"><table><tr>
          <td>主旨：</td><td></td>
        </tr></table></div>
    """.encode()

    class Response:
        def __init__(self, content: bytes):
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self):
            self.headers = {}
            self.detail_urls: list[str] = []

        def post(self, *_args, **_kwargs):
            return Response(result_html)

        def get(self, url: str, timeout: int):
            del timeout
            self.detail_urls.append(url)
            if url.endswith("/terminal"):
                return Response(terminal_html)
            if url.endswith("/dividend"):
                return Response(dividend_html)
            return Response(empty_html)

    session = Session()
    monkeypatch.setattr(downloader.requests, "Session", lambda: session)

    batch = downloader._download_tpex_month(2009, 10, 5)

    assert [record["symbols"] for record in batch.records] == ["5492"]
    assert batch.records[0]["short_open_ban_date"] == "2009-10-13"
    assert batch.records[0]["short_cover_deadline"] == "2009-10-16"
    assert "9999" not in batch.records[0]["body_text"]
    assert batch.detail_subject_backfills == 2
    assert batch.filtered_irrelevant == 1
    assert batch.detail_content_unavailable == 1
    assert batch.unparseable == 1
    assert len(session.detail_urls) == 3


def test_archive_marks_known_source_boundaries_without_requesting_them(monkeypatch) -> None:
    tpex_calls: list[tuple[int, int]] = []
    twse_calls: list[tuple[int, int]] = []

    def tpex_month(year: int, month: int, _timeout: int):
        tpex_calls.append((year, month))
        return []

    def twse_month(year: int, month: int, _timeout: int):
        twse_calls.append((year, month))
        return []

    monkeypatch.setattr(downloader, "_download_tpex_month", tpex_month)
    monkeypatch.setattr(downloader, "_download_twse_month", twse_month)
    monkeypatch.setattr(downloader, "_download_twse_keyword", lambda *_args: [])

    _, failures, entries = downloader._download_announcement_archive(
        start_year=2002,
        end_year=2002,
        workers=1,
        timeout=5,
    )

    assert failures == []
    assert tpex_calls == [(2002, 11), (2002, 12)]
    assert twse_calls == []
    tpex_unavailable = [
        row
        for row in entries
        if row["market"] == "tpex" and row["status"] == "source_unavailable"
    ]
    twse_unavailable = [
        row
        for row in entries
        if row["market"] == "twse" and row["status"] == "source_unavailable"
    ]
    assert len(tpex_unavailable) == 10
    assert len(twse_unavailable) == 12


def test_archive_receipts_explicit_out_of_universe_evidence(monkeypatch) -> None:
    excluded = _announcement_record(
        market="twse",
        issued_date="2021-07-21",
        number="six-digit-tdr",
        subject=(
            "杜康控股有限公司臺灣存託憑證（公司代號：911616）"
            "因終止上市而暫停融資融券交易。"
        ),
        body="",
        url="https://example.test/six-digit-tdr",
    )
    monkeypatch.setattr(downloader, "_download_tpex_month", lambda *_args: [])
    monkeypatch.setattr(downloader, "_download_twse_month", lambda *_args: [])
    monkeypatch.setattr(
        downloader,
        "_download_twse_keyword",
        lambda *_args: downloader._AnnouncementDownloadBatch(
            unparseable=1,
            unresolved_records=[excluded],
        ),
    )

    records, failures, entries = downloader._download_announcement_archive(
        start_year=2002,
        end_year=2002,
        workers=1,
        timeout=5,
    )

    assert records == []
    assert failures == []
    keyword = next(row for row in entries if row["source"] == "keyword:終止上市")
    assert keyword["unparseable"] == 0
    assert keyword["out_of_universe"] == 1
    assert keyword["out_of_universe_records"] == [
        {
            "announcement_date": "2021-07-21",
            "market": "twse",
            "document_number": "six-digit-tdr",
            "subject": excluded["subject"],
            "source_url": "https://example.test/six-digit-tdr",
        }
    ]


def test_archive_task_retries_transient_request_failures(monkeypatch) -> None:
    calls: list[int] = []
    delays: list[float] = []

    def transient_download():
        calls.append(1)
        if len(calls) < 3:
            raise downloader.requests.ConnectionError("temporary official-site reset")
        return downloader._AnnouncementDownloadBatch(records=[])

    monkeypatch.setattr(downloader.time, "sleep", lambda value: delays.append(float(value)))
    result = downloader._download_archive_task(
        "tpex",
        "tpex:test:monthly",
        transient_download,
        retries=3,
        retry_backoff=0.25,
    )

    assert isinstance(result, downloader._AnnouncementDownloadBatch)
    assert result.retry_count == 2
    assert len(calls) == 3
    assert delays == [0.25, 0.5]


def test_partial_archive_failure_is_nonzero_unless_explicitly_allowed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def tpex_month(_year: int, month: int, _timeout: int):
        if month == 1:
            raise RuntimeError("simulated month failure")
        if month == 2:
            return downloader._AnnouncementDownloadBatch(
                records=[
                    _announcement_record(
                        market="tpex",
                        issued_date="2003-02-01",
                        number="partial-success",
                        subject="公告股票代號：6107終止上櫃",
                        body="自92年3月1日起終止上櫃",
                        url="https://example.test/partial-success",
                    )
                ],
                filtered_irrelevant=2,
                unparseable=1,
            )
        return []

    monkeypatch.setattr(downloader, "_download_tpex_month", tpex_month)
    monkeypatch.setattr(downloader, "_download_twse_month", lambda *_args: [])
    monkeypatch.setattr(downloader, "_download_twse_keyword", lambda *_args: [])
    monkeypatch.setattr(downloader, "_download_tpex_current_terms", lambda *_args: pl.DataFrame())

    rejected_dir = tmp_path / "rejected"
    rejected = downloader.main(
        [
            "--output-dir",
            str(rejected_dir),
            "--start-year",
            "2003",
            "--end-year",
            "2003",
            "--workers",
            "1",
        ]
    )
    assert rejected == 1
    assert not (rejected_dir / "tw_delisting_short_sale_announcements.parquet").exists()
    rejected_report = json.loads(
        (rejected_dir / "tw_short_sale_download_report.json").read_text(encoding="utf-8")
    )
    january = next(
        row
        for row in rejected_report["requests"]
        if row["market"] == "tpex" and row["period"] == "2003-01"
    )
    assert january["status"] == "error"
    assert rejected_report["requests_complete"] is False
    assert rejected_report["data_output_written"] is False

    allowed_dir = tmp_path / "allowed"
    allowed = downloader.main(
        [
            "--output-dir",
            str(allowed_dir),
            "--start-year",
            "2003",
            "--end-year",
            "2003",
            "--workers",
            "1",
            "--allow-partial",
        ]
    )
    assert allowed == 0
    allowed_report = json.loads(
        (allowed_dir / "tw_short_sale_download_report.json").read_text(encoding="utf-8")
    )
    assert allowed_report["requests_complete"] is False
    assert allowed_report["data_output_written"] is True
    assert allowed_report["filtered_irrelevant"] == 2
    assert allowed_report["unparseable"] == 1


def test_download_quality_report_contains_coverage_and_rule_counts(tmp_path: Path) -> None:
    pl.DataFrame(
        {"symbol": ["4130", "5820"], "date": ["2026-07-28", "2022-11-11"]}
    ).write_parquet(
        tmp_path / "twse_delisted_company.parquet"
    )
    pl.DataFrame({"symbol": ["6107"], "date": ["2018-05-17"]}).write_parquet(
        tmp_path / "tpex_delisted_company.parquet"
    )
    announcements = pl.DataFrame(
        {
            "announcement_date": ["2026-07-03", "2022-09-29", "2018-05-17"],
            "market": ["twse", "twse", "tpex"],
            "document_number": ["a", "b", "c"],
            "source_url": ["u1", "u2", "u3"],
            "symbols": ["4130", "5820", "6107"],
            "subject": [
                "公告4130終止上櫃",
                "公告5820終止上櫃",
                "公告6107終止上櫃",
            ],
            "body_text": [
                "依第78條第1項第3款規定，無須通知於終止上櫃前10個營業日前償還或還券了結",
                "通知於終止上櫃前第10個營業日前償還或還券了結",
                "通知於終止上櫃前第10個營業日前償還或還券了結",
            ],
            "article_78_exempt": [True, False, False],
            "short_cover_lead_trading_days": [None, 10, 10],
            "short_cover_deadline": [None, None, None],
        }
    )

    quality = _announcement_data_quality(announcements, output_dir=tmp_path)
    assert quality["rows"] == 3
    assert quality["symbols_nonempty_rate"] == 1.0
    assert quality["composite_key_duplicate_rows"] == 0
    assert quality["article_78_exempt_count"] == 1
    assert quality["affirmative_cover_count"] == 2
    assert quality["coverage_complete"] is True
    assert quality["archive_cohort_coverage_complete"] is True
    assert quality["official_delisting_coverage"]["twse_delisted_company"]["covered_symbols"] == 2
    assert (
        quality["official_delisting_coverage"]["twse_delisted_company"]
        ["archive_available_cohort"]["coverage_rate"]
        == 1.0
    )


def test_download_quality_coverage_never_cross_matches_market_symbols(
    tmp_path: Path,
) -> None:
    pl.DataFrame({"symbol": ["2330"], "date": ["2024-01-05"]}).write_parquet(
        tmp_path / "twse_delisted_company.parquet"
    )
    pl.DataFrame({"symbol": ["2330"], "date": ["2024-01-05"]}).write_parquet(
        tmp_path / "tpex_delisted_company.parquet"
    )
    announcements = pl.DataFrame(
        {
            "announcement_date": ["2024-01-02"],
            "market": ["twse"],
            "document_number": ["a"],
            "source_url": ["u1"],
            "symbols": ["2330"],
            "subject": ["公告2330終止上市"],
            "body_text": ["股票終止上市"],
        }
    )

    quality = _announcement_data_quality(announcements, output_dir=tmp_path)
    coverage = quality["official_delisting_coverage"]
    assert coverage["twse_delisted_company"]["covered_symbols"] == 1
    assert coverage["tpex_delisted_company"]["covered_symbols"] == 0
    assert coverage["tpex_delisted_company"]["archive_available_cohort"]["covered_symbols"] == 0
