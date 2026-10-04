from __future__ import annotations

import io
import zipfile

import pytest

from downloader import taifex_rule_parsing as parser


def test_html_uses_main_content_and_preserves_blank_duplicate_columns():
    raw = b"""<html><body><nav>not data</nav><main><nav>menu</nav>
    <table><tr><th>code</th><th>value</th><th>value</th><th></th></tr>
    <tr><td>TX</td><td>2</td><td></td><td>note</td></tr></table>
    <script>danger()</script></main><footer>not data</footer></body></html>"""
    result = parser.extract_document(raw, "text/html", "https://www.taifex.com.tw/test")
    assert result["parsing_status"] == "parsed"
    assert "not data" not in result["text"]
    assert "menu" not in result["text"]
    assert "danger" not in result["text"]
    assert result["tables"][0]["rows"] == [{"code": "TX", "value": "2", "value__2": "", "column_4": "note"}]


@pytest.mark.parametrize("encoding", ["utf-8-sig", "cp950"])
def test_csv_metadata_and_source_precision_are_preserved(encoding):
    raw = "最新更新(生效)日期：2026/09/17\n契約代碼,調整後原始保證金,備註\nGUF,0.405000001,\n".encode(encoding)
    result = parser.extract_document(raw, "text/html;charset=MS950", "https://www.taifex.com.tw/cht/4/traderPLEquityDown")
    assert result["format"] == "csv"
    assert result["tables"][0]["rows"] == [{"契約代碼": "GUF", "調整後原始保證金": "0.405000001", "備註": ""}]
    assert "2026/09/17" in result["text"]


def test_temporal_dates_do_not_use_publication_as_effectiveness():
    text = (
        "發文日期：中華民國115年9月23日。"
        "依本公司111年6月20日台期監字第1111000782號函辦理。"
        "本次調整自115年9月24日(證券市場處置生效日次一營業日)\n"
        "一般交易時段結束後起實施，於115年10月5日一般交易時段結束後恢復"
        "為115年9月24日調整前之保證金，參照證券市場處置措施，恢復日順延執行。"
    )
    result = parser.temporal_mentions(text)
    assert result[0]["role"] == "publication"
    assert result[0]["boundary"] == "date_only"
    assert result[1]["role"] == "reference"
    assert result[2]["role"] == "effective_start"
    assert result[2]["boundary"] == "after_regular_session"
    assert result[3]["role"] == "effective_end"
    assert result[3]["boundary"] == "after_regular_session"
    assert result[4]["role"] == "unspecified"
    assert [r["date_iso"] for r in result] == ["2026-09-23", "2022-06-20", "2026-09-24", "2026-10-05", "2026-09-24"]


def test_roc_chinese_numerals_pdf_variant_characters_and_impossible_dates():
    rows = parser.temporal_mentions("自九十三年七月二十二日交易時段結束後起實施。自115年9月24日一般交易時段結束後起實施。2026/02/30公告。")
    assert [(r["date_iso"], r["boundary"]) for r in rows] == [
        ("2004-07-22", "after_trading_session"), ("2026-09-24", "after_regular_session"),
    ]


def test_current_filename_is_never_a_historical_date():
    result = parser.extract_document(b"code,value\nTX,200000\n", "text/csv", "https://www.taifex.com.tw/margintable_19990101.csv")
    assert result["temporal_mentions"] == []


def test_restoration_after_a_session_boundary_comma_is_not_an_unspecified_date():
    rows=parser.temporal_mentions('自115年8月12日一般交易時段結束後起實施，'
        '115年8月17日一般交易時段結束後，恢復為115年8月12日調整前之保證金。')
    assert rows[1]['date_iso']=='2026-08-17'
    assert rows[1]['role']=='effective_end'
    assert rows[1]['boundary']=='after_regular_session'
    assert rows[2]['role']=='unspecified'


def test_explicit_effective_date_interval_does_not_invent_time_of_day():
    rows = parser.temporal_mentions("適用期間自115年9月24日起至115年10月5日止。")
    assert [r["role"] for r in rows] == ["effective_start", "effective_end"]
    assert [r["boundary"] for r in rows] == ["date_only", "date_only"]


def test_explicit_temporary_session_interval_retains_scheduled_end():
    rows = parser.temporal_mentions('發文日期：中華民國105年2月2日。'
        '本次保證金調整實施期間自105年2月3日交易時段結束後起，'
        '預計至105年2月16日交易時段結束止。')
    assert [(r['date_iso'], r['role'], r['boundary']) for r in rows] == [
        ('2016-02-02', 'publication', 'date_only'),
        ('2016-02-03', 'effective_start', 'after_trading_session'),
        ('2016-02-16', 'effective_end', 'after_trading_session')]
    # A cited notice is not turned into a second activation date.
    reference = parser.temporal_mentions('依本公司105年2月3日台期結字第10503001141號函辦理。')
    assert reference[0]['role'] == 'reference'


def test_operative_self_clause_is_not_a_reference_to_another_notice():
    text=('期交所依規定調高晟德期貨契約所有月份保證金適用比例，'
          '自109年6月11日(證券市場處置生效日次一營業日)該契約交易時段結束後起實施，'
          '並於109年6月23日該契約交易時段結束後恢復為調整前之保證金。')
    rows=parser.temporal_mentions(text)
    assert [(r['date_iso'],r['role'],r['boundary']) for r in rows]==[
        ('2020-06-11','effective_start','after_trading_session'),
        ('2020-06-23','effective_end','after_trading_session')]
    reference=parser.temporal_mentions('依本公司109年6月11日台期字第109001號函，自即日起實施。')
    assert reference[0]['role']=='reference'


def test_parenthetical_effective_reference_does_not_override_publication():
    rows=parser.temporal_mentions(
        '期交所於115年2月10日(調整生效日前一日)公告調整保證金，'
        '實施期間為115年2月11日一般交易時段結束後生效。')
    assert [(r['date_iso'],r['role'],r['boundary']) for r in rows]==[
        ('2026-02-10','publication','date_only'),
        ('2026-02-11','effective_start','after_regular_session')]
    rows=parser.temporal_mentions(
        '期交所依規定自115年1月13日(證券市場處置生效日次一營業日)'
        '一般交易時段結束後起調高保證金。')
    assert rows[0]['role']=='effective_start'
    assert rows[0]['boundary']=='after_regular_session'


def test_unitless_margintable_is_not_assumed_twd_or_ratio():
    rows = [{"契約代碼": "GUF", "契約中文簡稱": "全新期貨", "調整後原始保證金": "0.405", "調整前原始保證金": "0.2025"}]
    result = parser.margin_changes(rows)
    assert len(result) == 2
    assert all(row["amount_vs_ratio"] == "unknown" and row["normalized_value"] is None for row in result)
    assert {row["phase"] for row in result} == {"before", "after"}
    with_evidence = parser.margin_changes(rows, unit_hint="ratio")
    assert with_evidence[0]["unit"] == "fraction"
    assert with_evidence[0]["normalized_value"] == "0.2025"


def test_explicit_margin_units_and_abc_semantics():
    rows = [
        {"契約代碼": "GUF", "調整後原始保證金": "40.50%", "調整後維持保證金": "31.05%"},
        {"契約代碼": "TXO", "契約ABC值": "A", "調整前結算保證金(TWD)": "200,000"},
        {"契約代碼": "GDF", "調整後原始保證金": "USD 2,000.50"},
    ]
    result = parser.margin_changes(rows)
    assert result[0]["normalized_value"] == "0.405"
    assert result[0]["amount_vs_ratio"] == "ratio"
    assert result[1]["margin_kind"] == "maintenance"
    assert result[2]["unit"] == "TWD"
    assert result[2]["contract_abc"] == "A"
    assert result[3]["normalized_value"] == "2000.50"
    assert result[3]["unit"] == "USD"


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-", "invalid", "-10"])
def test_non_numeric_margin_values_stay_generic_only(value):
    assert parser.margin_changes([{"契約代碼": "TX", "調整後原始保證金": value}]) == []


def test_csv_url_returning_soft_404_does_not_pass():
    result = parser.extract_document(b"<html><head><title>404</title></head><body>not found</body></html>", "text/html", "https://www.taifex.com.tw/margintable.csv")
    assert result["format"] == "html"
    assert result["parsing_status"] == "failed"
    assert result["tables"] == []


def test_binary_legacy_doc_is_retained_as_unparsed_not_decoded_csv():
    result = parser.extract_document(b"\xd0\xcf\x11\xe0" + b"a" * 20, "application/msword", "https://www.taifex.com.tw/old.doc")
    assert result["parsing_status"] == "unsupported"
    assert result["text"] == ""


def test_legacy_word_text_recovers_dates_but_does_not_invent_aligned_tables(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(parser.shutil, "which", lambda name: "/test/antiword")
    def run(args, **kwargs):
        assert kwargs["timeout"] == parser.LEGACY_WORD_TIMEOUT_SECONDS
        assert "shell" not in kwargs
        kwargs["stdout"].write("自99年1月25日起實施。\n原始 維持\nCDF 13.50% 10.35%".encode())
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(parser.subprocess, "run", run)
    result = parser.extract_document(b"\xd0\xcf\x11\xe0", "application/msword", "https://www.taifex.com.tw/old.doc")
    assert result["parsing_status"] == "parsed"
    assert result["format"] == "legacy_word"
    assert result["tables"] == []
    assert result["temporal_mentions"][0]["date_iso"] == "2010-01-25"
    assert "legacy_word_tables_require_layout_validation" in result["warnings"]


@pytest.mark.parametrize("failure", ["unavailable", "timeout", "oversize"])
def test_legacy_word_limits_and_missing_dependency_do_not_fabricate_rules(monkeypatch, failure):
    from types import SimpleNamespace
    monkeypatch.setattr(parser.shutil, "which", lambda name: None if failure == "unavailable" else "/test/antiword")
    monkeypatch.setattr(parser, "MAX_TEXT_CHARACTERS", 10)
    def run(args, **kwargs):
        if failure == "timeout":
            raise parser.subprocess.TimeoutExpired(args, 20)
        kwargs["stdout"].write(b"x" * 41)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(parser.subprocess, "run", run)
    result = parser.extract_document(b"\xd0\xcf\x11\xe0", "application/msword", "https://www.taifex.com.tw/old.doc")
    assert result["parsing_status"] in {"unsupported", "failed"}
    assert result["text"] == ""
    assert result["tables"] == []
    assert result["temporal_mentions"] == []


def test_pdf_without_text_is_pending_ocr_and_never_invents_table():
    from pypdf import PdfWriter

    output = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.write(output)
    result = parser.extract_document(output.getvalue(), "application/pdf", "https://www.taifex.com.tw/test.pdf")
    assert result["parsing_status"] == "pending_ocr"
    assert result["tables"] == []
    assert "pdf_tables_require_layout_validation" in result["warnings"]


def test_pdf_page_limit_is_visible(monkeypatch):
    from pypdf import PdfWriter

    output = io.BytesIO()
    writer = PdfWriter()
    for _ in range(3):
        writer.add_blank_page(width=100, height=100)
    writer.write(output)
    monkeypatch.setattr(parser, "MAX_PDF_PAGES", 1)
    result = parser.extract_document(output.getvalue(), "application/pdf", "https://www.taifex.com.tw/test.pdf")
    assert "pdf_page_limit_reached" in result["warnings"]
    assert result["tables"] == []


def test_damaged_pdf_is_failure_not_worker_exception():
    result = parser.extract_document(b"%PDF-1.7\ninvalid", "application/pdf", "https://www.taifex.com.tw/test.pdf")
    assert result["parsing_status"] == "failed"


def test_document_size_limit_is_visible(monkeypatch):
    monkeypatch.setattr(parser, "MAX_DOCUMENT_BYTES", 4)
    result = parser.extract_document(b"abcdef", "text/plain", "https://www.taifex.com.tw/test")
    assert result["parsing_status"] == "failed"
    assert result["warnings"] == ["document_byte_limit_exceeded"]


def test_escaped_legacy_html_is_extracted_without_executing_it():
    raw = "<html><div id='content'><form><input name='query'><span class='myContent'>自九十三年七月二十二日交易時段結束後起實施。&lt;br &gt;保證金&lt;a href='old.asp'&gt;一覽表&lt;/a&gt;</span></form></div></html>".encode()
    result = parser.extract_document(raw, "text/html", "https://www.taifex.com.tw/cht/11/newsDetail")
    assert "old.asp" not in result["text"]
    assert result["temporal_mentions"][0]["date_iso"] == "2004-07-22"
    assert result["temporal_mentions"][0]["role"] == "effective_start"


def test_escaped_news_content_is_decoded_and_numeric_zero_retained():
    result = parser.extract_document("<main><div id='news_content'>規則&lt;br&gt;保證金&lt;a href='old.asp'&gt;一覽表&lt;/a&gt;</div></main>".encode(), "text/html", "https://www.taifex.com.tw/news")
    assert "<br>" not in result["text"]
    assert "href=" not in result["text"]
    assert parser.margin_changes([{"契約代碼": "TX", "調整後原始保證金": 0}], unit_hint="TWD")[0]["normalized_value"] == "0"


def test_csv_repeated_section_headers_remain_raw_not_misaligned_facts():
    content = '最新更新(生效)日期：2026/07/16,,,單位：契約數\n商品代號,商品名稱,股價指數期貨\n,,自然人,法人\nTX,臺股期貨,12000,24000\n商品代號,商品名稱,商品期貨\nGDF,黃金期貨,1000,3000\n'.encode("cp950")
    result = parser.extract_document(content, "text/csv", "https://www.taifex.com.tw/position.csv")
    assert result["tables"][0]["columns"][0] == "column_1"
    assert len(result["tables"][0]["rows"]) == 6
    assert "csv_repeated_headers_preserved_without_semantic_alignment" in result["warnings"]


def test_spanned_html_table_keeps_physical_cells_without_wrong_header_alignment():
    raw = "<table><tr><th rowspan='2'>商品</th><th colspan='2'>保證金</th></tr><tr><th>原始</th><th>維持</th></tr><tr><td>TX</td><td>100000</td><td>90000</td></tr></table>".encode()
    result = parser.extract_document(raw, "text/html", "https://www.taifex.com.tw/table")
    assert result["tables"][0]["columns"] == ["column_1", "column_2", "column_3"]
    assert len(result["tables"][0]["rows"]) == 3
    assert result["tables"][0]["rows"][-1]["column_3"] == "90000"
    assert "html_spanned_cells_preserved_without_semantic_alignment" in result["warnings"]


def _office_package(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files:
            archive.writestr(name, content)
    return output.getvalue()


def _docx_xml(body):
    return ('<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            f'<w:body>{body}</w:body></w:document>').encode()


def _odf_xml(body):
    return ('<office:document-content '
            'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
            'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" '
            'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0">'
            f'<office:body>{body}</office:body></office:document-content>').encode()


def test_docx_extracts_main_text_and_physical_tables_without_executing_embeds():
    document = _docx_xml(
        '<w:p><w:r><w:t>自115年9月24日起實施。</w:t></w:r></w:p>'
        '<w:tbl><w:tr><w:tc><w:tcPr><w:gridSpan w:val="2"/></w:tcPr>'
        '<w:p><w:r><w:t>原始保證金</w:t></w:r></w:p></w:tc></w:tr>'
        '<w:tr><w:tc><w:p><w:r><w:t>TX</w:t></w:r></w:p>'
        '<w:p><w:r><w:t>臺指</w:t></w:r></w:p></w:tc>'
        '<w:tc><w:p><w:r><w:t>0.405000001</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'
    )
    raw = _office_package([
        ('[Content_Types].xml', '<Types/>'), ('word/document.xml', document),
        ('word/vbaProject.bin', b'never execute'),
        ('word/_rels/document.xml.rels', '<Relationships Target="https://example.invalid/private"/>'),
    ])
    result = parser.extract_document(raw, 'application/octet-stream', 'https://www.taifex.com.tw/file.docx')
    assert result['parser_version'] == parser.PARSER_VERSION == 7
    assert result['format'] == 'docx' and result['parsing_status'] == 'parsed'
    assert result['temporal_mentions'][0]['date_iso'] == '2026-09-24'
    assert result['temporal_mentions'][0]['role'] == 'effective_start'
    assert result['tables'][0]['columns'] == ['column_1', 'column_2']
    assert result['tables'][0]['rows'][-1] == {'column_1': 'TX 臺指', 'column_2': '0.405000001'}
    assert 'never execute' not in result['text'] and 'private' not in result['text']
    assert 'office_table_physical_cells_without_semantic_alignment' in result['warnings']


@pytest.mark.parametrize('suffix,mime', [
    ('odt', 'application/vnd.oasis.opendocument.text'),
    ('ods', 'application/vnd.oasis.opendocument.spreadsheet'),
])
def test_odf_extracts_raw_values_without_expanding_sparse_grids_or_evaluating_formulas(suffix, mime):
    document = _odf_xml(
        '<office:spreadsheet><text:p>發布日期：115年9月23日。</text:p>'
        '<text:p>A<text:s text:c="2"/>B</text:p>'
        '<table:table table:name="資料"><table:table-header-rows><table:table-row>'
        '<table:table-cell><text:p>商品</text:p></table:table-cell>'
        '<table:table-cell><text:p>數值</text:p></table:table-cell>'
        '</table:table-row></table:table-header-rows><table:table-row table:number-rows-repeated="999999999">'
        '<table:table-cell><text:p>TX</text:p></table:table-cell>'
        '<table:table-cell office:value="0.405000001" table:formula="of:=1/0"/>'
        '<table:table-cell table:number-columns-repeated="999999999"/>'
        '</table:table-row></table:table></office:spreadsheet>'
    )
    raw = _office_package([('mimetype', mime), ('content.xml', document)])
    result = parser.extract_document(raw, 'application/octet-stream', f'https://www.taifex.com.tw/file.{suffix}')
    assert result['format'] == suffix and result['parsing_status'] == 'parsed'
    assert 'A  B' in result['text']
    assert result['temporal_mentions'][0]['role'] == 'publication'
    assert len(result['tables'][0]['rows']) == 2
    assert result['tables'][0]['rows'][-1] == {
        'column_1': 'TX', 'column_2': '0.405000001', 'column_3': '',
    }
    assert 'odf_repeated_or_spanned_cells_preserved_without_expansion' in result['warnings']


@pytest.mark.parametrize('encoding', ['utf-8', 'utf-16'])
def test_office_xml_dtd_is_rejected_before_entity_expansion(encoding):
    xml = ('<?xml version="1.0"?><!DOCTYPE document [<!ENTITY extra "pretend value">]>'
           '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
           '<w:body><w:p><w:r><w:t>&extra;</w:t></w:r></w:p></w:body></w:document>').encode(encoding)
    raw = _office_package([('[Content_Types].xml', '<Types/>'), ('word/document.xml', xml)])
    result = parser.extract_document(raw, '', 'https://www.taifex.com.tw/doc.docx')
    assert result['parsing_status'] == 'failed'
    assert any('office_xml_dtd_or_entity_not_allowed' in item for item in result['warnings'])
    assert 'pretend value' not in result['text']


def test_office_xml_and_member_limits_reject_archive_expansion(monkeypatch):
    raw = _office_package([
        ('[Content_Types].xml', '<Types/>'),
        ('word/document.xml', _docx_xml('<w:p><w:r><w:t>' + 'A' * 1000 + '</w:t></w:r></w:p>')),
    ])
    monkeypatch.setattr(parser, 'MAX_OFFICE_XML_BYTES', 128)
    result = parser.extract_document(raw, '', 'https://www.taifex.com.tw/doc.docx')
    assert result['parsing_status'] == 'failed'
    assert any('office_xml_byte_limit_exceeded' in item for item in result['warnings'])
    monkeypatch.setattr(parser, 'MAX_ZIP_MEMBERS', 1)
    result = parser.extract_document(raw, '', 'https://www.taifex.com.tw/doc.docx')
    assert any('office_zip_member_limit_exceeded' in item for item in result['warnings'])


def test_ordinary_zip_is_not_recursively_parsed_even_with_office_url():
    raw = _office_package([('attachment.pdf', b'%PDF-1.7\ninvalid'), ('../../outside', 'not extracted')])
    result = parser.extract_document(raw, '', 'https://www.taifex.com.tw/looks-like.docx')
    assert result['format'] == 'office_or_archive'
    assert result['parsing_status'] == 'unsupported'
    assert result['tables'] == [] and result['text'] == ''


@pytest.mark.parametrize('content', [b'', b'\xd0\xcf\x11\xe0legacy', b'%PDF-1.7\ninvalid', b'code,value\nTX,0\n'])
def test_parser_version_is_present_on_every_result_including_failures(content):
    assert parser.extract_document(content, '', 'https://www.taifex.com.tw/test')['parser_version'] == parser.PARSER_VERSION
