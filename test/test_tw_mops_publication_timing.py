from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from stockagent.data.tw_mops_publication_timing import (
    _deadline_proxy, board_authorization_date, build_publication_candidates,
)


def test_board_date_is_not_the_reporting_period_or_an_ambiguous_note() -> None:
    end = date(2024, 12, 31)
    assert board_authorization_date(
        "民國113年及112年1月1日至12月31日之合併財報已於民國114年3月10日經董事會通過。",
        end,
    ) == date(2025, 3, 10)
    assert board_authorization_date(
        "本報告於一一四年三月十二日經董事會通過發布。", end,
    ) == date(2025, 3, 12)
    assert board_authorization_date(
        "報告於114年3月10日或114年3月11日通過。", end,
    ) is None
    assert board_authorization_date("2024年12月31日止財報", end) is None


def test_legacy_deadline_is_only_a_proxy() -> None:
    expected, source = _deadline_proxy("2009Q4", "tw_gaap")
    assert expected == date(2010, 4, 30)
    assert "20100520" in source


def test_document_candidates_cover_facts_and_keep_estimates_out_of_pit(tmp_path: Path) -> None:
    root = tmp_path / "mops_xbrl"
    archive_sha = "a" * 64
    facts_sha = "b" * 64
    receipt_dir = root / "raw/ifrs/2024Q4"
    receipt_dir.mkdir(parents=True)
    raw_path = receipt_dir / f"{archive_sha}.zip"
    raw_path.write_bytes(b"source")
    facts_path = root / f"normalized/ifrs/2024Q4/{archive_sha}/facts.parquet"
    facts_path.parent.mkdir(parents=True)
    rows = []
    for index in range(32):
        member = f"tifrs-fr1-m1-ci-cr-{2000 + index}-2024Q4.html"
        common = {"source_member": member, "document_sha256": f"{index:064x}",
                  "entity_identifier": str(2000 + index)}
        rows.append({**common, "concept": "ifrs:Revenue", "raw_value": "1"})
        if index < 31:
            rows.append({**common,
                         "concept": "tifrs-notes:DateAndProceduresOfAuthorisationForIssueOfFinancialStatements",
                         "raw_value": f"本財報於民國114年3月{index % 10 + 1}日經董事會通過。"})
    pl.DataFrame(rows).write_parquet(facts_path)
    (receipt_dir / f"{archive_sha}.json").write_text(__import__("json").dumps({
        "period": "2024Q4", "standard": "ifrs", "archive_sha256": archive_sha,
        "facts_sha256": facts_sha, "fact_count": len(rows),
        "raw_path": str(raw_path), "facts_path": str(facts_path),
    }))
    frame, summary = build_publication_candidates(root)
    assert frame.height == 32
    assert summary["fact_rows_covered"] == len(rows)
    assert summary["publication_time_basis_counts"] == {
        "estimated_from_issuer_board_authorization": 31,
        "estimated_same_quarter_board_date_p75": 1,
    }
    assert frame.get_column("publication_clock_taipei").null_count() == 32
    assert not frame.get_column("training_eligible").any()
    assert not frame.get_column("historical_point_in_time").any()
