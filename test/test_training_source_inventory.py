from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_training_source_inventory as audit


def write_dates(path: Path, values, *, date_type=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"date": pa.array(values, type=date_type)}), path)


@pytest.mark.parametrize("date_type", [pa.string(), pa.large_string()])
def test_iso_string_dates_preserve_keys_and_nulls(tmp_path, date_type):
    path = tmp_path / "prices.parquet"
    write_dates(
        path, ["2026-09-30 00:00:00", None, "2026-09-30 00:01:00"], date_type=date_type
    )
    keys, nulls = audit.timestamp_keys(path)
    assert nulls == 1
    assert len(keys) == 2
    assert keys[1] - keys[0] == 60_000_000_000


def test_invalid_string_date_is_not_replaced_with_fake_key(tmp_path):
    path = tmp_path / "invalid.parquet"
    write_dates(path, ["not-a-date"])
    with pytest.raises(Exception):
        audit.timestamp_keys(path)


def test_string_and_temporal_keys_use_the_same_nanoseconds(tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    write_dates(a, ["2026-09-30 00:00:00"])
    write_dates(b, [datetime(2026, 9, 30)], date_type=pa.timestamp("us"))
    assert np.array_equal(audit.timestamp_keys(a)[0], audit.timestamp_keys(b)[0])


@pytest.mark.parametrize(
    "arrays",
    [
        [[1, 2, 3], [2, 3, 4]],
        [[1, 1, 3], [1, 2, 2, 4]],
        [[3, 1, 3, 2], [4, 2]],
        [[], [1, 1, 2]],
        [[], []],
        [[1, 2], [1, 3], [2, 4]],
    ],
)
def test_ordered_union_is_exact_even_for_disorder_and_duplicates(arrays):
    values = [np.asarray(a, dtype=np.int64) for a in arrays]
    assert audit.unique_time_union_count(values) == len(
        np.unique(np.concatenate(values))
    )


def configure_report(monkeypatch, tmp_path):
    monkeypatch.setattr(audit, "REPO", tmp_path)
    out = tmp_path / "reports"
    out.mkdir()
    monkeypatch.setattr(audit, "OUTPUT", out)


def test_hot_tail_union_is_by_symbol_not_additive(monkeypatch, tmp_path):
    configure_report(monkeypatch, tmp_path)
    root = tmp_path / "data_bybit/1m"
    write_dates(
        root / "BTC_features.parquet", ["2026-09-30 00:00:00", "2026-09-30 00:01:00"]
    )
    write_dates(
        root / "_hot_tail/BTC_features.parquet",
        ["2026-09-30 00:01:00", "2026-09-30 00:02:00"],
    )
    write_dates(root / "ETH_features.parquet", ["2026-09-30 00:00:00"])
    spec = (
        "bybit-1m",
        "Bybit",
        "data_bybit/1m",
        "*_features.parquet",
        "symbol-date-tail",
        "primary",
    )
    row = audit.inventory_one(spec, [], set())
    assert row["errors"] == []
    assert row["footer_rows"] == 5
    assert row["unique_key_rows"] == 4
    assert row["duplicate_key_rows"] == 1
    assert row["symbols"] == 2
    assert not row["eviction_authorized_by_inventory"]


def test_mixed_source_files_are_not_whole_file_eviction_candidates():
    for dataset in ("binance-1m", "okx-1m", "tw-futures-daily", "tw-options-daily"):
        assert (
            audit.storage_class(dataset)
            == "source_and_projection_mixed_do_not_delete_whole"
        )
    assert audit.storage_class("tw-public-features").startswith(
        "service_projection_keep"
    )
    assert audit.storage_class("tw-minute").startswith("remote_rebuild_candidate")


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("mops_xbrl/income_statement.parquet", "financials_revenue"),
        ("mof_tax_revenue.parquet", "macro_original_releases"),
        ("tw_corporate_action_entitlements.parquet", "corporate_events"),
        ("tdcc_shareholding_distribution.parquet", "ownership_institutional_margin"),
        ("taifex_large_trader_futures_oi.parquet", "derivatives_funding_oi"),
        ("some_unknown_source", "other_sources_review_required"),
    ],
)
def test_public_category_is_an_explicit_routing_label(identifier, expected):
    assert audit.public_information_category(identifier) == expected


def test_public_inventory_keeps_observation_and_publication_bounds_separate(tmp_path):
    from datetime import date

    path = tmp_path / "financial.parquet"
    pq.write_table(
        pa.table(
            {
                "report_date": [date(2026, 6, 30)],
                "published_at": [datetime(2026, 8, 14, 19)],
                "issuer": ["2330"],
                "value": [2.0],
            }
        ),
        path,
    )
    row = audit.public_table_footer(path)
    assert row["rows"] == 1
    assert row["temporal_bounds"]["report_date"]["first"] == "2026-06-30"
    assert row["temporal_bounds"]["published_at"]["first"].startswith("2026-08-14")
    assert not row["unique_rows_verified"]
    assert not row["eviction_authorized_by_inventory"]
    assert audit.storage_class("unknown-new-dataset").startswith("unclassified_keep")


def test_missing_configured_root_does_not_claim_unique_zero(monkeypatch, tmp_path):
    configure_report(monkeypatch, tmp_path)
    spec = (
        "tw-derivatives-bidask",
        "BidAsk",
        "missing",
        "*.parquet",
        None,
        "configured",
    )
    row = audit.inventory_one(spec, [], set())
    assert not row["exists"]
    assert row["unique_key_rows"] is None
    assert row["key_check"] == "not_scanned"


def test_compound_keys_exclude_nulls_and_find_duplicates(monkeypatch, tmp_path):
    configure_report(monkeypatch, tmp_path)
    path = tmp_path / "features.parquet"
    pq.write_table(
        pa.table(
            {"date": ["2026-09-30"] * 4, "symbol": ["2330", "2330", "2317", None]}
        ),
        path,
    )
    spec = (
        "example",
        "Features",
        "features.parquet",
        "",
        ["date", "symbol"],
        "derived",
    )
    row = audit.inventory_one(spec, [], set())
    assert row["errors"] == []
    assert row["unique_key_rows"] == 2
    assert row["null_key_rows"] == 1
    assert row["duplicate_key_rows"] == 1


def test_source_host_unit_keeps_service_projection_not_research_builds():
    root = Path(__file__).resolve().parents[1]
    template = (
        root / "deploy/systemd/stockagent-tw-public-feature-reconcile.service.in"
    ).read_text()
    command = next(
        line for line in template.splitlines() if line.startswith("ExecStart=")
    )
    assert "reconcile_tw_public_training_features.py" in command
    assert "--defer-market-hours" in command
    assert "--research-" not in command
    assert "OnSuccess=stockagent-tw-public-cold-publish.service" in template


def test_source_host_default_reconcile_does_not_launch_research(monkeypatch, tmp_path):
    from contextlib import nullcontext
    from scripts import reconcile_tw_public_training_features as reconcile

    commands = []
    monkeypatch.setattr(
        reconcile, "needs_rebuild", lambda *_args: (False, "2026-09-30", False)
    )
    monkeypatch.setattr(reconcile, "_source_update_lock", lambda _root: nullcontext())
    monkeypatch.setattr(
        reconcile.subprocess, "run", lambda command, **_kwargs: commands.append(command)
    )
    monkeypatch.setattr(
        reconcile.sys, "argv", ["reconcile", "--input-dir", str(tmp_path)]
    )
    assert reconcile.main() == 0
    assert commands == []
