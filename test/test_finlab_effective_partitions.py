"""Full source-choice parity including legitimate sparse/error partitions."""
from datetime import date
import itertools
import hashlib

import pytest

from scripts import download_finlab_market_intraday as market
from scripts import benchmark_finlab_effective_partitions as trial


STATUSES = (None, "downloaded_unverified_for_pit", "derived_unverified_for_pit",
            "derived_no_regular_trades", "verified_closed_date", "verified_no_trade",
            "partition_not_ready", "derivation_error", "authentication_failed",
            "unknown_future_failure", "")


def relation(connection):
    return connection.execute("SELECT * FROM effective_partitions "
                              "ORDER BY kind,symbol,trade_date").fetchall()


@pytest.mark.parametrize("direct_status,derived_status", tuple(itertools.product(STATUSES, repeat=2)))
def test_candidate_matches_reference_for_every_source_status(tmp_path, direct_status, derived_status):
    connection = market._open_index(tmp_path / "index.sqlite3")
    day = date(2026, 10, 14)
    # Deliberately different payloads and check clocks expose wrong-source picks.
    if direct_status is not None:
        market._record(connection, "tw_minute", "2330", day, direct_status, 127, 1111)
    if derived_status is not None:
        market._record(connection, "tw_minute_derived", "2330", day, derived_status, 93, 2222)
    market._record(connection, "tw_tick", "2330", day, "partition_not_ready", 7, 3333)
    connection.execute("DROP VIEW effective_partitions")
    connection.execute(trial.REFERENCE_VIEW)
    reference = relation(connection)
    trial.candidate_view(connection)
    assert relation(connection) == reference
    minute = [row for row in reference if row[0] == "tw_minute"]
    assert len(minute) == int(direct_status is not None or derived_status is not None)
    for kind in ("tw_tick", "tw_minute"):
        actual = connection.execute("SELECT * FROM effective_partitions WHERE kind=? "
            "AND symbol=? AND trade_date=?", (kind, "2330", str(day))).fetchall()
        assert actual == [row for row in reference if row[:3] == (kind, "2330", str(day))]
    connection.close()


def test_candidate_uses_same_snapshot_without_persistent_schema_or_cache(tmp_path):
    connection = market._open_index(tmp_path / "index.sqlite3")
    day = date(2026, 10, 14)
    market._record(connection, "tw_minute", "2330", day, "partition_not_ready", 0, 0)
    market._record(connection, "tw_minute_derived", "2330", day, "derived_unverified_for_pit", 90, 2222)
    original_schema = connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall()
    trial.candidate_view(connection)
    assert relation(connection)[0][3] == "derived_unverified_for_pit"
    market._record(connection, "tw_minute", "2330", day, "downloaded_unverified_for_pit", 127, 1111)
    assert relation(connection)[0][3:6] == ("downloaded_unverified_for_pit", 127, 1111)
    market._record(connection, "tw_minute", "2330", day, "partition_not_ready", 0, 0)
    assert relation(connection)[0][3:6] == ("derived_unverified_for_pit", 90, 2222)
    assert connection.execute("SELECT * FROM sqlite_master ORDER BY name").fetchall() == original_schema
    connection.close()


def test_runtime_view_identity_matches_actual_connection_not_code_label(tmp_path):
    connection = market._open_index(tmp_path / "index.sqlite3")
    actual = market._effective_partitions_view_identity(connection)
    sql = connection.execute("SELECT sql FROM sqlite_temp_master "
                             "WHERE name='effective_partitions'").fetchone()[0]
    assert actual["sql_sha256"] == hashlib.sha256(sql.encode()).hexdigest()
    connection.execute("DROP VIEW effective_partitions")
    connection.execute(trial.REFERENCE_VIEW)
    old = market._effective_partitions_view_identity(connection)
    assert old != actual
    connection.execute("DROP VIEW effective_partitions")
    with pytest.raises(ValueError, match="missing"):
        market._effective_partitions_view_identity(connection)
    connection.close()


def test_benchmark_refuses_source_output_overlap_before_native_writes(tmp_path):
    with pytest.raises(ValueError, match="separate from source"):
        trial.benchmark(tmp_path / "finlab", tmp_path / "public", tmp_path / "finlab" / "trial")


def test_benchmark_rejects_unbounded_repetition(tmp_path):
    with pytest.raises(ValueError, match="matched pairs"):
        trial.benchmark(tmp_path / "finlab", tmp_path / "public", tmp_path / "trial", pairs=9)
