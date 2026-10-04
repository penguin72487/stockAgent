from __future__ import annotations

from pathlib import Path
from datetime import date
import pytest

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from stockagent.data.columnar_lake import (
    SourceFileContract,
    compact_parquet_files,
    source_signature,
    read_daily_projection,
)


@pytest.mark.parametrize('engine',['arrow','polars','duckdb'])
def test_daily_projection_keeps_exact_source_values_and_string_identity(tmp_path,engine):
    source=tmp_path/'real-shape.parquet'
    table=pa.table({'date':[date(2026,10,1),date(2026,10,2),date(2026,10,2)],
                    'symbol':['0050','0050','2330'],'close':[123.456789,None,float('nan')]})
    pq.write_table(table,source)
    actual=read_daily_projection(source,columns=['symbol','close'],symbols=['0050'],
                                 start_date=date(2026,10,2),end_date=date(2026,10,2),engine=engine,duckdb_threads=1)
    assert actual.to_pylist()==[{'symbol':'0050','close':None}]
    quoted=read_daily_projection(source,columns=['symbol','close'],symbols=["0050' OR true --"],
                                 start_date=date(2026,10,1),engine=engine,duckdb_threads=1)
    assert quoted.num_rows==0
    finite=read_daily_projection(source,columns=['symbol','close'],symbols=['0050'],
                                 start_date=date(2026,10,1),end_date=date(2026,10,1),engine=engine,duckdb_threads=1)
    assert finite['close'][0].as_py()==123.456789


def test_daily_projection_does_not_reinterpret_timestamp_or_numeric_symbol(tmp_path):
    source=tmp_path/'wrong-grain.parquet'
    pq.write_table(pa.table({'date':['2026-10-01'],'symbol':['0050']}),source)
    with pytest.raises(ValueError,match='reinterpret'):
        read_daily_projection(source,columns=['symbol'],symbols=['0050'],start_date=date(2026,10,1))
    with pytest.raises(ValueError,match='strings'):
        read_daily_projection(source,columns=['symbol'],symbols=[50],start_date=date(2026,10,1))


def test_source_signature_is_order_independent_and_contract_sensitive() -> None:
    first = SourceFileContract("a", "/a", 2, 100, 10)
    second = SourceFileContract("b", "/b", 3, 200, 20)
    assert source_signature([first, second]) == source_signature([second, first])
    changed = SourceFileContract("b", "/b", 4, 200, 20)
    assert source_signature([first, second]) != source_signature([first, changed])


def test_compact_parquet_files_unions_schema_and_validates_three_engines(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    output = tmp_path / "compact" / "segment.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": "2024-01-01", "close": 1.0},
                {"date": "2024-01-02", "close": 2.0},
            ]
        ),
        first,
        compression="snappy",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [{"date": "2024-01-03", "close": 3.0, "volume": 100}]
        ),
        second,
        compression="snappy",
    )

    receipt = compact_parquet_files(
        [first, second],
        output,
        expected_rows=3,
        threads=1,
        memory_limit="512MB",
        row_group_size_rows=2,
    )

    assert receipt.source_files == 2
    assert receipt.source_rows == receipt.output_rows == 3
    assert receipt.pyarrow_rows == receipt.polars_rows == receipt.duckdb_rows == 3
    assert receipt.compression == "zstd"
    assert receipt.compression_level == 3
    assert receipt.date_column == "date"
    assert receipt.min_date == "2024-01-01"
    assert receipt.max_date == "2024-01-03"
    assert output.is_file()
    assert not list(output.parent.glob(".*.tmp"))
    assert "volume" in pq.ParquetFile(output).schema_arrow.names
    assert pl.read_parquet(output).height == 3
    with duckdb.connect(":memory:") as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(output)]
            ).fetchone()[0]
            == 3
        )
