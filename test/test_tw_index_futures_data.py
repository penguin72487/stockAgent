from __future__ import annotations

import csv
from pathlib import Path
import zipfile

import numpy as np
import pytest

from stockagent.data.tw_index_futures import (
    build_taifex_index_futures_day_session,
    load_taifex_index_futures_day_session,
)
from stockagent.data.tw_all_futures import (
    build_taifex_all_futures_front_panel,
    build_taifex_all_futures_front_panels,
    load_taifex_all_futures_afterhours_context,
    load_taifex_all_futures_front_context,
)


_HEADER = [
    "交易日期",
    "契約",
    "到期月份(週別)",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "結算價",
    "成交量",
    "未沖銷契約數",
    "最後最佳買價",
    "最後最佳賣價",
    "歷史最高價",
    "歷史最低價",
    "是否因訊息面暫停交易",
    "交易時段",
    "價差對單式委託成交量",
]


def _write_csv(path: Path, rows: list[list[object]]) -> None:
    with path.open("w", encoding="cp950", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(_HEADER)
        writer.writerows(rows)


def _write_legacy_csv(path: Path, rows: list[list[object]]) -> None:
    session_index = _HEADER.index("交易時段")
    legacy_header = [
        value for index, value in enumerate(_HEADER) if index != session_index
    ]
    legacy_rows = [
        [value for index, value in enumerate(row) if index != session_index]
        for row in rows
    ]
    with path.open("w", encoding="cp950", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(legacy_header)
        writer.writerows(legacy_rows)


def _row(
    date: str,
    product: str,
    contract: str,
    open_price: float,
    close_price: float,
    *,
    session: str = "一般",
    volume: int = 100,
) -> list[object]:
    return [
        date,
        product,
        contract,
        open_price,
        max(open_price, close_price) + 10,
        min(open_price, close_price) - 10,
        close_price,
        close_price,
        volume,
        0,
        close_price,
        close_price,
        close_price,
        close_price,
        "",
        session,
        0,
    ]


def test_build_filters_session_weekly_and_selects_front_month(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "source.csv"
    rows = [
        _row("2025/01/02", "TX", "202501", 20000, 20100),
        _row("2025/01/02", "TX", "202502", 20010, 20110),
        _row(
            "2025/01/02",
            "TX",
            "202501",
            19900,
            19800,
            session="盤後",
        ),
        _row("2025/01/02", "MTX", "202501", 20000, 20100),
        _row("2025/01/02", "MTX", "202501W1", 20020, 20120),
        _row("2025/01/02", "TMF", "202501", 20000, 20100),
        _row("2025/01/03", "TX", "202501", 20100, 20000),
        _row("2025/01/03", "MTX", "202501", 20100, 20000),
        _row("2025/01/03", "TMF", "202501", 20100, 20000),
    ]
    _write_csv(csv_path, rows)
    output = build_taifex_index_futures_day_session(
        [csv_path],
        tmp_path / "normalized.parquet",
    )
    market = load_taifex_index_futures_day_session(output)

    import pyarrow.parquet as pq

    normalized = pq.read_table(output)
    # The v2 parquet retains the deferred month required to compute a real roll.
    assert normalized.num_rows == 7
    assert int(sum(normalized["is_front_month"].to_pylist())) == 6

    assert market.products == ("TX", "MTX", "TMF")
    assert market.contract_months[0].tolist() == [
        "202501",
        "202501",
        "202501",
    ]
    np.testing.assert_allclose(market.open_prices[0], [20000, 20000, 20000])
    np.testing.assert_allclose(
        market.log_returns[0],
        np.log(np.asarray([20100, 20100, 20100]) / 20000),
    )
    assert market.tradable_mask.all()
    np.testing.assert_array_equal(market.multipliers, [200, 50, 10])


def test_rolling_buy_hold_uses_new_contract_prior_close_on_roll(
    tmp_path: Path,
) -> None:
    source = tmp_path / "roll.csv"
    _write_csv(
        source,
        [
            _row("2025/01/02", "TX", "202501", 100, 100),
            _row("2025/01/02", "TX", "202502", 110, 110),
            _row("2025/01/03", "TX", "202501", 100, 101),
            _row("2025/01/03", "TX", "202502", 110, 111),
            # The January contract is no longer listed. February becomes front.
            _row("2025/01/06", "TX", "202502", 111, 112),
        ],
    )
    output = build_taifex_index_futures_day_session(
        [source],
        tmp_path / "contracts.parquet",
        products=("TX",),
    )
    market = load_taifex_index_futures_day_session(output, products=("TX",))

    np.testing.assert_array_equal(
        market.contract_months[:, 0],
        ["202501", "202501", "202502"],
    )
    rolling = market.reference_rolling_buy_hold_log_returns("TX")
    assert np.isnan(rolling[0])
    assert rolling[1] == pytest.approx(np.log(101 / 100))
    # Crucial: no artificial 112/101 old/new-contract price jump.
    assert rolling[2] == pytest.approx(np.log(112 / 111))
    np.testing.assert_array_equal(
        market.reference_front_month_roll_mask("TX"),
        [False, False, True],
    )


def test_build_reads_cp950_csv_inside_zip(tmp_path: Path) -> None:
    csv_path = tmp_path / "inside.csv"
    _write_csv(
        csv_path,
        [
            _row("2025/01/02", "TX", "202501", 20000, 20100),
            _row("2025/01/02", "MTX", "202501", 20000, 20100),
            _row("2025/01/02", "TMF", "202501", 20000, 20100),
        ],
    )
    archive_path = tmp_path / "annual.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(csv_path, arcname="fut.csv")

    output = build_taifex_index_futures_day_session(
        [archive_path],
        tmp_path / "normalized.parquet",
    )
    market = load_taifex_index_futures_day_session(output)
    assert market.dates.tolist() == [np.datetime64("2025-01-02")]


def test_legacy_schema_without_session_column_is_day_only(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.csv"
    _write_legacy_csv(
        source,
        [
            _row("2005/1/3", "TX", "200501", 6000, 6010),
            _row("2005/1/3", "MTX", "200501", 6000, 6010),
        ],
    )
    output = build_taifex_index_futures_day_session(
        [source],
        tmp_path / "normalized.parquet",
    )
    market = load_taifex_index_futures_day_session(output)
    assert market.tradable_mask[0, :2].all()
    assert not market.tradable_mask[0, 2]


def test_overlapping_sources_must_agree(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    _write_csv(first, [_row("2025/01/02", "TX", "202501", 20000, 20100)])
    _write_csv(second, [_row("2025/01/02", "TX", "202501", 20000, 20200)])

    with pytest.raises(ValueError, match="conflicting TAIFEX rows"):
        build_taifex_index_futures_day_session(
            [first, second],
            tmp_path / "normalized.parquet",
        )


def test_all_product_context_keeps_every_root_and_is_prior_session_only(
    tmp_path: Path,
) -> None:
    source = tmp_path / "all.csv"
    _write_csv(
        source,
        [
            _row("2025/01/02", "TX", "202501", 20000, 20100),
            _row("2025/01/02", "TX", "202502", 20010, 20110),
            _row("2025/01/02", "TE", "202501", 1000, 1010),
            _row("2025/01/03", "TX", "202501", 20100, 20000),
            _row("2025/01/03", "TE", "202501", 1010, 1000),
        ],
    )
    normalized = build_taifex_all_futures_front_panel(
        [source], tmp_path / "all_front.parquet"
    )
    features, mask, roots = load_taifex_all_futures_front_context(
        normalized,
        panel_dates=np.asarray(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
    )

    assert roots == ("TE", "TX")
    assert features.shape == (2, 2, 13)
    assert not mask[0].any()
    assert mask[1].all()
    tx = roots.index("TX")
    assert features[1, tx, 0] == pytest.approx(np.log(20100 / 20000))


def test_all_product_afterhours_context_is_visible_on_attributed_session(
    tmp_path: Path,
) -> None:
    source = tmp_path / "all_sessions.csv"
    _write_csv(
        source,
        [
            _row("2025/01/02", "TX", "202501", 20000, 20100),
            _row(
                "2025/01/02",
                "TX",
                "202501",
                20120,
                20200,
                session="盤後",
            ),
            _row("2025/01/03", "TX", "202501", 20210, 20150),
            _row(
                "2025/01/03",
                "TX",
                "202501",
                20160,
                20300,
                session="盤後",
            ),
        ],
    )
    outputs = build_taifex_all_futures_front_panels(
        [source],
        {
            "regular": tmp_path / "regular.parquet",
            "afterhours": tmp_path / "afterhours.parquet",
        },
    )
    panel_dates = np.asarray(
        ["2025-01-02", "2025-01-03", "2025-01-06"],
        dtype="datetime64[D]",
    )
    day_features, day_mask, day_roots = load_taifex_all_futures_front_context(
        outputs["regular"], panel_dates=panel_dates
    )
    night_features, night_mask, night_roots = (
        load_taifex_all_futures_afterhours_context(
            outputs["afterhours"], panel_dates=panel_dates
        )
    )

    assert day_roots == night_roots == ("TX",)
    assert not day_mask[0, 0] and night_mask[0, 0]
    assert night_features[0, 0, 0] == pytest.approx(np.log(20200 / 20120))
    assert day_features[1, 0, 0] == pytest.approx(np.log(20100 / 20000))
    assert night_features[1, 0, 0] == pytest.approx(np.log(20300 / 20160))
    # There is no fabricated forward-fill into Monday when no attributed
    # after-hours row exists for Monday.
    assert not night_mask[2, 0]
