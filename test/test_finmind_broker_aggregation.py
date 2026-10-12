from decimal import localcontext

import pyarrow as pa
import pytest

from downloader.finmind_broker_aggregation import aggregate_brokers_columnar
from downloader.finmind_broker_aggregation import aggregate_brokers_table, _weighted_prices
from downloader.finmind_supplemental import aggregate_brokers


def broker(**changes):
    return {'date': '2026-10-05', 'stock_id': '2330', 'securities_trader_id': '1020',
            'securities_trader': 'first name', 'price': 100.15, 'buy': 1001, 'sell': 300, **changes}


def test_columnar_preserves_exact_decimal_output_order_first_name_and_duplicates():
    rows = [broker(), broker(securities_trader='later name', price=100.1, buy=17),
            broker(stock_id='2317', buy=0, sell=0), broker(), broker(price=100.005)]
    assert aggregate_brokers_columnar(pa.Table.from_pylist(rows)) == aggregate_brokers(rows)


@pytest.mark.parametrize('price', [0., .01, .1, 10.15, 1080.5, 500000.25])
def test_prices_use_decimal_strings_not_binary_float_products(price):
    rows = [broker(price=price, buy=987654321), broker(price=price+.01, buy=99999999)]
    assert aggregate_brokers_columnar(pa.Table.from_pylist(rows)) == aggregate_brokers(rows)


@pytest.mark.parametrize('mutation', [{'price': float('nan')}, {'price': float('inf')},
                                     {'price': -1}, {'buy': -1}, {'sell': None}])
def test_invalid_values_are_rejected_before_aggregation(mutation):
    with pytest.raises(ValueError, match='invalid_broker_quantity_or_price'):
        aggregate_brokers_columnar(pa.Table.from_pylist([broker(**mutation)]))


def test_fractional_quantities_and_wide_products_explicitly_keep_reference():
    assert aggregate_brokers_columnar(pa.Table.from_pylist([broker(buy=1.5)])) is None
    assert aggregate_brokers_columnar(pa.Table.from_pylist([broker(price=1e30)])) is None
    with localcontext() as context:
        context.prec = 16
        assert aggregate_brokers_columnar(pa.Table.from_pylist([broker()])) is None


def test_empty_rows_and_required_fields():
    table = pa.Table.from_pylist([broker()]).slice(0, 0)
    assert aggregate_brokers_columnar(table) == []
    with pytest.raises(ValueError):
        aggregate_brokers_columnar(table.drop(['securities_trader']))


def test_parallel_grouping_keeps_first_name_and_exact_wide_coefficients():
    rows = [broker(buy=2**61, price=1000.01), broker(stock_id='2317', price=100.1),
            broker(stock_id='2317', securities_trader='later', price=1.01)]
    observed = aggregate_brokers_table(pa.Table.from_pylist(rows), use_threads=True)
    assert observed.to_pylist() == aggregate_brokers(rows)


def test_chunked_sliced_decimal_buffers_keep_scale_and_offset():
    from decimal import Decimal
    pieces = [pa.array([Decimal('999.00'), Decimal('10.01')], pa.decimal128(28, 2)).slice(1),
              pa.array([Decimal('184467440737095516.17'), Decimal('0.00')], pa.decimal128(28, 2))]
    amounts = pa.chunked_array(pieces)
    volumes = pa.array([3, 5, 0], type=pa.int64())
    assert _weighted_prices(amounts, volumes).to_pylist() == [
        float(Decimal('10.01') / 3), float(Decimal('184467440737095516.17') / 5), 0.0]


def test_many_groups_random_decimal_prices_preserve_reference_values():
    import random
    rng = random.Random(24601)
    rows = [broker(stock_id=str(rng.randrange(80)), securities_trader_id=str(rng.randrange(10)),
                   price=rng.randrange(1, 10000000) / 100,
                   buy=rng.randrange(100000), sell=rng.randrange(100000)) for _ in range(10000)]
    assert aggregate_brokers_table(pa.Table.from_pylist(rows)).to_pylist() == aggregate_brokers(rows)
