"""Exact columnar acceleration of the canonical broker-detail derivation.

Keep Decimal arithmetic and first-source broker names. Unsupported numerical
shapes return None so the existing arbitrary-precision row implementation owns
them; this is not a float reduction or a replacement provider observation.
"""
from __future__ import annotations

from decimal import Decimal, getcontext
import sys

import pyarrow as pa
import pyarrow.compute as pc
import numpy as np


KEYS = ('date', 'stock_id', 'securities_trader_id')
FIELDS = (*KEYS, 'securities_trader', 'price', 'buy', 'sell')


def _weighted_prices(amounts: pa.ChunkedArray, volumes: pa.Array) -> pa.Array:
    """Exact rational Decimal division, without millions of Decimal scalars.

    Decimal128 is a scaled signed 128-bit integer. Interpret its own validated
    little-endian buffer, including array offsets; big-endian hosts keep the
    portable Arrow conversion. No floating-point multiplication/reduction.
    """
    amount = amounts.combine_chunks()
    if sys.byteorder != 'little':
        return pa.array([float(a / int(v)) if v else 0.0 for a, v in
                         zip(amount.to_pylist(), volumes.to_numpy(), strict=True)], type=pa.float64())
    words = np.frombuffer(amount.buffers()[1], dtype='<u8').reshape(-1, 2)[amount.offset:amount.offset + len(amount)]
    denominator_scale = 10 ** amount.type.scale
    low_only = not np.any(words[:, 1])
    coefficients = words[:, 0] if low_only else ((int(high) << 64) | int(low) for low, high in words)
    return pa.array([
        float(Decimal(int(coefficient)) / Decimal(int(volume) * denominator_scale)) if volume else 0.0
        for coefficient, volume in zip(coefficients, volumes.to_numpy(zero_copy_only=True), strict=True)
    ], type=pa.float64())


def aggregate_brokers_table(table: pa.Table, *, use_threads: bool = True) -> pa.Table | None:
    if not set(FIELDS) <= set(table.column_names):
        raise ValueError('invalid_broker_quantity_or_price')
    if not table.num_rows:
        return pa.table({name: [] for name in (*KEYS, 'securities_trader', 'buy_volume', 'buy_price', 'sell_volume', 'sell_price')})
    if any(table[name].null_count for name in FIELDS):
        raise ValueError('invalid_broker_quantity_or_price')
    # Integer source shares are exact. Fractional/string quantities retain the
    # canonical Decimal(str(value)) path rather than being truncated or rounded.
    if (getcontext().prec != 28 or any(not pa.types.is_integer(table[name].type) for name in ('buy', 'sell'))
            or not (pa.types.is_floating(table['price'].type) or pa.types.is_integer(table['price'].type))):
        return None
    for name in ('price', 'buy', 'sell'):
        if (pc.any(pc.invert(pc.is_finite(table[name]))).as_py()
                or pc.any(pc.less(table[name], 0)).as_py()):
            raise ValueError('invalid_broker_quantity_or_price')

    prices = pc.cast(table['price'], pa.string())
    # Arrow emits the shortest round-trippable decimal string, not the binary
    # float expansion. Scientific notation and wide/ultra-precise inputs stay
    # with the reference path so the Decimal context is never silently changed.
    if pc.any(pc.match_substring_regex(prices, '[eE]')).as_py():
        return None
    fractional = pc.if_else(pc.match_substring_regex(prices, r'\.'),
                            pc.utf8_length(pc.replace_substring_regex(prices, r'^\d+\.', '')), 0)
    scale = pc.max(fractional).as_py()
    digits = pc.max(pc.utf8_length(pc.replace_substring_regex(prices, r'\.\d+$', ''))).as_py()
    precision = max(1, digits + scale)
    # A 20-digit quantity * price must fit Decimal128. The row reference uses
    # the default 28-digit context; do not promote products that would need it.
    if scale > 12 or precision + 20 > 28:
        return None
    price = pc.cast(prices, pa.decimal128(precision, scale))
    quantities = {side: pc.cast(table[side], pa.decimal128(20, 0)) for side in ('buy', 'sell')}
    projected = pa.table({
        **{name: pc.cast(table[name], pa.string()) for name in KEYS},
        '_source_row': pa.array(np.arange(table.num_rows, dtype=np.int64)),
        **quantities,
        **{side + '_amount': pc.multiply(quantity, price) for side, quantity in quantities.items()},
    })
    grouped = projected.group_by(list(KEYS), use_threads=use_threads).aggregate([
        ('_source_row', 'min'), ('buy', 'sum'), ('sell', 'sum'),
        ('buy_amount', 'sum'), ('sell_amount', 'sum'),
    ]).sort_by([(name, 'ascending') for name in KEYS])
    # Positive input terms are monotone. If the final fixed-point sum needs
    # more than the reference's 28 significant digits, intermediate Decimal
    # rounding could matter; retain the row oracle rather than changing it.
    for side in ('buy', 'sell'):
        if (len(pc.max(grouped[side + '_amount_sum']).as_py().as_tuple().digits) > 28
                or pc.max(grouped[side + '_sum']).as_py() > 2**63 - 1):
            return None
    # Associative min(source-row) preserves the reference's FIRST name even
    # when Arrow groups in parallel. Only the exact weighted divisions cross
    # into Python Decimal; keys and output stay columnar through publication.
    output = {name: grouped[name] for name in KEYS}
    output['securities_trader'] = pc.take(table['securities_trader'], grouped['_source_row_min'])
    for side in ('buy', 'sell'):
        quantity = pc.cast(grouped[side + '_sum'], pa.int64()).combine_chunks()
        output[side + '_volume'] = quantity
        output[side + '_price'] = _weighted_prices(grouped[side + '_amount_sum'], quantity)
    return pa.table(output)


def aggregate_brokers_columnar(table: pa.Table) -> list[dict] | None:
    """Compatibility view for small callers; production keeps the Arrow table."""
    grouped = aggregate_brokers_table(table, use_threads=False)
    return grouped.to_pylist() if grouped is not None else None
