"""Financial values and missingness must survive independent storage readers."""
from datetime import date
import math

import pyarrow as pa
import pytest

from scripts.verify_lakehouse_engines import same_values,private_database_options


def values():
    return pa.table({'date':[date(2026,10,1),date(2026,10,2),date(2026,10,3)],
                     'price':[123.456789,None,float('nan')],
                     'code':['0050','2330','2317']})


def test_row_order_and_string_offset_representation_preserve_source_values():
    source=values()
    other=source.take([2,0,1]).cast(pa.schema([('date',pa.date32()),('price',pa.float64()),('code',pa.large_string())]))
    same_values(other,source)


@pytest.mark.parametrize('prices',[[123.456788,None,float('nan')],
                                  [123.456789,float('nan'),float('nan')],
                                  [123.456789,None,0.]])
def test_rounding_null_to_nan_or_fabricating_missing_values_is_rejected(prices):
    source=values()
    other=source.set_column(1,'price',pa.array(prices))
    with pytest.raises(ValueError):same_values(other,source)


def test_missing_observation_or_provenance_column_is_rejected():
    source=values()
    for changed in (source.slice(0,2),source.drop(['code'])):
        with pytest.raises(ValueError):same_values(changed,source)


def test_trial_catalog_refuses_public_or_unknown_credentials(tmp_path):
    path=tmp_path/'control.env';path.chmod(0o600) if path.exists() else None
    for dsn in ('postgresql://stockagent_control:x@example.com/stockagent_control',
                'postgresql://unknown:x@127.0.0.1/stockagent_control'):
        path.write_text('CONTROL_PLANE_DSN='+dsn+'\n');path.chmod(0o600)
        with pytest.raises(ValueError,match='loopback'):private_database_options(path)
    path.chmod(0o644)
    with pytest.raises(ValueError,match='private'):private_database_options(path)
