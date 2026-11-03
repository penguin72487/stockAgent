import pytest

from stockagent.control.backup import control_backup_capacity

GIB = 1024**3


def config(enrolled=False):
    value = {'reserve_bytes': 64*GIB}
    if enrolled:
        value['lifecycle_sql_backup_capacity'] = {'contract': 'lifecycle_sql_backup_reserve_v1',
            'use_backup_reserve': True, 'user_instruction': 'reserve is for backup'}
    return value


def test_default_reserve_is_preserved_for_unenrolled_backup():
    result = control_backup_capacity(free_bytes=56*GIB, required_growth_bytes=GIB, configuration=config())
    assert result['state'] == 'waiting_capacity' and result['operational_floor_bytes'] == 64*GIB


def test_only_enrolled_sql_backup_can_use_the_pool_with_its_full_growth():
    result = control_backup_capacity(free_bytes=56*GIB, required_growth_bytes=GIB, configuration=config(True))
    assert result['state'] == 'admitted' and result['operational_floor_bytes'] == 32*GIB
    assert result['required_free_bytes'] == 33*GIB and result['ordinary_reserve_bytes'] == 64*GIB
    assert result['source_retirement'] is False and result['nas_acceptance'] is False
    assert control_backup_capacity(free_bytes=33*GIB-1, required_growth_bytes=GIB,
        configuration=config(True))['state'] == 'waiting_capacity'


@pytest.mark.parametrize('key,bad', [('free_bytes', True), ('free_bytes', -1),
                                   ('required_growth_bytes', float('nan'))])
def test_ambiguous_or_negative_capacity_is_rejected(key, bad):
    arguments = {'free_bytes': 56*GIB, 'required_growth_bytes': GIB, 'configuration': config(True)}
    arguments[key] = bad
    with pytest.raises(ValueError): control_backup_capacity(**arguments)


def test_enrollment_without_instruction_cannot_lower_the_floor():
    value = config(True)
    value['lifecycle_sql_backup_capacity']['user_instruction'] = ''
    with pytest.raises(ValueError):
        control_backup_capacity(free_bytes=56*GIB, required_growth_bytes=GIB, configuration=value)
