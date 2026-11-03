from scripts.audit_tej_option_axis_labels import parse_label, profile


def test_month_and_week_exact_description_roundtrip():
    monthly = parse_label('AAA200307C30==>南亞買權 200307 30')
    weekly = parse_label('TXO202605F2P37750==>加權指數賣權 202605F2 37750')
    assert (monthly['prefix'], monthly['month'], monthly['side']) == ('AAA', '07', 'C')
    assert (weekly['week'], weekly['strike'], weekly['side']) == ('F2', '37750', 'P')


def test_rejects_ambiguous_or_inconsistent_labels():
    for label in ('STKO==>股票選擇權', 'TXO202613C1==>指數買權 202613 1',
                  'TXO202605C1==>指數賣權 202605 1', 'TXO202605C1==>指數買權 202605 2',
                  'TXO202605C1==>指數買權 202605 1==>其他'):
        assert parse_label(label) is None


def test_noncontract_labels_are_retained_and_duplicate_codes_reported():
    stats = profile(['AAA200307C30==>南亞買權 200307 30',
                     'AAA200307C30==>南亞買權 200307 30', 'STKO==>股票選擇權'])
    assert stats['labels'] == 3
    assert stats['candidate_kinds'] == {'monthly': 2}
    assert stats['duplicate_code_strings'] == 1
    assert stats['noncontract_or_unparsed_labels'] == ['STKO==>股票選擇權']


def test_source_whitespace_is_allowed_and_missing_name_is_not_invented():
    gold = 'TGO202210C6050==>黃金選擇權契約買權  202210 6050'
    missing = 'CDB202212C500==>買權  202212 500'
    assert parse_label(gold)['description_prefix'] == '黃金選擇權契約'
    assert parse_label(missing)['description_prefix'] == ''
    assert profile([gold, missing])['candidate_empty_underlying_descriptions'] == 1
