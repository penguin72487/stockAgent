"""The option change is a source event, not automatic account admission."""
from decimal import Decimal

import pytest

from stockagent.data.tw_option_contract_terms import gold_option_2026_resize


SIZE = ('「黃金選擇權契約」規格修正對照表修 正 條 文 現 行 條 文 說 明 '
        '契約規模 ● 1 台兩（10 台錢、37.5 公克） '
        '契約規模 ● 5 台兩（50 台錢、187.5 公克） '
        '權利金報價單位 ● 0.5 點（新臺幣5 元） '
        '權利金報價單位 ● 0.5 點（新臺幣25 元）')
RULE = ('「黃金選擇權契約」交易規則修正條文對照表修 正 條 文 現 行 條 文 說 明 '
        '前項履約價格乘數為十。前項履約價格乘數為五十。'
        '權利金乘數為新臺幣十元，權利金乘數為新臺幣五十元')
NOTICE = ('發文字號：台期交字第1150201214號 '
          '本公司115年6月12日台期交字第1150200958號函相關規章修正條文自115年7月6日起實施。'
          '7月6日開盤前之各契約未沖銷部位按調降比例進行調整，黃金期貨、臺幣黃金期貨及'
          '非金電期貨之未沖銷部位，1口調整為10口；另黃金選擇權之未沖銷部位，1口調整為5口。')


def test_extracted_event_preserves_signed_open_exposure_without_creating_a_fill():
    result = gold_option_2026_resize([SIZE, RULE], [NOTICE])
    assert result['effective_date'] == '2026-07-06'
    assert result['effective_phase'] == 'before_open'
    assert result['point_in_time_verified'] is False and result['training_ready'] is False
    assert result['old']['premium_multiplier_twd_per_point'] == '50'
    assert result['new']['premium_multiplier_twd_per_point'] == '10'
    assert result['open_position_split'] == {'old_contracts': 1, 'new_contracts': 5}
    for signed_contracts in [-3, 0, 4]:
        old = Decimal(signed_contracts) * Decimal(result['old']['premium_multiplier_twd_per_point'])
        new = Decimal(signed_contracts * 5) * Decimal(result['new']['premium_multiplier_twd_per_point'])
        assert old == new
    assert 'known_at' not in result and 'execution_price' not in result


@pytest.mark.parametrize('size,rule,notice', [
    (SIZE.replace('0.5 點（新臺幣25 元）', '0.5 點（新臺幣20 元）'), RULE, NOTICE),
    (SIZE.replace('5 台兩', '4 台兩'), RULE, NOTICE),
    (SIZE, RULE.replace('前項履約價格乘數為五十。', ''), NOTICE),
    (SIZE, RULE.replace('前項履約價格乘數為十。前項履約價格乘數為五十。',
                        '前項履約價格乘數為五十。前項履約價格乘數為十。'), NOTICE),
    (SIZE, RULE, NOTICE.replace('調整為5口', '調整為10口')),
    (SIZE, RULE, NOTICE.replace('7月6日開盤前', '7月7日開盤前')),
    (SIZE, RULE, NOTICE.replace('1150200958號', '1150200959號')),
    (SIZE.replace('現 行 條 文', '未明條文'), RULE, NOTICE),
    (SIZE, RULE, NOTICE.replace('開盤前', '開盤後')),
])
def test_rejects_missing_ambiguous_or_inconsistent_source(size, rule, notice):
    with pytest.raises(ValueError):
        gold_option_2026_resize([size, rule], [notice])


def test_rejects_duplicate_specification_pages():
    with pytest.raises(ValueError, match='one gold-option'):
        gold_option_2026_resize([SIZE, SIZE, RULE], [NOTICE])
