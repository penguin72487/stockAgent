import pytest

from scripts.publish_txo_friday_20261023_notice import notice_facts


TYPHOON = ('<html><p>2026/10/22 因颱風115年10月23日休市，'
           '臺指選擇權週五到期契約，最後交易日及最後結算日順延至最近之次一營業日。</p></html>').encode()
HOLIDAY = '台期交字第1150201963號 10月26日(星期一)補假2日，本公司期貨集中交易市場配合休市2日。'


def test_notice_exposes_conditional_date_only():
    facts = notice_facts(TYPHOON, HOLIDAY)
    assert facts['relative_rule'] == 'next_business_day'
    assert facts['october_26_closed'] is True
    assert facts['candidate_if_no_additional_closure'] == '2026-10-27'
    assert 'actual_settlement_date' not in facts


@pytest.mark.parametrize('typhoon,holiday', [
    (TYPHOON.replace('週五'.encode(), '週三'.encode()), HOLIDAY),
    (TYPHOON, HOLIDAY.replace('10月26日', '10月27日')),
])
def test_notice_rejects_wrong_scope(typhoon, holiday):
    with pytest.raises(ValueError):
        notice_facts(typhoon, holiday)
