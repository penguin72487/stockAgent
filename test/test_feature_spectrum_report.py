"""Rendered spectrum percentages must agree with the independent CSV oracle."""

import pytest

from scripts.verify_feature_spectrum_report import _verify_inverse_markdown


THRESHOLDS=[
    {'target_percent':'0','dimensions':'0','retained_percent':'0','meaning':'minimum_dimensions'},
    {'target_percent':'25','dimensions':'34','retained_percent':'25.256453188105265','meaning':'minimum_dimensions'},
    {'target_percent':'100','dimensions':'10984','retained_percent':'99.99999999999997',
     'meaning':'untruncated_active_coordinate_guarantee'},
]
REPORT='''# Test report
| 目標能量 % | 維度 | 實際累積能量 % | 定義 |
| ---: | ---: | ---: | --- |
| 0 | 0 | 0.0000000000 | 最少維度 |
| 25 | 34 | 25.2564531881 | 最少維度 |
| 100 | 10,984 | 100.0000000000 | 完整 active、不截斷上界 |
'''


def test_inverse_markdown_checks_rendered_rows_and_safe_100_percent():
    _verify_inverse_markdown(REPORT,THRESHOLDS)


@pytest.mark.parametrize('before,after',[
    ('| 25 | 34 |','| 25 | 35 |'),
    ('| 25 | 34 |','| 26 | 34 |'),
    ('25.2564531881','25.3564531881'),
    ('完整 active、不截斷上界','最少維度'),
    ('| 25 | 34 | 25.2564531881 | 最少維度 |\n',''),
    ('| 25 | 34 | 25.2564531881 | 最少維度 |',
     '| 25 | 34 | 25.2564531881 | 最少維度 |\n| 25 | 34 | 25.2564531881 | 最少維度 |'),
])
def test_inverse_markdown_rejects_wrong_or_missing_rendered_rows(before,after):
    with pytest.raises(AssertionError,match='Markdown'):
        _verify_inverse_markdown(REPORT.replace(before,after),THRESHOLDS)
