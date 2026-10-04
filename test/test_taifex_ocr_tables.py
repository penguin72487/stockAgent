"""OCR cell geometry must not silently drop numbers or merge adjacent cells."""
import pytest


def test_only_exactly_white_render_without_native_text_is_blank():
    from types import SimpleNamespace
    from scripts.extract_taifex_rule_review_candidates import is_blank_render
    assert is_blank_render(SimpleNamespace(samples_mv=memoryview(bytes([255] * 20))), '')
    assert not is_blank_render(SimpleNamespace(samples_mv=memoryview(bytes([255] * 19 + [254]))), '')
    assert not is_blank_render(SimpleNamespace(samples_mv=memoryview(bytes([255] * 20))), '8,000')


def test_cell_padding_preserves_text_but_rejects_tokens_across_two_cells():
    cv2=pytest.importorskip('cv2')
    import numpy as np
    from scripts.extract_taifex_rule_review_candidates import ruled_ocr_tables
    page=np.full((300,400),255,dtype=np.uint8)
    for y in (50,100,150):cv2.line(page,(50,y),(350,y),0,2)
    for x in (50,150,250,350):cv2.line(page,(x,50),(x,150),0,2)
    def token(left,top,right,bottom,text):
        return dict(box=[[left,top],[right,top],[right,bottom],[left,bottom]],txt=text)
    tokens=[token(65,47,130,103,'8,000'),
            token(165,110,230,140,'CQ'),
            token(240,58,335,94,'2,000 4,000'),
            token(50,28,220,52,'提高部位限制數之契約')]
    tables=ruled_ocr_tables(page,tokens)
    assert len(tables)==1
    table=tables[0]
    assert table['cells'][0][0]=='8,000'
    assert table['cells'][1][1]=='CQ'
    assert not any('2,000' in (cell or '') for row in table['cells'] for cell in row)
    assert table['excluded_boundary_tokens']==[2]
    assert table['token_whitespace_padding_max_pixels']==8
    assert '提高部位限制數之契約' in table['caption']
