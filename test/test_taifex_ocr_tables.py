"""OCR cell geometry must not silently drop numbers or merge adjacent cells."""
import pytest


def complete_position_tokens():
    def token(x,y,w,text):
        return dict(txt=text,box=[[x,y],[x+w,y],[x+w,y+20],[x,y+20]])
    return [token(10,100,80,'持有部位'),token(150,100,45,'AAF'),token(300,100,45,'AA1'),
            token(10,150,100,'每口折算股數'),token(140,150,65,'2,000'),token(290,150,65,'2,100'),
            token(10,230,80,'適用期間'),token(140,230,110,'自107.07.26起至'),
            token(330,230,180,'自107.09.20起至AA1契約'),token(140,260,110,'107.09.19止'),
            token(330,260,180,'終止掛牌前一營業日止'),
            token(10,320,60,'自然人'),token(200,320,80,'4,400,000股'),token(400,320,80,'4,000,000股'),
            token(10,370,80,'法人機構'),token(200,370,85,'13,200,000股'),token(400,370,85,'12,000,000股'),
            token(10,420,60,'造市者'),token(200,420,85,'33,000,000股'),token(400,420,85,'30,000,000股')]


def test_complete_polygon_axes_recover_cells_without_scan_rules():
    from copy import deepcopy
    from scripts.extract_taifex_rule_review_candidates import position_polygon_tables
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    tokens=complete_position_tokens();saved=deepcopy(tokens)
    tables=position_polygon_tables(tokens)
    assert tokens==saved and len(tables)==2
    assert tables[0]['cells']==[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]
    assert tables[1]['cells'][1]==['自然人','4,400,000股','4,000,000股']
    own=dict(from_product='AAF',product='AA1',effective_date='2018-07-26',contract_multiplier=2100.)
    page=dict(page=3,native_text='AA1與AAF部位合併計算。',tables=tables)
    rows=corporate_position_table_candidates([page],corporate=[own])
    assert {(r['product'],r['effective_date'],r['natural_person_limit']) for r in rows}=={
        (c,day,amount) for c in ('AAF','AA1') for day,amount in
        [('2018-07-26',4400000.),('2018-09-20',4000000.)]}
    for table in tables:
        proof=table['position_polygon_structure']
        assert all(dict(saved[item['index']],index=item['index'])==item for item in proof['original_tokens'])
        assert sorted(i for cell in table['cell_evidence'] for i in cell['token_indices'])==proof['token_indices']


@pytest.mark.parametrize('problem',['missing_amount','missing_category','duplicate_category',
    'duplicate_code','wrong_units','crossed_unit_column','crossed_person_row','crossed_header',
    'third_period','unowned_header','extra_amount','nonfinite_box','merged_person_token'])
def test_polygon_fallback_rejects_incomplete_or_ambiguous_axes(problem):
    from copy import deepcopy
    from scripts.extract_taifex_rule_review_candidates import position_polygon_tables
    tokens=complete_position_tokens()
    if problem=='missing_amount':tokens.pop(13)
    if problem=='missing_category':tokens.pop(17)
    if problem=='duplicate_category':tokens.append(deepcopy(tokens[11]))
    if problem=='duplicate_code':tokens[2]['txt']='AAF'
    if problem=='wrong_units':tokens[4]['txt']='0'
    if problem=='crossed_unit_column':tokens[4]['box']=[[130,150],[350,150],[350,170],[130,170]]
    if problem=='crossed_person_row':tokens[13]['box']=deepcopy(tokens[16]['box'])
    if problem=='crossed_header':tokens[7]['box'][1][0]=tokens[7]['box'][2][0]=450
    if problem=='third_period':tokens.append(dict(txt='自107.10.20起',box=[[350,280],[450,280],[450,300],[350,300]]))
    if problem=='unowned_header':tokens.append(dict(txt='不明文字',box=[[220,280],[440,280],[440,300],[220,300]]))
    if problem=='extra_amount':tokens.append(dict(txt='1,000股',box=[[600,320],[670,320],[670,340],[600,340]]))
    if problem=='nonfinite_box':tokens[4]['box'][0][0]=float('nan')
    if problem=='merged_person_token':tokens[13]['box']=[[400,300],[480,300],[480,380],[400,380]]
    assert not position_polygon_tables(tokens)


def test_grid_fallback_needs_no_new_image_recognition():
    pytest.importorskip('cv2')
    import numpy as np
    from scripts.extract_taifex_rule_review_candidates import ruled_ocr_tables
    tables=ruled_ocr_tables(np.full((500,600),255,dtype=np.uint8),complete_position_tokens())
    assert len(tables)==2 and all('position_polygon_structure' in t for t in tables)


def position_merged_row():
    def token(x,y,text):
        return dict(txt=text,box=[[x,y],[x+70,y],[x+70,y+16],[x,y+16]])
    tokens=[token(15,110,'自然人'),token(15,150,'法人機構'),
            token(115,110,'4,000,000股'),token(115,150,'12,000,000股'),
            token(215,110,'2,000,000股'),token(215,150,'6,000,000股')]
    table=dict(cells=[['適用期間','自100.01.01起至100.09.21止','自100.09.22起至AA1契約終止掛牌前一營業日止'],
                      ['自然人\n法人機構','4,000,000股\n12,000,000股','2,000,000股\n6,000,000股']],
        cell_evidence=[dict(row=1,column=c,bbox=[c*100,100,(c+1)*100,180],
            token_indices=ids) for c,ids in enumerate([[0,1],[2,3],[4,5]])],excluded_boundary_tokens=[])
    return table,tokens


def test_merged_person_rows_use_retained_vertical_ownership_without_ocr():
    from copy import deepcopy
    from scripts.extract_taifex_rule_review_candidates import refine_position_ocr_cells
    table,tokens=position_merged_row();saved=deepcopy(table)
    result=refine_position_ocr_cells(table,tokens)
    assert result['cells'][1:]==[['自然人','4,000,000股','2,000,000股'],
                                ['法人機構','12,000,000股','6,000,000股']]
    assert result['position_cell_refinement']['original_cells']==saved['cells']
    assert table==saved
    assert refine_position_ocr_cells(result,tokens)==result


@pytest.mark.parametrize('problem',['missing_amount','crossed_rows','crossed_columns','unknown_person','altered_cell'])
def test_unproved_person_row_ownership_does_not_guess_amounts(problem):
    from scripts.extract_taifex_rule_review_candidates import refine_position_ocr_cells
    table,tokens=position_merged_row()
    if problem=='missing_amount':
        table['cells'][1][2]='2,000,000股';table['cell_evidence'][2]['token_indices']=[4]
    if problem=='crossed_rows':tokens[4]['box']=tokens[5]['box']
    if problem=='crossed_columns':tokens[4]['box']=tokens[2]['box']
    if problem=='unknown_person':
        tokens[1]['txt']='其他人';table['cells'][1][0]='自然人\n其他人'
    if problem=='altered_cell':table['cells'][1][1]='8,000,000股\n12,000,000股'
    if problem=='altered_cell':
        with pytest.raises(ValueError,match='differs'):refine_position_ocr_cells(table,tokens)
    else:assert refine_position_ocr_cells(table,tokens)==table


@pytest.mark.parametrize('spanning',[False,True])
def test_missing_label_requires_unique_glyph_within_actual_numeric_row(spanning):
    from scripts.extract_taifex_rule_review_candidates import refine_position_ocr_cells
    label='自然人' if spanning else '每口折算股數'
    text='適用期間\n自然人' if spanning else '持有部位'
    tokens=[dict(txt=part,box=[[10,y],[70,y],[70,y+15],[10,y+15]])
            for part,y in [(text.splitlines()[0],10),(label,65)]]
    table=dict(cells=[[text,'AAF','AA1'],[None,'2,000','2,100']],
        cell_evidence=[dict(row=0,column=0,bbox=[0,0,100,100 if spanning else 50],
                            token_indices=[0,1] if spanning else [0]),
                       *[dict(row=1,column=c,bbox=[c*100,50,(c+1)*100,100],token_indices=[])
                         for c in [1,2]]],excluded_boundary_tokens=[])
    result=refine_position_ocr_cells(table,tokens)
    assert result['cells'][1][0]==label
    assert result['cells'][0][0]==text.splitlines()[0]
    tokens[1]['box']=[[105,65],[175,65],[175,80],[105,80]]
    assert refine_position_ocr_cells(table,tokens)==table


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
