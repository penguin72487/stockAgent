"""Precision experiments preserve source graphs and retain a separate FP32 control."""
import json

import numpy as np
import pytest


def test_bf16_builder_retains_fp32_io_and_separate_control(tmp_path):
    onnx = pytest.importorskip('onnx')
    from downloader.artifact_io import sha256_file
    from scripts.build_ocr_bf16_candidate import build_candidate
    h, t = onnx.helper, onnx.TensorProto
    source = tmp_path / 'source.onnx'
    graph = h.make_graph([
        h.make_node('Conv', ['x', 'w'], ['conv'], name='convolution'),
        h.make_node('Relu', ['conv'], ['y']),
    ], 'precision', [h.make_tensor_value_info('x', t.FLOAT, [1, 1, 3, 3])],
        [h.make_tensor_value_info('y', t.FLOAT, [1, 1, 3, 3])],
        [onnx.numpy_helper.from_array(np.ones((1, 1, 1, 1), np.float32), 'w')])
    model = h.make_model(graph, opset_imports=[h.make_opsetid('', 14)], ir_version=10)
    h.set_model_props(model, {'character': '甲\n乙', 'stockagent_ctc_recipe': 'retained'})
    onnx.save(model, source)
    original = source.read_bytes()
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'models': {
        'Det': {'path': str(source), 'sha256': sha256_file(source)}}, 'optimizations': {}}))
    output = tmp_path / 'precision'
    receipts = build_candidate(config, output)
    assert source.read_bytes() == original
    assert len(receipts) == 2
    control = onnx.load(output / 'fp32_opset22/Det.onnx')
    candidate = onnx.load(output / 'bf16/Det.onnx')
    onnx.checker.check_model(control, full_check=True)
    onnx.checker.check_model(candidate, full_check=True)
    assert [n.op_type for n in control.graph.node] == ['Conv', 'Relu']
    assert [n.op_type for n in candidate.graph.node] == ['Cast', 'Cast', 'Conv', 'Cast', 'Relu']
    assert [n.attribute[0].i for n in candidate.graph.node if n.op_type == 'Cast'] == [
        t.BFLOAT16, t.BFLOAT16, t.FLOAT]
    assert candidate.graph.input[0].type.tensor_type.elem_type == t.FLOAT
    assert candidate.graph.output[0].type.tensor_type.elem_type == t.FLOAT
    assert candidate.graph.initializer[0].data_type == t.FLOAT
    meta = {p.key: p.value for p in candidate.metadata_props}
    assert meta['character'] == '甲\n乙'
    assert meta['stockagent_precision_source_sha256'] == sha256_file(source)
    assert receipts[1]['converted_nodes'] == [{'name': 'convolution', 'op': 'Conv'}]
    assert json.loads((output / 'bf16.json').read_text())['optimizations']['use_tf32'] is True
    assert not json.loads((output / 'fp32_opset22.json').read_text())['optimizations'].get('use_tf32', False)
    # Reusing a changed upstream source must fail before producing new graphs.
    source.write_bytes(b'corrupted')
    with pytest.raises(ValueError, match='source hash mismatch'):
        build_candidate(config, tmp_path / 'corrupted')
