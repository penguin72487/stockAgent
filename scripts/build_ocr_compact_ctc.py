"""Derive reduced CTC outputs from a verified local ONNX recognition model.

No weights, softmax, dictionary or input shapes change. ONNX is needed only
at build time. The runtime binds both source and derived SHA-256 hashes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file

RECIPE = 'ctc_argmax_max_nan_v1'


def compact_model(source, expected_sha256, output):
    import onnx
    from onnx import TensorProto, helper
    if sha256_file(source) != expected_sha256:
        raise ValueError('recognition source model hash mismatch')
    model = onnx.load(str(source))
    opset = next(o.version for o in model.opset_import if o.domain == '')
    if opset != 10 or len(model.graph.output) != 1:
        raise ValueError('this recipe requires the retained FP32 CTC opset-10 model')
    original = model.graph.output[0]
    shape = original.type.tensor_type.shape.dim
    if (original.type.tensor_type.elem_type != TensorProto.FLOAT or len(shape) != 3
            or shape[2].dim_value < 2):
        raise ValueError('expected float32 [batch,time,characters] probabilities')
    probability = original.name
    nodes = [
        helper.make_node('ArgMax', [probability], ['ctc_indices'], axis=2, keepdims=0, name='stockagent_ctc_argmax'),
        helper.make_node('ReduceMax', [probability], ['ctc_scores'], axes=[2], keepdims=0, name='stockagent_ctc_max'),
        helper.make_node('IsNaN', [probability], ['ctc_nan'], name='stockagent_ctc_nan'),
        helper.make_node('Cast', ['ctc_nan'], ['ctc_nan_float'], to=TensorProto.FLOAT, name='stockagent_ctc_nan_cast'),
        helper.make_node('ReduceMax', ['ctc_nan_float'], ['ctc_invalid'], keepdims=0, name='stockagent_ctc_nan_any'),
    ]
    names = {name for node in model.graph.node for name in node.output}
    if any(name in names for node in nodes for name in node.output):
        raise ValueError('compact CTC node name collision')
    model.graph.node.extend(nodes)
    del model.graph.output[:]
    model.graph.output.extend([
        helper.make_tensor_value_info('ctc_indices', TensorProto.INT64, ['batch', 'time']),
        helper.make_tensor_value_info('ctc_scores', TensorProto.FLOAT, ['batch', 'time']),
        helper.make_tensor_value_info('ctc_invalid', TensorProto.FLOAT, []),
    ])
    props = {p.key:p.value for p in model.metadata_props}
    props.update(stockagent_ctc_recipe=RECIPE, stockagent_ctc_source_sha256=expected_sha256,
                 stockagent_ctc_characters=str(shape[2].dim_value))
    helper.set_model_props(model, props)
    onnx.checker.check_model(model)
    payload = model.SerializeToString()
    if output.exists() and output.read_bytes() != payload:
        raise ValueError('refusing to overwrite a different derived model')
    atomic_write_bytes(output, payload)
    receipt = dict(recipe=RECIPE, source_path=str(source), source_sha256=expected_sha256,
                   path=str(output), sha256=sha256_file(output), onnx=onnx.__version__,
                   builder_sha256=sha256_file(Path(__file__)), characters=shape[2].dim_value)
    atomic_write_json(output.with_suffix('.receipt.json'), receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    entry = json.loads(args.config.read_text())['models']['Rec']
    print(json.dumps(compact_model(Path(entry['path']), entry['sha256'], args.output), indent=2))


if __name__ == '__main__':
    main()
