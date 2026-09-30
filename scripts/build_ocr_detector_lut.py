"""Bind exact uint8 detector normalization into a retained ONNX graph."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file

RECIPE = 'uint8_nhwc_exact_normalization_lut_v1'


def detector_model(source, expected_sha256, output, *, mean, std):
    import numpy as np
    import onnx
    from onnx import TensorProto, helper, numpy_helper
    if sha256_file(source) != expected_sha256:
        raise ValueError('detector source model hash mismatch')
    model = onnx.load(str(source))
    if len(model.graph.input) != 1:
        raise ValueError('expected one detector input')
    original = model.graph.input[0]
    shape = original.type.tensor_type.shape.dim
    if (original.type.tensor_type.elem_type != TensorProto.FLOAT or len(shape) != 4
            or shape[1].dim_value != 3):
        raise ValueError('expected float32 NCHW detector')
    if len(mean) != 3 or len(std) != 3 or any(s <= 0 for s in std):
        raise ValueError('three finite normalization channels required')
    # Follow RapidOCR's float32 multiplication then float64 mean/std exactly.
    values = np.repeat(np.arange(256,dtype=np.uint8)[:,None,None],3,axis=2)
    table = ((values.astype('float32')*(1/255.0)-np.array(mean))/np.array(std)).astype(np.float32)
    table = table.reshape(256,3).T.copy().reshape(-1)
    if not np.isfinite(table).all():
        raise ValueError('nonfinite detector normalization')
    lut_sha256 = hashlib.sha256(table.tobytes()).hexdigest()
    nodes = [
        helper.make_node('Cast',['stockagent_pixels'],['stockagent_pixel_int'],to=TensorProto.INT32,name='stockagent_pixel_cast'),
        helper.make_node('Add',['stockagent_pixel_int','stockagent_channel_offsets'],['stockagent_lut_indices'],name='stockagent_channel_index'),
        helper.make_node('Gather',['stockagent_normalize_lut','stockagent_lut_indices'],['stockagent_normalized'],axis=0,name='stockagent_normalize_gather'),
        helper.make_node('Transpose',['stockagent_normalized'],[original.name],perm=[0,3,1,2],name='stockagent_normalize_nchw'),
    ]
    if any(name.startswith('stockagent_') for node in model.graph.node for name in node.output):
        raise ValueError('detector graph name collision')
    original_nodes = list(model.graph.node)
    del model.graph.node[:]
    model.graph.node.extend(nodes + original_nodes)
    model.graph.initializer.extend([
        numpy_helper.from_array(table,'stockagent_normalize_lut'),
        numpy_helper.from_array(np.array([0,256,512],dtype=np.int32),'stockagent_channel_offsets'),
    ])
    del model.graph.input[:]
    model.graph.input.extend([helper.make_tensor_value_info('stockagent_pixels',TensorProto.UINT8,['batch','height','width',3])])
    props={p.key:p.value for p in model.metadata_props}
    props.update(stockagent_det_recipe=RECIPE, stockagent_det_source_sha256=expected_sha256,
                 stockagent_det_lut_sha256=lut_sha256)
    helper.set_model_props(model,props)
    onnx.checker.check_model(model)
    payload=model.SerializeToString()
    if output.exists() and output.read_bytes()!=payload:
        raise ValueError('refusing to overwrite a different derived model')
    atomic_write_bytes(output,payload)
    receipt=dict(recipe=RECIPE,source_path=str(source),source_sha256=expected_sha256,
                 path=str(output),sha256=sha256_file(output),onnx=onnx.__version__,
                 mean=mean,std=std,lut_sha256=lut_sha256,builder_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output.with_suffix('.receipt.json'),receipt)
    return receipt


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    config=json.loads(args.config.read_text());entry=config['models']['Det']
    print(json.dumps(detector_model(Path(entry['path']),entry['sha256'],args.output,
        mean=config['params'].get('Det.mean',[.5]*3),std=config['params'].get('Det.std',[.5]*3)),indent=2))


if __name__=='__main__':
    main()
