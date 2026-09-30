"""Build an explicitly experimental BF16 Conv/MatMul OCR candidate and FP32 control.

Upgrade ONNX schemas to opset 22 for BF16 Conv. Keep normalization, activations,
pooling, softmax, CTC reductions and public I/O FP32. Original weights stay
untouched; constant weight casts can be folded by ORT. This is mixed precision,
not a claim that every OCR operator supports BF16.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file


def build_candidate(config_path, output_dir):
    import onnx
    from onnx import TensorProto, helper
    source_config=json.loads(config_path.read_text())
    configs={precision:copy.deepcopy(source_config) for precision in ['fp32_opset22','bf16']}
    # ORT's cuDNN frontend also filters BF16 Tensor Core plans when use_tf32
    # is false. This all-component experiment converts Conv/MatMul to BF16;
    # explicitly permit Tensor Core plans and retain the strict FP32 control.
    configs['bf16'].setdefault('optimizations', {})['use_tf32'] = True
    receipts=[]
    for name,entry in source_config['models'].items():
        if name=='Rec' and source_config.get('optimizations',{}).get('compact_ctc'):
            entry=source_config['compact_rec_model'];key='compact_rec_model'
        elif name=='Det' and source_config.get('optimizations',{}).get('gpu_det_normalize'):
            entry=source_config['uint8_det_model'];key='uint8_det_model'
        else:
            key=None
        source=Path(entry['path'])
        if sha256_file(source)!=entry['sha256']:
            raise ValueError('precision source hash mismatch')
        original=onnx.load(str(source))
        upgraded=onnx.version_converter.convert_version(original,22)
        upgraded.ir_version=max(upgraded.ir_version,10)
        inferred=onnx.shape_inference.infer_shapes(upgraded)
        types={v.name:v.type.tensor_type.elem_type for v in
               list(inferred.graph.input)+list(inferred.graph.output)+list(inferred.graph.value_info)}
        types.update({v.name:v.data_type for v in inferred.graph.initializer})
        for precision,config in configs.items():
            model=copy.deepcopy(upgraded)
            nodes=[];converted=[]
            for index,node in enumerate(model.graph.node):
                inputs=[v for v in node.input if v]
                if (precision=='bf16' and node.op_type in {'Conv','MatMul'} and node.domain==''
                        and all(types.get(v)==TensorProto.FLOAT for v in inputs)):
                    new=copy.deepcopy(node)
                    for i,input_name in enumerate(node.input):
                        if not input_name:continue
                        cast_name=f'stockagent_bf16_{index}_in_{i}'
                        nodes.append(helper.make_node('Cast',[input_name],[cast_name],to=TensorProto.BFLOAT16,
                                                      name=cast_name))
                        new.input[i]=cast_name
                    for i,output_name in enumerate(node.output):
                        new.output[i]=f'stockagent_bf16_{index}_out_{i}'
                    nodes.append(new)
                    for i,output_name in enumerate(node.output):
                        nodes.append(helper.make_node('Cast',[new.output[i]],[output_name],to=TensorProto.FLOAT,
                                                      name=f'stockagent_bf16_{index}_restore_{i}'))
                    converted.append(dict(name=node.name,op=node.op_type))
                else:
                    nodes.append(node)
            del model.graph.node[:];model.graph.node.extend(nodes)
            if precision=='bf16' and not converted:
                raise ValueError(f'{name}: no FP32 Conv/MatMul eligible for BF16')
            props={p.key:p.value for p in model.metadata_props}
            props.update(stockagent_compute_precision='bf16_conv_matmul_fp32_other' if converted else 'fp32',
                         stockagent_precision_source_sha256=entry['sha256'])
            helper.set_model_props(model,props)
            onnx.checker.check_model(model)
            output=output_dir/precision/(name+'.onnx')
            payload=model.SerializeToString()
            if output.exists() and output.read_bytes()!=payload:
                raise ValueError('refusing to overwrite a different precision model')
            atomic_write_bytes(output,payload)
            receipt=dict(precision=precision,source_path=str(source),source_sha256=entry['sha256'],
                path=str(output),sha256=sha256_file(output),opset=22,converted_nodes=converted,
                onnx=onnx.__version__,builder_sha256=sha256_file(Path(__file__)))
            atomic_write_json(output.with_suffix('.receipt.json'),receipt);receipts.append(receipt)
            target=config[key] if key else config['models'][name]
            target.update(path=str(output),sha256=receipt['sha256'])
    for precision,config in configs.items():
        atomic_write_json(output_dir/(precision+'.json'),config)
    return receipts


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--output-dir',type=Path,required=True)
    args=p.parse_args()
    for r in build_candidate(args.config,args.output_dir):
        print(json.dumps(dict(precision=r['precision'],path=r['path'],converted=len(r['converted_nodes']))))


if __name__=='__main__':
    main()
