"""Bitwise pixel equivalence and CTC ties/blank/non-finite correctness."""
from pathlib import Path

import numpy as np
import pytest

from stockagent.ocr.fastpath import CompactCTC, ExactDetPreprocess, Optimizations


@pytest.mark.parametrize('mean,std', [([.5]*3,[.5]*3), ([.485,.456,.406],[.229,.224,.225])])
def test_lut_preserves_every_channel_value_and_layout(mean, std):
    pytest.importorskip('cv2')
    class Native:
        def normalize(self, img):
            return (img.astype('float32') * (1/255.0)-np.array(mean))/np.array(std)

        def resize(self, img):
            return img

        def __call__(self, img):
            return self.normalize(img).transpose(2,0,1)[None].astype(np.float32)

    native = Native()
    values = np.random.default_rng(123).integers(0, 256, (191,257,3), dtype=np.uint8)
    values[0,:256] = np.arange(256, dtype=np.uint8)[:,None]
    result = ExactDetPreprocess(native)(values)
    assert np.array_equal(result, native(values))
    assert result.dtype == np.float32 and result.flags.c_contiguous
    # Caller-supplied float pixels must not be quantized to a 256-entry LUT.
    floats = values.astype(np.float32)+.125
    assert np.array_equal(ExactDetPreprocess(native)(floats), native(floats))


def test_compact_outputs_keep_first_tie_blank_and_repeated_positions():
    probabilities = np.array([[[.8,.1,.1],[.1,.7,.7],[.1,.9,0],[1.,0,0],[.1,.7,.7]]], dtype=np.float32)
    compact = CompactCTC(probabilities.argmax(axis=2), probabilities.max(axis=2), np.array(0,dtype=np.float32))
    assert compact.argmax(2).tolist() == [[0,1,1,0,1]]
    np.testing.assert_array_equal(compact.max(2), probabilities.max(axis=2))
    with pytest.raises(ValueError, match='character-axis'):
        compact.argmax(1)


@pytest.mark.parametrize('scores,invalid', [([np.nan],0), ([np.inf],0), ([.8],1)])
def test_compact_nonfinite_is_never_accepted(scores, invalid):
    with pytest.raises(ValueError, match='invalid compact'):
        CompactCTC(np.array([[0]],dtype=np.int64), np.array([scores],dtype=np.float32), np.array(invalid))


def test_compact_graph_matches_numpy_and_retains_dictionary(tmp_path):
    onnx = pytest.importorskip('onnx')
    ort = pytest.importorskip('onnxruntime')
    from scripts.build_ocr_compact_ctc import compact_model
    from downloader.artifact_io import sha256_file
    h, t = onnx.helper, onnx.TensorProto
    source = tmp_path/'source.onnx'
    graph = h.make_graph([h.make_node('Identity',['x'],['prob'])], 'test',
        [h.make_tensor_value_info('x', t.FLOAT, [None,None,5])],
        [h.make_tensor_value_info('prob', t.FLOAT, [None,None,5])])
    model = h.make_model(graph, opset_imports=[h.make_opsetid('',10)],ir_version=10)
    h.set_model_props(model, {'character':'甲\n乙\n丙'})
    onnx.save(model, source)
    original = source.read_bytes()
    output = tmp_path/'compact.onnx'
    receipt = compact_model(source, sha256_file(source), output)
    assert source.read_bytes() == original
    session = ort.InferenceSession(str(output), providers=['CPUExecutionProvider'])
    assert session.get_modelmeta().custom_metadata_map['character'] == '甲\n乙\n丙'
    assert receipt['source_sha256'] == sha256_file(source)
    values = np.random.default_rng(4).random((2,9,5),dtype=np.float32)
    values[0,0,:] = .2
    values[1,3,1:3] = 1
    indices, scores, invalid = session.run(None, {'x':values})
    np.testing.assert_array_equal(indices, values.argmax(2))
    np.testing.assert_array_equal(scores, values.max(2))
    assert not invalid
    values[0,0,2] = np.nan
    assert session.run(None, {'x':values})[2]
    with pytest.raises(ValueError, match='hash mismatch'):
        compact_model(source, '0'*64, tmp_path/'other.onnx')


@pytest.mark.parametrize('config', [{'compact_ctc':1}, {'arena_shrink':'yes'}, {'conv_search':'random'}])
def test_invalid_optimization_is_rejected(config):
    with pytest.raises(ValueError):
        Optimizations(**config)


def test_gpu_detector_graph_is_bitwise_native_normalization(tmp_path):
    onnx = pytest.importorskip('onnx')
    ort = pytest.importorskip('onnxruntime')
    from scripts.build_ocr_detector_lut import detector_model
    from downloader.artifact_io import sha256_file
    h, t = onnx.helper, onnx.TensorProto
    source = tmp_path/'source.onnx'
    graph = h.make_graph([h.make_node('Identity',['x'],['normalized'])], 'test',
        [h.make_tensor_value_info('x', t.FLOAT, [None,3,None,None])],
        [h.make_tensor_value_info('normalized', t.FLOAT, [None,3,None,None])])
    onnx.save(h.make_model(graph,opset_imports=[h.make_opsetid('',14)],ir_version=10),source)
    output = tmp_path/'gpu.onnx'
    mean,std = [.485,.456,.406],[.229,.224,.225]
    detector_model(source,sha256_file(source),output,mean=mean,std=std)
    session = ort.InferenceSession(str(output),providers=['CPUExecutionProvider'])
    values = np.random.default_rng(6).integers(0,256,(1,29,256,3),dtype=np.uint8)
    values[0,0] = np.arange(256,dtype=np.uint8)[:,None]
    expected = ((values.astype('float32')*(1/255.0)-np.array(mean))/np.array(std)).astype(np.float32).transpose(0,3,1,2)
    actual = session.run(None, {'stockagent_pixels':values})[0]
    np.testing.assert_array_equal(actual,expected)


def test_gpu_detector_rejects_a_changed_normalization():
    pytest.importorskip('cv2')
    from stockagent.ocr.fastpath import GPUDetPreprocess
    class Native:
        def normalize(self, values):
            return values.astype(np.float32)/255
    with pytest.raises(ValueError,match='differs from native'):
        GPUDetPreprocess(Native(),'0'*64)


def test_engine_path_aliases_cannot_create_duplicate_device_owner(monkeypatch,tmp_path):
    import stockagent.ocr.rapidocr as module
    seen=[]
    monkeypatch.setattr(module,'_rapidocr_engine',lambda *args:seen.append(args))
    config=tmp_path/'config.json'
    module.rapidocr_engine(config)
    module.rapidocr_engine(str(config),None)
    assert seen[0]==seen[1]==(str(config),None)


def test_width_microbatch_keeps_original_padding_and_order():
    from types import SimpleNamespace
    from stockagent.ocr.fastpath import InferenceCall
    calls=[]
    def run(names,feed,opts):
        tensor=feed['x'];calls.append(tensor.copy())
        indices=np.repeat(tensor[:,0,0,0,None].astype(np.int64),3,axis=1)
        return [indices,np.full(indices.shape,.9,dtype=np.float32),np.array(0,dtype=np.float32)]
    session=SimpleNamespace(get_inputs=lambda:[SimpleNamespace(name='x')],
        get_outputs=lambda:[SimpleNamespace(name=x) for x in ['ctc_indices','ctc_scores','ctc_invalid']],run=run)
    tensor=np.zeros((6,3,48,1800),dtype=np.float32)
    tensor[:,0,0,0]=np.arange(6)
    events=[]
    result=InferenceCall(session,'Rec',events,compact=True,batch_width_budget=4096)(tensor)
    assert [a.shape for a in calls]==[(2,3,48,1800)]*3
    np.testing.assert_array_equal(np.concatenate(calls),tensor)
    np.testing.assert_array_equal(result.indices[:,0],np.arange(6))
    assert sum(e['input_bytes'] for e in events)==tensor.nbytes


def test_page_reclamation_runs_only_on_last_microbatch():
    from types import SimpleNamespace
    from stockagent.ocr.fastpath import ComponentBoundary, InferenceCall
    options=[]
    marker=object()
    def run(names,feed,option):
        options.append(option)
        n=feed['x'].shape[0]
        return [np.zeros((n,3),dtype=np.int64),np.ones((n,3),dtype=np.float32),np.array(0,dtype=np.float32)]
    session=SimpleNamespace(get_inputs=lambda:[SimpleNamespace(name='x')],
        get_outputs=lambda:[SimpleNamespace(name=x) for x in ['ctc_indices','ctc_scores','ctc_invalid']],run=run)
    adapter=InferenceCall(session,'Rec',[],compact=True,batch_width_budget=1600,
        run_options=marker,shrink_per_page=True)
    batch=np.zeros((2,3,48,1600),dtype=np.float32)
    with pytest.raises(RuntimeError,match='batch boundary'):
        adapter(batch)
    def original(images):
        adapter(batch);adapter(batch[:1])
    ComponentBoundary(original,adapter,2)([1,2,3])
    assert options==[None,None,marker]
    assert adapter.remaining_calls==0
