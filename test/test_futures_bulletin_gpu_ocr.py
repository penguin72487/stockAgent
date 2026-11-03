"""Exercise the candidate workflow with a fake CUDA runner, without a GPU."""
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from scripts import search_futures_bulletin_holiday_ocr as search


def test_bulletin_profile_pins_v5_normalization_without_resource_caps():
    config = json.loads(Path(search.CUDA_CONFIG).read_text())
    assert config['params']['Det.mean'] == [0.485, 0.456, 0.406]
    assert config['params']['Det.std'] == [0.229, 0.224, 0.225]
    assert config['params']['Det.limit_type'] == 'min'
    assert config['optimizations']['avoid_det_upscale'] is True
    execution = config['execution']
    assert all(execution[key] is None for key in
               ('cpu_threads', 'cpu_cores', 'nice', 'gpu_memory_mb',
                'model_memory_mb', 'min_free_gpu_mb'))
    assert execution['exclusive_gpu_owner'] is False and execution['shared_thread_pool'] is True


@pytest.mark.parametrize('default_engine', [False, True])
def test_gpu_search_preserves_source_image_text_blocks_and_cuda_receipt(monkeypatch, tmp_path, default_engine):
    import stockagent.ocr.rapidocr as rapidocr

    pdf = b'%PDF-test-source'
    monkeypatch.setattr(search, 'SOURCES', (('source.pdf', sha256(pdf).hexdigest()),))
    rgb = bytes([1, 2, 3, 4, 5, 6])
    pixmap = SimpleNamespace(n=3, height=1, width=2, stride=6,
                            samples_mv=memoryview(rgb), tobytes=lambda fmt: b'png-evidence')
    page = SimpleNamespace(get_pixmap=lambda **kwargs: pixmap)
    class Document:
        def __len__(self): return 1
        def __iter__(self): return iter([page])
        def close(self): pass
    fake_fitz = SimpleNamespace(open=lambda **kwargs: Document())
    response = SimpleNamespace(content=pdf, raise_for_status=lambda: None)
    fake_requests = SimpleNamespace(get=lambda *args, **kwargs: response)
    monkeypatch.setitem(__import__('sys').modules, 'fitz', fake_fitz)
    monkeypatch.setitem(__import__('sys').modules, 'requests', fake_requests)

    class Runner:
        budget = SimpleNamespace(device='cuda')
        last_call = {'wall_s': 0.1, 'device': 'cuda'}
        closed = False
        def __call__(self, pixels):
            assert pixels.tolist() == [[[3, 2, 1], [6, 5, 4]]]
            return SimpleNamespace(txts=('春節休市候選', '其他'),
                                   to_json=lambda: [{'text': '春節休市候選'}])
        def finish_profiling(self):
            return {'providers': {'Det': ['CUDAExecutionProvider']}, 'profiles': {'Det': 'fake'}}
        def close(self): self.closed = True
    runner = Runner()
    monkeypatch.setattr(rapidocr, 'rapidocr_engine', lambda config, profile_dir: runner)
    if default_engine:
        root = tmp_path / 'automatic-runs'
        monkeypatch.setattr(search, 'DEFAULT_OUTPUT_ROOT', root)
        search.main([])
        (out,) = list(root.iterdir())
    else:
        out = tmp_path / 'new-run'
        search.main(['--engine', 'rapidocr-cuda', '--output-dir', str(out)])
    receipt = json.loads((out / 'receipt.json').read_text())
    row = receipt['pages'][0]
    assert receipt['candidate_only'] and not receipt['point_in_time_verified'] and not receipt['training_ready']
    assert row['source_sha256'] == sha256(pdf).hexdigest()
    assert row['image_sha256'] == sha256(b'png-evidence').hexdigest()
    assert (out / row['text']).read_text() == '春節休市候選\n其他'
    assert row['matching_lines'] == ['春節休市候選']
    assert json.loads((out / row['blocks']).read_text()) == [{'text': '春節休市候選'}]
    assert receipt['gpu_evidence']['providers']['Det'][0] == 'CUDAExecutionProvider'
    assert runner.closed


def test_gpu_search_rejects_existing_evidence_directory(tmp_path):
    with pytest.raises(FileExistsError):
        search.main(['--engine', 'rapidocr-cuda', '--output-dir', str(tmp_path)])


def test_cuda_search_rejects_cpu_config_before_initializing_or_creating_output(tmp_path):
    config = tmp_path / 'cpu.json'
    config.write_text(json.dumps({'execution': {'device': 'cpu'}}))
    output = tmp_path / 'new-run'
    with pytest.raises(ValueError, match='requires a CUDA execution profile'):
        search.main(['--engine', 'rapidocr-cuda', '--rapidocr-config', str(config),
                     '--output-dir', str(output)])
    assert not output.exists()


def test_rejects_changed_source_before_ocr(monkeypatch, tmp_path):
    monkeypatch.setattr(search, 'SOURCES', (('source.pdf', '0' * 64),))
    response = SimpleNamespace(content=b'%PDF-changed', raise_for_status=lambda: None)
    with pytest.raises(ValueError, match='source content changed'):
        search._search_sources(None, None, tmp_path, [], None,
            SimpleNamespace(get=lambda *a, **k: response), None, None)
