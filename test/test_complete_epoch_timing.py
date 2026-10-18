import json

import pytest
import torch

from scripts.benchmark_tw_day_trade_batch_sizes import _read_epoch_curve, _score_curve
from stockagent.training import trainer


def test_complete_epoch_record_includes_curve_and_lifecycle_elapsed_time(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer.time, 'perf_counter', lambda: 17.)
    curve = tmp_path/'epoch_curve.jsonl'
    # The earlier scalar was captured before 2 seconds of output work.
    curve.write_text(json.dumps(dict(epoch=3, epoch_wall_s_max_rank=5.))+'\n')
    trainer._record_complete_epoch_timing(curve, epoch=3, epoch_started_at=10.,
        train_total_s=4., device=torch.device('cpu'))
    rows = _read_epoch_curve(curve)
    assert rows[0]['epoch_wall_s_max_rank'] == 5.
    assert rows[0]['complete_epoch_wall_s_max_rank'] == 7.
    assert rows[0]['complete_train_total_s_max_rank'] == 4.
    assert rows[0]['timing_rank_count'] == 1


def test_complete_epoch_uses_slowest_rank_and_rejects_duplicate_receipts(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer, '_max_rank_epoch_timing', lambda **_: dict(
        epoch_wall_s_max_rank=9., train_total_s_max_rank=7., timing_rank_count=2))
    curve = tmp_path/'epoch_curve.jsonl'
    curve.write_text(json.dumps(dict(epoch=4))+'\n')
    trainer._record_complete_epoch_timing(curve, epoch=4, epoch_started_at=0.,
        train_total_s=3., device=torch.device('cpu'))
    assert _read_epoch_curve(curve)[0]['complete_epoch_wall_s_max_rank'] == 9.
    trainer._record_complete_epoch_timing(curve, epoch=4, epoch_started_at=0.,
        train_total_s=3., device=torch.device('cpu'))
    with pytest.raises(ValueError, match='duplicate'):
        _read_epoch_curve(curve)


def test_throughput_prefers_complete_output_boundary_and_rejects_missing_epoch():
    row = dict(epoch=3, epoch_wall_s_max_rank=5., train_total_s_max_rank=3.,
        complete_epoch_wall_s_max_rank=7., complete_train_total_s_max_rank=3.,
        train_loss=-.1, val_mean=-.2, test_mean=-.3, train_batches=3,
        train_grad_norm_before_clip_mean=1., dynamo_unique_graphs_epoch_delta=0)
    kwargs = dict(skip_epochs=2, minimum_steady_epochs=1, train_rows=90,
        global_batch_size=32, memory=dict(ok=True, per_gpu={}, max_peak_fraction=.5,
        min_headroom_gib=10.), max_peak_fraction=.9, min_headroom_gib=1., world_size=2)
    score = _score_curve([row], **kwargs)
    assert score['ok'] and score['median_epoch_wall_s'] == 7.
    assert score['complete_epoch_timing_observed']
    missing = dict(row, epoch=4)
    missing.pop('complete_epoch_wall_s_max_rank')
    assert not _score_curve([row, missing], **kwargs)['ok']
