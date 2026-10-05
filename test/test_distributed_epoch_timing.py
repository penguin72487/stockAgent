import torch

from stockagent.training import trainer


def test_epoch_timing_single_process(monkeypatch):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: False)
    assert trainer._max_rank_epoch_timing(
        epoch_wall_s=3.0, train_total_s=2.0, device=torch.device("cpu")
    ) == {
        "epoch_wall_s_max_rank": 3.0,
        "train_total_s_max_rank": 2.0,
        "timing_rank_count": 1,
    }


def test_epoch_timing_reduces_maximum_not_mean(monkeypatch):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)

    def remote_max(payload, *, op):
        assert op == trainer.dist.ReduceOp.MAX
        payload.copy_(torch.maximum(payload, payload.new_tensor([4.0, 1.0])))

    monkeypatch.setattr(trainer.dist, "all_reduce", remote_max)
    assert trainer._max_rank_epoch_timing(
        epoch_wall_s=3.0, train_total_s=2.0, device=torch.device("cpu")
    ) == {
        "epoch_wall_s_max_rank": 4.0,
        "train_total_s_max_rank": 2.0,
        "timing_rank_count": 2,
    }
