import torch

from stockagent.training.time_block_dataset import TimeBlockSplit


def _make_split() -> TimeBlockSplit:
    features = torch.arange(10 * 3 * 2, dtype=torch.float32).reshape(10, 3, 2)
    returns = torch.randn(10, 3)
    mask = torch.ones(10, 3, dtype=torch.bool)
    return TimeBlockSplit(
        features=features,
        valid_indices=torch.arange(2, 10),
        future_log_returns=returns,
        tradable_mask=mask,
        can_buy_mask=mask.clone(),
        can_sell_mask=mask.clone(),
        benchmark=returns.mean(dim=1),
        lookback=3,
    )


def test_time_block_split_iter_blocks_uses_single_contiguous_context() -> None:
    split = _make_split()
    specs = split.iter_blocks(target_block_size=4)

    assert len(specs) == 2
    assert (specs[0].context_start, specs[0].context_end) == (0, 6)
    assert (specs[0].target_start, specs[0].target_end, specs[0].target_offset) == (2, 6, 2)
    assert (specs[1].context_start, specs[1].context_end) == (4, 10)
    assert (specs[1].target_start, specs[1].target_end, specs[1].target_offset) == (6, 10, 2)


def test_time_block_split_get_block_shapes_and_values() -> None:
    split = _make_split()
    spec = split.iter_blocks(target_block_size=4)[0]
    batch = split.get_block(spec, device=torch.device("cpu"), non_blocking=False)

    assert batch["x_context"].shape == (6, 3, 2)
    assert batch["future_log_returns"].shape == (4, 3)
    assert batch["tradable_mask"].shape == (4, 3)
    assert batch["target_offset"] == 2
    assert batch["target_len"] == 4
    assert torch.equal(batch["x_context"], split.features[0:6])
    assert torch.equal(batch["context_positions"], torch.arange(0, 6, dtype=torch.float32))
