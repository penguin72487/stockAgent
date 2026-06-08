import torch

from stockagent.models.transformer_base_portfolio import local_causal_mask


def test_local_causal_mask_limits_visible_history() -> None:
    mask = local_causal_mask(seq_len=5, window=3, device=torch.device("cpu"))

    expected = torch.tensor(
        [
            [True, False, False, False, False],
            [True, True, False, False, False],
            [True, True, True, False, False],
            [False, True, True, True, False],
            [False, False, True, True, True],
        ]
    )
    assert torch.equal(mask, expected)


def test_local_causal_mask_zero_window_means_full_causal_history() -> None:
    mask = local_causal_mask(seq_len=4, window=0, device=torch.device("cpu"))
    assert torch.equal(mask, torch.tril(torch.ones(4, 4, dtype=torch.bool)))
