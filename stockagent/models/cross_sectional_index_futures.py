"""Cross-sectional stock encoder with one Taiwan-index-futures exposure head."""

from __future__ import annotations

from typing import Any

import torch
from torch import nn

from stockagent.models.normalization import finite_mask_fill_value
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel


class CrossSectionalIndexFuturesModel(TransformerBasePortfolioModel):
    """Read the stock cross-section and emit one signed market exposure.

    TX, MTX, and TMF share the same underlying index.  Product selection and
    integer sizing therefore belong to execution, not to three independent
    model heads.  This head attention-pools the final stock embeddings and
    returns one bounded scalar per decision row.
    """

    def __init__(
        self,
        *args: Any,
        max_abs_exposure: float = 1.0,
        futures_head_hidden_dim: int | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs["execution_mode"] = "naive"
        kwargs["return_aux"] = bool(kwargs.get("return_aux", False))
        super().__init__(*args, **kwargs)
        if not 0.0 < float(max_abs_exposure) <= 1.0:
            raise ValueError("max_abs_exposure must be in (0, 1]")
        self.max_abs_exposure = float(max_abs_exposure)
        hidden = (
            self.d_model
            if futures_head_hidden_dim is None
            else int(futures_head_hidden_dim)
        )
        if hidden < 1:
            raise ValueError("futures_head_hidden_dim must be positive")
        # The inherited score head maps every stock to a traded stock.  It is
        # intentionally removed so no dead stock-trading parameters remain.
        del self.score_head
        self.futures_pool_score = nn.Linear(self.d_model, 1)
        self.futures_head = nn.Sequential(
            nn.Linear(self.d_model, hidden),
            nn.SiLU(),
            nn.Linear(hidden, 1),
        )

    def _portfolio_outputs_from_stock_embeddings(
        self,
        z_stock: torch.Tensor,
        mask_bool: torch.Tensor,
        aux: dict[str, torch.Tensor],
        *,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        return_scores: bool = False,
    ):
        if z_stock.ndim != 3:
            raise ValueError(
                f"z_stock must have shape [B,S,D], got {tuple(z_stock.shape)}"
            )
        if tuple(mask_bool.shape) != tuple(z_stock.shape[:2]):
            raise ValueError("mask_bool must have shape [B,S]")
        mask = mask_bool.to(device=z_stock.device, dtype=torch.bool)
        masked_embeddings = z_stock.masked_fill(~mask.unsqueeze(-1), 0.0)
        pool_logits = self.futures_pool_score(masked_embeddings).squeeze(-1)
        pool_logits = torch.nan_to_num(
            pool_logits,
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        if temperature is None:
            temp = pool_logits.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=pool_logits.device, dtype=pool_logits.dtype)
        else:
            temp = pool_logits.new_tensor(float(temperature))
        temp = temp.clamp_min(0.05)
        masked_logits = (pool_logits / temp).masked_fill(
            ~mask,
            finite_mask_fill_value(pool_logits),
        )
        pool_weights = torch.softmax(masked_logits, dim=-1)
        has_stocks = mask.any(dim=-1)
        pool_weights = torch.where(
            has_stocks.unsqueeze(-1),
            pool_weights * mask.to(dtype=pool_weights.dtype),
            torch.zeros_like(pool_weights),
        )
        pool_weights = pool_weights / pool_weights.sum(
            dim=-1,
            keepdim=True,
        ).clamp_min(torch.finfo(pool_weights.dtype).eps)
        market_embedding = torch.sum(
            pool_weights.unsqueeze(-1) * masked_embeddings,
            dim=1,
        )
        exposure_logit = self.futures_head(market_embedding).squeeze(-1)
        exposure_logit = torch.nan_to_num(
            exposure_logit,
            nan=0.0,
            posinf=20.0,
            neginf=-20.0,
        )
        exposure = (
            torch.tanh(exposure_logit) * float(self.max_abs_exposure)
        )
        exposure = torch.where(has_stocks, exposure, torch.zeros_like(exposure))

        if return_scores:
            return exposure, exposure_logit
        include_aux = bool(return_aux is True or (return_aux is None and self.return_aux))
        if include_aux:
            output_aux = dict(aux)
            output_aux.update(
                {
                    "z_stock": masked_embeddings,
                    "futures_pool_weights": pool_weights,
                    "market_embedding": market_embedding,
                    "futures_exposure_logit": exposure_logit,
                    "futures_exposure": exposure,
                }
            )
            if return_aux is True:
                return exposure, exposure_logit, output_aux
            return {
                "weights": exposure,
                "scores": exposure_logit,
                "futures_exposure": exposure,
                "futures_exposure_logit": exposure_logit,
                "aux": output_aux,
                **output_aux,
            }
        return exposure


__all__ = ["CrossSectionalIndexFuturesModel"]
