from __future__ import annotations

from abc import ABC, abstractmethod

import torch
from torch import nn


class PortfolioModel(nn.Module, ABC):
    """Abstract interface for all portfolio allocation models."""

    @abstractmethod
    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Return normalized per-symbol weights with shape [B, S]."""
