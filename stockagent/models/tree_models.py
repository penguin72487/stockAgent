from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, normalize_portfolio_activation


@dataclass(slots=True)
class _TreeData:
    x: np.ndarray
    y: np.ndarray


class _CrossSectionalTreeBase(nn.Module):
    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        long_only: bool,
        portfolio_activation: str = "identity",
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.input_dim = self.lookback * self.num_features
        self._fitted = False

    def _prepare_train_data(
        self,
        x: torch.Tensor,
        future_log_returns: torch.Tensor,
        tradable_mask: torch.Tensor,
    ) -> _TreeData:
        # x: [N, lookback, S, F] -> [N*S, lookback*F]
        n_rows, lookback, num_symbols, num_features = x.shape
        if lookback != self.lookback or num_symbols != self.num_symbols or num_features != self.num_features:
            raise ValueError(
                "Training data shape mismatch: "
                f"expected [N, {self.lookback}, {self.num_symbols}, {self.num_features}], "
                f"got [N, {lookback}, {num_symbols}, {num_features}]"
            )

        x_np = (
            x.permute(0, 2, 1, 3)
            .reshape(n_rows * num_symbols, self.input_dim)
            .detach()
            .cpu()
            .numpy()
            .astype(np.float32, copy=False)
        )
        y_np = future_log_returns.reshape(n_rows * num_symbols).detach().cpu().numpy().astype(np.float32, copy=False)
        m_np = tradable_mask.reshape(n_rows * num_symbols).detach().cpu().numpy().astype(bool, copy=False)

        if m_np.any():
            x_np = x_np[m_np]
            y_np = y_np[m_np]

        if x_np.shape[0] == 0:
            raise ValueError("No tradable samples available for tree model training.")
        return _TreeData(x=x_np, y=y_np)

    def fit(
        self,
        x: torch.Tensor,
        future_log_returns: torch.Tensor,
        tradable_mask: torch.Tensor,
    ) -> None:
        data = self._prepare_train_data(x, future_log_returns, tradable_mask)
        self._fit_numpy(data.x, data.y)
        self._fitted = True

    def _fit_numpy(self, x: np.ndarray, y: np.ndarray) -> None:
        raise NotImplementedError

    def _predict_numpy(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        if not self._fitted:
            raise RuntimeError("Tree model is not fitted yet. Call fit() before inference.")

        bsz, lookback, num_symbols, num_features = x.shape
        if lookback != self.lookback or num_symbols != self.num_symbols or num_features != self.num_features:
            raise ValueError(
                "Inference data shape mismatch: "
                f"expected [B, {self.lookback}, {self.num_symbols}, {self.num_features}], "
                f"got [B, {lookback}, {num_symbols}, {num_features}]"
            )

        x_np = (
            x.permute(0, 2, 1, 3)
            .reshape(bsz * num_symbols, self.input_dim)
            .detach()
            .cpu()
            .numpy()
            .astype(np.float32, copy=False)
        )
        logits_np = self._predict_numpy(x_np).reshape(bsz, num_symbols)
        logits = torch.from_numpy(logits_np).to(device=x.device, dtype=x.dtype)

        if self.long_only:
            return masked_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        return dual_branch_softmax(logits, tradable_mask, activation=self.portfolio_activation)


class CrossSectionalLightGBM(_CrossSectionalTreeBase):
    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        long_only: bool,
        portfolio_activation: str = "identity",
        *,
        use_gpu: bool,
        gpu_device_id: int,
        n_estimators: int,
        num_leaves: int,
        max_depth: int,
        learning_rate: float,
        subsample: float,
        colsample_bytree: float,
        reg_lambda: float,
        n_jobs: int,
        random_state: int,
    ) -> None:
        super().__init__(lookback, num_features, num_symbols, long_only, portfolio_activation)
        self._feature_names = [f"f{i}" for i in range(self.input_dim)]
        self._use_gpu = bool(use_gpu)
        self._gpu_device_id = int(gpu_device_id)
        try:
            from lightgbm import LGBMRegressor
        except ImportError as exc:
            raise ImportError("LightGBM is not installed. Please install with `pip install lightgbm`.") from exc

        self._model: Any = LGBMRegressor(
            objective="regression",
            n_estimators=int(n_estimators),
            num_leaves=int(num_leaves),
            max_depth=int(max_depth),
            learning_rate=float(learning_rate),
            subsample=float(subsample),
            colsample_bytree=float(colsample_bytree),
            reg_lambda=float(reg_lambda),
            n_jobs=int(n_jobs),
            random_state=int(random_state),
            # conda-forge cuda build uses device_type='cuda' (OpenCL 'gpu' is a different backend).
            device_type="cuda" if self._use_gpu else "cpu",
            gpu_device_id=self._gpu_device_id,
            verbosity=-1,
        )

    def _fit_numpy(self, x: np.ndarray, y: np.ndarray) -> None:
        try:
            self._model.fit(x, y, feature_name=self._feature_names)
        except Exception as exc:
            if self._use_gpu:
                raise RuntimeError(
                    "LightGBM GPU training failed. Ensure LightGBM is built with GPU/OpenCL support "
                    "and that GPU device is available."
                ) from exc
            raise

    def _predict_numpy(self, x: np.ndarray) -> np.ndarray:
        pred = self._model.predict(x)
        return np.asarray(pred, dtype=np.float32)


class CrossSectionalXGBoost(_CrossSectionalTreeBase):
    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        long_only: bool,
        portfolio_activation: str = "identity",
        *,
        use_gpu: bool,
        gpu_device_id: int,
        n_estimators: int,
        max_depth: int,
        learning_rate: float,
        subsample: float,
        colsample_bytree: float,
        reg_lambda: float,
        n_jobs: int,
        random_state: int,
    ) -> None:
        super().__init__(lookback, num_features, num_symbols, long_only, portfolio_activation)
        self._feature_names = [f"f{i}" for i in range(self.input_dim)]
        self._use_gpu = bool(use_gpu)
        self._gpu_device_id = int(gpu_device_id)
        try:
            from xgboost import XGBRegressor
        except ImportError as exc:
            raise ImportError("XGBoost is not installed. Please install with `pip install xgboost`.") from exc

        self._model: Any = XGBRegressor(
            objective="reg:squarederror",
            n_estimators=int(n_estimators),
            max_depth=int(max_depth),
            learning_rate=float(learning_rate),
            subsample=float(subsample),
            colsample_bytree=float(colsample_bytree),
            reg_lambda=float(reg_lambda),
            n_jobs=int(n_jobs),
            random_state=int(random_state),
            tree_method="hist",
            device=(f"cuda:{self._gpu_device_id}" if self._use_gpu else "cpu"),
            verbosity=0,
        )

    def _fit_numpy(self, x: np.ndarray, y: np.ndarray) -> None:
        self._model.fit(x, y)

    def _predict_numpy(self, x: np.ndarray) -> np.ndarray:
        pred = self._model.predict(x)
        return np.asarray(pred, dtype=np.float32)
