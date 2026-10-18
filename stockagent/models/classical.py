"""Serializable score models using the canonical cash / allocation function."""
from __future__ import annotations

from pathlib import Path
import tempfile

import numpy as np
import torch
from torch import nn

from stockagent.models.normalization import masked_score_entmax_scale_separated_cash_weights


class CrossSectionalClassical(nn.Module):
    def __init__(self, *, lookback, num_features, num_symbols, long_only,
                 portfolio_activation, kind):
        super().__init__()
        if portfolio_activation not in {"identity","pre_normalized"}:
            raise ValueError("classical full-window v1 requires identity or pre_normalized allocation")
        if kind not in {"ridge", "lightgbm", "lightgbm_ranker", "xgboost", "catboost"}:
            raise ValueError(f"unsupported classical model: {kind}")
        self.lookback, self.num_features, self.num_symbols = lookback, num_features, num_symbols
        self.long_only, self.kind = bool(long_only), kind
        self.native = None
        self.coefficient = None
        self.intercept = 0.0
        self.target_scale = 1.0
        self.iteration = 0
        self.threads = 1

    def weights(self, scores, mask):
        scores = torch.as_tensor(scores, dtype=torch.float32, device=mask.device)
        short_mask = torch.zeros_like(mask) if self.long_only else mask
        return masked_score_entmax_scale_separated_cash_weights(
            scores, mask, short_mask=short_mask)

    def predict(self, x, *, iteration=None):
        n = self.iteration if iteration is None else int(iteration)
        if self.kind == "ridge":
            result = x @ self.coefficient + self.intercept
        elif self.kind.startswith("lightgbm"):
            result = self.native.predict(x, num_iteration=n or None, num_threads=self.threads)
        elif self.kind == "xgboost":
            import xgboost as xgb
            # A dense matrix retains numeric zeros. CSR defaults are missing in
            # XGBoost and therefore cannot implement this input ABI.
            if hasattr(x, "toarray"):
                x = x.toarray()
            result = self.native.inplace_predict(np.asarray(x, dtype=np.float32),
                iteration_range=(0, n))
        else:
            result = self.native.predict(x, ntree_end=n, thread_count=self.threads)
        return np.asarray(result, dtype=np.float32)

    def get_extra_state(self):
        state = dict(contract="canonical_classical_score_v1", kind=self.kind,
            lookback=self.lookback, num_features=self.num_features,
            num_symbols=self.num_symbols, long_only=self.long_only,
            intercept=float(self.intercept), target_scale=float(self.target_scale),
            iteration=int(self.iteration), threads=int(self.threads))
        if self.kind == "ridge":
            if self.coefficient is None:
                raise ValueError("cannot checkpoint an unfitted Ridge")
            state["coefficient"] = torch.from_numpy(np.asarray(self.coefficient).copy())
        elif self.kind.startswith("lightgbm"):
            state["native"] = self.native.model_to_string(num_iteration=self.iteration)
        elif self.kind == "xgboost":
            state["native"] = bytes(self.native[:self.iteration].save_raw(raw_format="ubj"))
        else:
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "model.cbm"
                model = self.native.copy()
                model.shrink(self.iteration)
                model.save_model(str(path))
                state["native"] = path.read_bytes()
        return state

    def set_extra_state(self, state):
        expected = dict(contract="canonical_classical_score_v1", kind=self.kind,
            lookback=self.lookback, num_features=self.num_features,
            num_symbols=self.num_symbols, long_only=self.long_only)
        if any(state.get(k) != v for k, v in expected.items()):
            raise ValueError("incompatible classical model / input / allocation ABI")
        self.intercept, self.target_scale = state["intercept"], state["target_scale"]
        self.iteration, self.threads = state["iteration"], state["threads"]
        if self.kind == "ridge":
            self.coefficient = state["coefficient"].cpu().numpy().copy()
        elif self.kind.startswith("lightgbm"):
            import lightgbm as lgb
            self.native = lgb.Booster(model_str=state["native"])
        elif self.kind == "xgboost":
            import xgboost as xgb
            self.native = xgb.Booster(model_file=bytearray(state["native"]))
        else:
            from catboost import CatBoostRegressor
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "model.cbm"
                path.write_bytes(state["native"])
                self.native = CatBoostRegressor().load_model(str(path))

    def forward(self, x, tradable_mask=None):
        b, l, s, f = x.shape
        if (l, s, f) != (self.lookback, self.num_symbols, self.num_features):
            raise ValueError("classical input shape mismatch")
        matrix = x.permute(0, 2, 1, 3).reshape(b * s, l * f).float().cpu().numpy()
        scores = self.predict(matrix).reshape(b, s)
        mask = (torch.ones((b, s), dtype=torch.bool, device=x.device)
                if tradable_mask is None else tradable_mask)
        return self.weights(scores, mask)
