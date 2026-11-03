"""Bind real earlier feature rows without inventing futures account days."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from stockagent.data.panel_cache import array_content_fingerprint


CONTRACT = "futures_feature_only_history_v2"


@dataclass(frozen=True, slots=True)
class FuturesFeatureHistory:
    dates: np.ndarray
    features: Any
    alive_mask: np.ndarray
    prefix_rows: int
    symbols: tuple[str, ...]
    feature_names: tuple[str, ...]

    def verify(self, panel: Any) -> None:
        dates = np.asarray(self.dates, dtype="datetime64[D]")
        decision_dates = np.asarray(panel.dates, dtype="datetime64[D]")
        offset = self.prefix_rows
        if (isinstance(offset, bool) or int(offset) != offset or offset <= 0
                or dates.ndim != 1 or not len(decision_dates) or np.isnat(dates).any()
                or np.any(dates[1:] <= dates[:-1])
                or not np.array_equal(dates[offset:], decision_dates)):
            raise ValueError("feature history must precede and exactly contain the decision calendar")
        if (self.symbols != tuple(panel.symbols) or self.feature_names != tuple(panel.feature_names)
                or tuple(self.features.shape) != (len(dates), len(self.symbols), len(self.feature_names))
                or tuple(panel.features.shape) != (len(decision_dates), len(self.symbols), len(self.feature_names))):
            raise ValueError("feature history has incompatible symbol or feature axes")
        alive = np.asarray(self.alive_mask)
        if (alive.dtype != np.bool_ or alive.shape != (len(dates), len(self.symbols))
                or not np.array_equal(alive[offset:], np.asarray(panel.alive_mask))):
            raise ValueError("feature history alive mask is not the decision panel's exact tail")
        if getattr(self.features, "_stockagent_factorized_features", False):
            if not getattr(panel.features, "_stockagent_factorized_features", False):
                raise ValueError("feature history storage differs from its decision panel")
            tail = self.features.slice_dates(offset)
            if tail.content_fingerprint != panel.features.content_fingerprint:
                raise ValueError("factorized feature history is not the decision panel's exact tail")
        elif not np.array_equal(np.asarray(self.features)[offset:], np.asarray(panel.features), equal_nan=True):
            raise ValueError("feature history is not the decision panel's exact tail")

    def fingerprint(self) -> dict[str, Any]:
        features = (dict(self.features.content_fingerprint)
                    if getattr(self.features, "_stockagent_factorized_features", False)
                    else array_content_fingerprint(np.asarray(self.features)))
        return {"contract": CONTRACT, "prefix_rows": self.prefix_rows,
                "dates": array_content_fingerprint(np.asarray(self.dates, dtype="datetime64[D]")),
                "features": features,
                "alive_mask": array_content_fingerprint(np.asarray(self.alive_mask))}


def bind_futures_feature_history(source_panel: Any, decision_panel: Any, *, lookback: int, feature_lag: int) -> FuturesFeatureHistory:
    """Take a shared immutable view of the earlier and decision feature rows."""
    dates = np.asarray(source_panel.dates, dtype="datetime64[D]").copy()
    decision_dates = np.asarray(decision_panel.dates, dtype="datetime64[D]")
    if not len(decision_dates):
        raise ValueError("feature history requires at least one decision session")
    offset = int(np.searchsorted(dates, decision_dates[0], side="left"))
    if int(lookback) < 1 or int(feature_lag) < 0 or offset < int(lookback) - 1 + int(feature_lag):
        raise ValueError("real feature history is too short for the first causal decision")
    dates.flags.writeable = False
    result = FuturesFeatureHistory(dates, source_panel.features, source_panel.alive_mask, offset,
                                   tuple(source_panel.symbols), tuple(source_panel.feature_names))
    result.verify(decision_panel)
    return result
