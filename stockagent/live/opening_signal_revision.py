"""Small revision ABI shared by the canonical inference and paper owners.

Only input facts, not receipt churn or last/bid/ask ticks, trigger a new opening
decision. Each published revision is immutable; the executor trades its target
minus actual inventory. No broker session, quote collector or ledger lives here.
"""
from __future__ import annotations

import hashlib
import string

import numpy as np

from stockagent.live.market_config import SIGNAL_UPDATE_ONCE, SIGNAL_UPDATE_PROGRESSIVE


def _valid_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in string.hexdigits for c in value)


def opening_input_fingerprint(request_key, snapshot, *, session_date):
    from stockagent.live.quote_provider import tw_session_open_observed_mask

    observed = tw_session_open_observed_mask(snapshot, session_date=session_date)
    opening = (np.asarray(snapshot.open_prices, dtype=np.float64) if snapshot.open_prices is not None
               else np.full(len(snapshot.prices), np.nan))
    if observed.shape != opening.shape or opening.ndim != 1:
        raise ValueError("opening revision requires one aligned complete model axis")
    # Canonicalize only unavailable values in the KEY; never mutate source data
    # or reinterpret this marker as an observed opening price/volume/fill.
    values = np.where(observed, opening, 0.).astype("<f8", copy=False)
    digest = hashlib.sha256(str(request_key).encode())
    digest.update(str(session_date).encode())
    digest.update(observed.tobytes())
    digest.update(values.tobytes())
    return digest.hexdigest()


def revision_fields(*, policy, signal_id, input_fingerprint, previous=None):
    if policy == SIGNAL_UPDATE_ONCE:
        if previous is not None:
            raise ValueError("a frozen opening signal cannot be revised")
        return {}
    if policy != SIGNAL_UPDATE_PROGRESSIVE or not signal_id or not _valid_digest(input_fingerprint):
        raise ValueError("invalid progressive signal identity")
    index, root, parent = 0, signal_id, None
    if previous is not None:
        prior = previous.get("opening_signal_revision") or {}
        if (previous.get("day_trade_signal_update_policy") != policy
                or type(prior.get("index")) is not int or prior["index"] < 0
                or not prior.get("root_signal_id") or not previous.get("signal_id")
                or previous["signal_id"] == signal_id):
            raise ValueError("incompatible progressive parent signal")
        if prior.get("input_fingerprint") == input_fingerprint:
            raise ValueError("unchanged opening inputs must reuse the immutable signal")
        index, root, parent = prior["index"] + 1, prior["root_signal_id"], previous["signal_id"]
    return {"day_trade_signal_update_policy": policy,
            "day_trade_signal_update_policy_fingerprint": hashlib.sha256(policy.encode()).hexdigest(),
            "opening_signal_revision": {"index": index, "root_signal_id": root,
                "parent_signal_id": parent, "input_fingerprint": input_fingerprint,
                "trigger": "new_observed_session_open_not_latest_tick",
                "execution": "actual_inventory_delta_fifo_reductions_before_additions"}}


def revision_admission(*, policy, summary, mode, session_date):
    """Allow skipped superseded revisions, reject old/cohort/model replays.

    Return (is_same_session_revision, error). Default once-only signals retain
    their existing gate. No received quote or source failure is a fill proof.
    """
    declared = summary.get("day_trade_signal_update_policy", SIGNAL_UPDATE_ONCE)
    if declared != policy:
        return False, "signal_update_policy_mismatch"
    if policy == SIGNAL_UPDATE_ONCE:
        return False, None
    revision = summary.get("opening_signal_revision") or {}
    index = revision.get("index")
    if (summary.get("day_trade_signal_update_policy_fingerprint") != hashlib.sha256(policy.encode()).hexdigest()
            or type(index) is not int or index < 0
            or not _valid_digest(revision.get("input_fingerprint"))
            or not isinstance(revision.get("root_signal_id"), str)
            or not revision["root_signal_id"]
            or (index == 0 and (revision["root_signal_id"] != summary.get("signal_id")
                               or revision.get("parent_signal_id") is not None))
            or (index > 0 and (not isinstance(revision.get("parent_signal_id"), str)
                               or not revision["parent_signal_id"]
                               or revision["parent_signal_id"] == summary.get("signal_id")))):
        return False, "invalid_signal_revision"
    same = mode.get("session_date") == session_date and bool(mode.get("entry_completed_at"))
    if not same:
        # A restarted consumer may first see revision N, not necessarily zero.
        # It still starts an independently recorded new cohort, never old state.
        if not revision.get("root_signal_id"):
            return False, "invalid_signal_revision_root"
        return False, None
    old = mode.get("opening_signal_revision") or {}
    if (mode.get("day_trade_signal_update_policy") != policy
            or not revision.get("root_signal_id")
            or revision["root_signal_id"] != old.get("root_signal_id")
            or type(old.get("index")) is not int or index <= old["index"]
            or revision["input_fingerprint"] == old.get("input_fingerprint")):
        return False, "stale_or_foreign_signal_revision"
    if any(summary.get(key) != mode.get(key) for key in ("checkpoint_fingerprint", "config_fingerprint")):
        return False, "signal_revision_model_changed"
    return True, None
