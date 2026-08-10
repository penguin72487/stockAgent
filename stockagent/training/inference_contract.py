"""Stable deployment boundary for checkpoint and runtime semantics.

All implementations live in orchestration-free modules. Importing or calling
this boundary must never load :mod:`stockagent.training.trainer`.
"""

from stockagent.training.checkpoint_contract import (
    align_panel_to_checkpoint_universe,
    build_checkpoint_manifest,
    checkpoint_manifest_symbols,
    validate_checkpoint_manifest,
)
from stockagent.training.runtime_configuration import configure_inference_runtime


__all__ = [
    "align_panel_to_checkpoint_universe",
    "build_checkpoint_manifest",
    "checkpoint_manifest_symbols",
    "configure_inference_runtime",
    "validate_checkpoint_manifest",
]
