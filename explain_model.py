from __future__ import annotations

"""Offline, checkpoint-based full-portfolio explainability entry point.

The implementation lives in :mod:`stockagent.explainability`; this wrapper is
kept intentionally small so ``run_fintech_python explain_model.py --config ...``
remains the stable user command.
"""

from stockagent.explainability import main


if __name__ == "__main__":
    main()
