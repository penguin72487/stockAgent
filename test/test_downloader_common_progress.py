from __future__ import annotations

from types import SimpleNamespace

import pytest

from downloader import common


@pytest.mark.parametrize("interactive, expected_interval", [(True, 0.1), (False, 10.0)])
def test_parallel_tasks_keep_progress_with_bounded_non_tty_refresh(
    monkeypatch: pytest.MonkeyPatch, interactive: bool, expected_interval: float,
) -> None:
    observed: dict[str, object] = {}

    class Progress:
        def __init__(self, **kwargs: object) -> None:
            observed.update(kwargs)
            observed["updates"] = 0

        def update(self, count: int) -> None:
            observed["updates"] = int(observed["updates"]) + count

        def close(self) -> None:
            observed["closed"] = True

    monkeypatch.setattr(common, "sys", SimpleNamespace(
        stderr=SimpleNamespace(isatty=lambda: interactive),
    ))
    monkeypatch.setattr(common, "tqdm", Progress)

    result = common.run_parallel_tasks(
        [1, 2, 3], lambda value: value * 2,
        max_workers=2, desc="sample", unit="item",
    )

    assert sorted(result) == [2, 4, 6]
    assert observed == {
        "total": 3, "desc": "sample", "unit": "item",
        "mininterval": expected_interval,
        "updates": 3, "closed": True,
    }
