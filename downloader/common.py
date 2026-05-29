from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import TypeVar

from tqdm import tqdm

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


def resolve_end_date(value: str) -> str:
    text = value.strip().lower()
    if text in {"today", "now"}:
        return date.today().isoformat()
    return value.strip()


def run_parallel_tasks(
    items: Iterable[TItem],
    worker: Callable[[TItem], TResult],
    *,
    max_workers: int,
    desc: str,
    unit: str = "item",
    on_error: Callable[[TItem, Exception], TResult] | None = None,
) -> list[TResult]:
    item_list = list(items)
    if not item_list:
        return []

    results: list[TResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        futures = {executor.submit(worker, item): item for item in item_list}
        progress = tqdm(total=len(futures), desc=desc, unit=unit)
        try:
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    if on_error is None:
                        raise
                    result = on_error(item, exc)
                results.append(result)
                progress.update(1)
        finally:
            progress.close()

    return results
