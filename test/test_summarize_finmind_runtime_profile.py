import pytest

from scripts.summarize_finmind_runtime_profile import summarize


def test_exclusive_categories_and_inclusive_weights_are_distinct():
    result = summarize("thread (1): MainThread;run_once (x:1);_status (x:2);_next_task (x:3) 4\n"
                       "thread (1): MainThread;run_once (x:1);_store (x:2);row_fingerprint (x:4) 2\n"
                       "thread (2): worker;_fetch_rows (x:1) 3\n")
    assert result["total_stack_weight"] == 9
    assert result["disjoint_categories"]["full_status"]["weight"] == 4
    assert result["disjoint_categories"]["semantic_fingerprint"]["weight"] == 2
    assert "dispatch" not in result["disjoint_categories"]
    assert sum(row["weight"] for row in result["disjoint_categories"].values()) == 9
    assert dict(result["inclusive_functions"])["_next_task"] == 4


@pytest.mark.parametrize("body", ["", "thread;fn 0", "thread;fn -1", "thread;fn x"])
def test_empty_or_bad_weights_fail(body):
    with pytest.raises(ValueError):
        summarize(body)
