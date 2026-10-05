from scripts.benchmark_opening_signal_pipeline import _measurement_summary


def _sample(variant, published, complete, **cache_flags):
    stages = dict.fromkeys(("panel_cache_hit", "checkpoint_cache_hit",
                           "model_cache_hit", "alignment_cache_hit"), True)
    stages.update(cache_flags)
    return {"variant": variant, "all_published_ms": published,
            "all_artifacts_ms": complete, "markets": [{"stages": stages}]}


def test_summary_preserves_cold_sample_and_separates_complete_artifacts():
    records = [_sample("control", 100, 200),
               _sample("control", 10000, 12000, panel_cache_hit=False),
               _sample("candidate", 90, 180)]
    summary = _measurement_summary(records)
    assert summary["all_samples_retained"] is True
    control = summary["variants"]["control"]
    assert control["all_samples"] == {"sample_count": 2,
        "median_all_published_ms": 5050, "median_all_artifacts_ms": 6100}
    assert control["fully_warm_samples"] == {"sample_count": 1,
        "median_all_published_ms": 100, "median_all_artifacts_ms": 200}
    assert control["not_fully_warm_sample_count"] == 1
    assert len(records) == 3


def test_summary_does_not_invent_warm_latency_for_unknown_cache_state():
    sample = _sample("control", 10, 20, alignment_cache_hit=None)
    control = _measurement_summary([sample])["variants"]["control"]
    assert control["fully_warm_samples"] == {"sample_count": 0,
        "median_all_published_ms": None, "median_all_artifacts_ms": None}
    assert control["all_samples"]["sample_count"] == 1


def test_summary_empty_samples_remain_unknown():
    assert _measurement_summary([]) == {"all_samples_retained": True, "variants": {}}
