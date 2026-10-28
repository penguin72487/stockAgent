"""Shared freshness boundary for a news owner's physical backing sample."""

# The check's start time is conservative: the physical observation occurs
# sometime after it. An idle heartbeat must not extend this validity window.
BACKING_SAMPLE_MAX_AGE_SECONDS = 90
