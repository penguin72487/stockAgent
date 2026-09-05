# Downloader architecture and correctness contract

This document is the shortest authoritative map for humans and coding agents
changing a downloader.  Provider-specific parsers remain separate; transport,
artifact publication, scheduling evidence, and static inventory are shared.

## First-principles model

A downloader is a state transition, not an HTTP call:

```text
schedule -> provider adapter -> rate-limited transport -> raw evidence
         -> normalize/validate -> atomic mutable workspace artifact
         -> source receipt/progress -> catalog audit -> immutable release
```

Each arrow has a different truth condition.  A successful HTTP status does not
prove payload completeness.  A Parquet file does not prove point-in-time
availability.  A completed source receipt does not publish a packed release.
The catalog-backed release wrapper remains the only mutable-to-immutable
boundary.

For one independent provider bucket with `C` allowed cost units per `W` seconds,
the request-cost ceiling is `C/W`.  If observed request latency is `L`, roughly
`ceil((C/W) * L)` in-flight workers are enough to fill the bucket.  More workers
cannot increase provider throughput; they only add memory, scheduling, and
connection pressure.  Independent provider buckets may run concurrently.
Endpoints that share an IP/account/weight bucket must share the same limiter
name and use the documented request cost.

## Shared modules

- `downloader/common.py`: provider limit registry, process/host-shared limiter,
  bounded backoff, truthful logical progress/ETA, and bounded parallel task
  helpers.
- `downloader/http_transport.py`: timeout, request-start pacing, retryable HTTP
  statuses, `Retry-After`, URL/query redaction, and attempt telemetry.  It never
  interprets provider data.
- `downloader/artifact_io.py`: collision-safe same-filesystem atomic bytes,
  text, JSON, and Parquet publication plus streaming SHA-256.  Small control
  receipts may request fsync durability; high-rate raw payloads should not pay a
  storage barrier per response.
- `scripts/write_downloader_step_receipt.py`: sanitized scheduler lifecycle
  evidence.  It deliberately excludes commands, URLs, and arguments because
  they may contain credentials.
- `scripts/audit_downloader_contracts.py`: reproducible static inventory of
  canonical module and script-level entrypoints.  Flags are review leads, not
  proof of runtime correctness.

## Required adapter contract

Every networked entrypoint must make these decisions explicit:

1. Source identity and point-in-time availability.
2. Provider bucket, request cost, timeout, bounded retries, and terminal errors.
3. Idempotent resume unit: symbol, date window, page cursor, or immutable object.
4. Raw evidence and content hash when the source can later change.
5. Schema, primary key, sort order, deduplication rule, coverage bounds, and gap
   semantics.  Missing data remains missing/partial/blocked; it is never filled
   merely to make a job complete.
6. Atomic workspace output followed by a source receipt.  A failed refresh must
   preserve the last certified canonical output and publish failure evidence.
7. Progress denominator based on logical completion units.  HTTP page/request
   counts are telemetry only and must not advance the completion ratio.
8. Catalog audit and release publication, when enabled, only after the writer
   exits and all freshness/coverage/rights gates pass.

Provider clients may keep specialized session, authentication, pagination, and
schema logic.  Do not force WebSocket streams, SDK calls, weighted REST APIs,
and static archives through one parser or one global rate limit.

## FRED vintage example

`download_fred_crypto_macro_vintages.py` demonstrates the intended historical
pattern.  Closed vintage windows are content-addressed and reused only when the
window receipt, age, gzip payload, and uncompressed SHA-256 all verify.  The
window containing the requested end date is always refreshed.  Independent
windows run concurrently behind one `fred_api` limiter, which hides request
latency without exceeding the provider policy.  A failed window writes its own
receipt, leaves the previous `observations.parquet` untouched, and marks the run
summary failed.

## Reproducible audit

Run from the repository root:

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_downloader_contracts.py \
  --output-dir artifacts/downloader_review/latest
```

The CSV is convenient for sorting.  The JSON is the AI-readable source for file
role, transport type, endpoints, rate profiles, credential variable names,
parallelism, progress, atomic publication, receipts, scheduler visibility, and
static flags.  It never includes credential values.

Static success is only the first gate.  Changes must also run focused parser,
failure-injection, resume, atomicity, and integration tests.  Scheduled-service
acceptance additionally checks the durable step receipt, source receipt, data
coverage, and output hash rather than only process exit status.
