"""Versioned PostgreSQL DDL, packaged and hashed with the Python code release."""

SCHEMA_VERSION = 1
DDL = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id text PRIMARY KEY,
    capabilities text[] NOT NULL,
    cpu_slots integer NOT NULL CHECK (cpu_slots > 0),
    memory_bytes bigint NOT NULL CHECK (memory_bytes > 0),
    scratch_bytes bigint NOT NULL CHECK (scratch_bytes >= 0),
    observation jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS jobs (
    key text PRIMARY KEY,
    identity_sha256 text NOT NULL,
    spec jsonb NOT NULL,
    kind text NOT NULL,
    priority integer NOT NULL CHECK (priority BETWEEN 0 AND 100),
    state text NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    cpu_slots integer NOT NULL CHECK (cpu_slots > 0),
    memory_bytes bigint NOT NULL CHECK (memory_bytes > 0),
    scratch_bytes bigint NOT NULL CHECK (scratch_bytes >= 0),
    node_id text REFERENCES nodes(node_id),
    worker_id text,
    lease_token uuid,
    lease_until timestamptz,
    ready_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result jsonb,
    CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    CHECK ((state = 'running') = (lease_token IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS dependencies (
    job_key text NOT NULL REFERENCES jobs(key),
    dependency_key text NOT NULL REFERENCES jobs(key),
    PRIMARY KEY (job_key, dependency_key),
    CHECK (job_key <> dependency_key)
);
CREATE TABLE IF NOT EXISTS attempts (
    job_key text NOT NULL REFERENCES jobs(key),
    attempt integer NOT NULL,
    node_id text NOT NULL,
    worker_id text NOT NULL,
    lease_token uuid NOT NULL UNIQUE,
    state text NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'expired')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    result jsonb,
    PRIMARY KEY (job_key, attempt)
);
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(kind, ready_at, created_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(lease_until) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS jobs_node_idx ON jobs(node_id) WHERE state = 'running';
"""
