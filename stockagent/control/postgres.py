"""Transactional work claims and fencing; never a collector or execution ledger."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any
import uuid

from stockagent.control.contracts import SUPPORTED_KINDS, WorkSpec, identifier, integer
from stockagent.control._schema import DDL, SCHEMA_VERSION


class LeaseLost(RuntimeError):
    """The attempt cannot publish completion after expiry or reassignment."""


@dataclass(frozen=True)
class Claim:
    spec: WorkSpec
    attempt: int
    node_id: str
    worker_id: str
    token: str
    lease_until: datetime


class ControlStore:
    def __init__(self, dsn: str, *, schema: str = 'stockagent_control'):
        if not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}', schema):
            raise ValueError('invalid control schema')
        # Driver is an optional control-role dependency, not a torch/runtime import.
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
        self._json = Jsonb
        self.connection = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)
        self.schema = schema
        self.connection.execute('SET statement_timeout = 10000')
        self.connection.execute('SET lock_timeout = 5000')
        self.connection.execute(psycopg.sql.SQL('SET search_path TO {}').format(psycopg.sql.Identifier(schema)))
        # Startup admission is repeated inside every operation transaction.
        # A connected worker cannot retain permission after a version change.
        try:
            existing = self.connection.execute('SELECT to_regclass(%s) AS version_table',
                                                (schema + '.contract_version',)).fetchone()
            if existing['version_table'] is not None:
                with self.connection.transaction():
                    self._admit_contract()
            elif self.connection.execute('''SELECT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=%s AND c.relkind IN ('r','p')
            ) AS tables_exist''', (schema,)).fetchone()['tables_exist']:
                raise ValueError('missing stored control contract; explicit migration required')
        except Exception:
            self.connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()

    def _admit_contract(self) -> None:
        from psycopg import errors, sql
        try:
            # SHARE is compatible across workers but blocks contract metadata
            # INSERT/UPDATE/DELETE/TRUNCATE/DDL through transaction completion.
            # Acquire it before the first SELECT in a repeatable-read snapshot.
            self.connection.execute(sql.SQL('LOCK TABLE {}.contract_version IN SHARE MODE').format(
                sql.Identifier(self.schema)))
            versions = self.connection.execute('SELECT version FROM contract_version ORDER BY version').fetchall()
        except errors.UndefinedTable as error:
            raise ValueError('missing stored control contract; explicit migration required') from error
        if versions != [{'version': SCHEMA_VERSION}]:
            raise ValueError('unsupported stored control contract; explicit migration required')

    @contextmanager
    def _transaction(self, *, repeatable_read: bool = False):
        with self.connection.transaction():
            if repeatable_read:
                self.connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            self._admit_contract()
            yield

    def initialize(self) -> None:
        from psycopg import sql
        with self.connection.transaction():
            self.connection.execute('SELECT pg_advisory_xact_lock(673819261023)')
            self.connection.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(self.schema)))
            existing = self.connection.execute('SELECT to_regclass(%s) AS version_table',
                                                (self.schema + '.contract_version',)).fetchone()
            if existing['version_table'] is not None:
                self._admit_contract()
            else:
                if self.connection.execute('''SELECT EXISTS (
                    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname=%s AND c.relkind IN ('r','p')
                ) AS tables_exist''', (self.schema,)).fetchone()['tables_exist']:
                    raise ValueError('missing stored control contract; explicit migration required')
                self.connection.execute('CREATE TABLE contract_version (version integer PRIMARY KEY)')
                self.connection.execute('INSERT INTO contract_version VALUES (%s)', (SCHEMA_VERSION,))
            self.connection.execute(DDL)

    def register_node(self, node_id: str, *, cpu_slots: int, memory_bytes: int,
                      scratch_bytes: int, observation: dict[str, Any],
                      capabilities: tuple[str, ...] = ('verify-code-release',), ttl_seconds: int = 60) -> None:
        identifier(node_id)
        integer(cpu_slots, 'cpu_slots', minimum=1, maximum=1024)
        integer(memory_bytes, 'memory_bytes', minimum=1, maximum=2**60)
        integer(scratch_bytes, 'scratch_bytes', maximum=2**60)
        integer(ttl_seconds, 'node ttl', minimum=1, maximum=3600)
        if not capabilities or not set(capabilities) <= SUPPORTED_KINDS or not isinstance(observation, dict) or not observation:
            raise ValueError('node capability/observation contract is incomplete')
        with self._transaction():
            self.connection.execute('''INSERT INTO nodes
                (node_id, capabilities, cpu_slots, memory_bytes, scratch_bytes, observation, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp() + %s * interval '1 second')
                ON CONFLICT (node_id) DO UPDATE SET capabilities=excluded.capabilities,
                  cpu_slots=excluded.cpu_slots, memory_bytes=excluded.memory_bytes,
                  scratch_bytes=excluded.scratch_bytes, observation=excluded.observation,
                  expires_at=excluded.expires_at, updated_at=clock_timestamp()''',
                (node_id, list(capabilities), cpu_slots, memory_bytes, scratch_bytes,
                 self._json(observation), ttl_seconds))

    def submit(self, spec: WorkSpec) -> dict[str, Any]:
        with self._transaction():
            self.connection.execute('''INSERT INTO jobs
                (key,identity_sha256,spec,kind,priority,state,max_attempts,cpu_slots,memory_bytes,scratch_bytes)
                VALUES (%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s) ON CONFLICT (key) DO NOTHING''',
                (spec.key, spec.identity_sha256, self._json(spec.as_dict()), spec.kind, spec.priority,
                 spec.max_attempts, spec.cpu_slots, spec.memory_bytes, spec.scratch_bytes))
            row = self.connection.execute('SELECT * FROM jobs WHERE key=%s FOR UPDATE', (spec.key,)).fetchone()
            if row['identity_sha256'] != spec.identity_sha256:
                raise ValueError('existing work key has incompatible inputs or work contract')
            # Dependencies refer to already-submitted immutable jobs. Cycles cannot be added later.
            for dependency in spec.dependencies:
                if self.connection.execute('SELECT 1 FROM jobs WHERE key=%s', (dependency,)).fetchone() is None:
                    raise ValueError('dependency must already be submitted')
                self.connection.execute('INSERT INTO dependencies VALUES (%s,%s) ON CONFLICT DO NOTHING',
                                        (spec.key, dependency))
            return row

    def _expire(self) -> int:
        rows = self.connection.execute('''SELECT * FROM jobs WHERE state='running'
            AND lease_until <= clock_timestamp() FOR UPDATE SKIP LOCKED''').fetchall()
        for row in rows:
            self.connection.execute('''UPDATE attempts SET state='expired',finished_at=clock_timestamp(),
                result=%s WHERE job_key=%s AND attempt=%s AND state='running' ''',
                (self._json({'reason': 'lease_expired', 'side_effects': 'read_only_work_kind'}), row['key'], row['attempt']))
            self.connection.execute('''UPDATE jobs SET state=%s,lease_token=NULL,lease_until=NULL,
                ready_at=clock_timestamp(),updated_at=clock_timestamp() WHERE key=%s''',
                ('pending' if row['attempt'] < row['max_attempts'] else 'failed', row['key']))
        return len(rows)

    def expire_leases(self) -> int:
        with self._transaction():
            return self._expire()

    def claim(self, node_id: str, worker_id: str, *, lease_seconds: int = 60) -> Claim | None:
        identifier(node_id); identifier(worker_id)
        integer(lease_seconds, 'lease', minimum=1, maximum=3600)
        with self._transaction():
            node = self.connection.execute('SELECT * FROM nodes WHERE node_id=%s FOR UPDATE', (node_id,)).fetchone()
            now = self.connection.execute('SELECT clock_timestamp() AS now').fetchone()['now']
            if node is None or node['expires_at'] <= now:
                raise ValueError('node requires a fresh resource/capability observation')
            self._expire()
            used = self.connection.execute('''SELECT COALESCE(SUM(cpu_slots),0) AS cpu,
                COALESCE(SUM(memory_bytes),0) AS memory,COALESCE(SUM(scratch_bytes),0) AS scratch
                FROM jobs WHERE state='running' AND node_id=%s AND lease_until>clock_timestamp()''', (node_id,)).fetchone()
            row = self.connection.execute('''SELECT j.* FROM jobs j WHERE j.state='pending'
                AND j.ready_at<=clock_timestamp() AND j.kind=ANY(%s)
                AND j.cpu_slots<=%s AND j.memory_bytes<=%s AND j.scratch_bytes<=%s
                AND NOT EXISTS (SELECT 1 FROM dependencies d JOIN jobs p ON p.key=d.dependency_key
                                WHERE d.job_key=j.key AND p.state<>'succeeded')
                ORDER BY j.priority + EXTRACT(EPOCH FROM clock_timestamp()-j.ready_at)/60 DESC,
                         j.created_at,j.key FOR UPDATE OF j SKIP LOCKED LIMIT 1''',
                (node['capabilities'], node['cpu_slots']-used['cpu'], node['memory_bytes']-used['memory'],
                 node['scratch_bytes']-used['scratch'])).fetchone()
            if row is None:
                return None
            token = str(uuid.uuid4())
            row = self.connection.execute('''UPDATE jobs SET state='running',attempt=attempt+1,
                node_id=%s,worker_id=%s,lease_token=%s,lease_until=clock_timestamp()+%s*interval '1 second',
                updated_at=clock_timestamp() WHERE key=%s RETURNING *''',
                (node_id, worker_id, token, lease_seconds, row['key'])).fetchone()
            self.connection.execute('''INSERT INTO attempts (job_key,attempt,node_id,worker_id,lease_token,state)
                VALUES (%s,%s,%s,%s,%s,'running')''', (row['key'], row['attempt'], node_id, worker_id, token))
            return Claim(WorkSpec.from_dict(row['spec']), row['attempt'], node_id, worker_id, token, row['lease_until'])

    def _owned(self, claim: Claim) -> dict[str, Any]:
        row = self.connection.execute('SELECT * FROM jobs WHERE key=%s FOR UPDATE', (claim.spec.key,)).fetchone()
        now = self.connection.execute('SELECT clock_timestamp() AS now').fetchone()['now']
        if (row is None or row['state'] != 'running' or row['lease_until'] <= now
                or row['attempt'] != claim.attempt or str(row['lease_token']) != claim.token
                or row['node_id'] != claim.node_id or row['worker_id'] != claim.worker_id
                or row['identity_sha256'] != claim.spec.identity_sha256):
            raise LeaseLost('attempt no longer owns an unexpired work lease')
        return row

    def heartbeat(self, claim: Claim, *, lease_seconds: int = 60) -> datetime:
        integer(lease_seconds, 'lease', minimum=1, maximum=3600)
        with self._transaction():
            self._owned(claim)
            row = self.connection.execute('''UPDATE jobs SET lease_until=clock_timestamp()+%s*interval '1 second',
                updated_at=clock_timestamp() WHERE key=%s RETURNING lease_until''', (lease_seconds, claim.spec.key)).fetchone()
            return row['lease_until']

    def finish(self, claim: Claim, result: dict[str, Any]) -> None:
        inputs = dict(claim.spec.inputs)
        if (not isinstance(result, dict) or result.get('source_files_verified') is not True
                or result.get('receipt_sha256') != inputs['receipt_sha256']
                or result.get('source_sha256') != inputs['source_sha256']):
            raise ValueError('completion requires matching canonical source-verification evidence')
        with self._transaction():
            self._owned(claim)
            self._terminal(claim, result, 'succeeded', 'succeeded', 0)

    def fail(self, claim: Claim, error: str, *, retry_delay_seconds: int = 30) -> str:
        integer(retry_delay_seconds, 'retry delay', maximum=86400)
        if not isinstance(error, str) or not error.strip() or len(error) > 4096:
            raise ValueError('failure requires a bounded nonempty reason')
        state = 'pending' if claim.attempt < claim.spec.max_attempts else 'failed'
        with self._transaction():
            self._owned(claim)
            self._terminal(claim, {'reason': error}, state, 'failed', retry_delay_seconds)
        return state

    def _terminal(self, claim: Claim, result: dict[str, Any], state: str, attempt_state: str, delay: int) -> None:
        self.connection.execute('''UPDATE attempts SET state=%s,finished_at=clock_timestamp(),result=%s
            WHERE job_key=%s AND attempt=%s''', (attempt_state, self._json(result), claim.spec.key, claim.attempt))
        self.connection.execute('''UPDATE jobs SET state=%s,lease_token=NULL,lease_until=NULL,result=%s,
            ready_at=clock_timestamp()+%s*interval '1 second',updated_at=clock_timestamp() WHERE key=%s''',
            (state, self._json(result), delay, claim.spec.key))

    def snapshot(self) -> dict[str, Any]:
        with self._transaction(repeatable_read=True):
            return {'schema_version': SCHEMA_VERSION, **{
                name: self.connection.execute(f'SELECT * FROM {name} ORDER BY {key}').fetchall()
                for name, key in [('nodes','node_id'), ('jobs','key'),
                                  ('dependencies','job_key,dependency_key'), ('attempts','job_key,attempt')]}}
