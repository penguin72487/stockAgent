"""Observe chosen connections, optionally retiring idle sockets before sending.

Each news worker owns a serial Session and each origin adapter retains one
pool. Hooks delegate to urllib3's original checkout and connect implementations;
they do not skip dropped-connection checks, weaken TLS or resend requests.
The small observation contains no URL, IP, credentials, headers or body.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import ssl
import time
from urllib.parse import urlsplit

import requests
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.poolmanager import PoolManager


CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION = 1
IDLE_CONNECTION_EXPIRY_CONTRACT_VERSION = 1
# Native failures repeatedly reused an apparently-open 58.5--58.9s idle
# socket. Retire it before that observed failure band, not after a failed GET.
# This is a client connection lifetime, NOT source pacing or a retry deadline.
MAX_IDLE_CONNECTION_SECONDS = 45
CONNECTION_OBSERVATION_FIELDS = frozenset({
    'contract_version', 'socket_open_at_checkout', 'previous_request_count',
    'idle_since_previous_terminal_seconds', 'connect_attempts',
    'connect_completed', 'connect_wall_seconds',
})
IDLE_BOUNDARIES = ((1, 'lt_1s'), (5, 'lt_5s'), (15, 'lt_15s'),
                   (60, 'lt_60s'), (300, 'lt_300s'))
CONNECTION_BUCKETS = frozenset({
    'unobserved', 'newly_connected', 'failed_connect', 'not_connected',
    'open_reused_idle_unknown', 'open_reused_idle_ge_300s',
    *('open_reused_idle_' + label for _, label in IDLE_BOUNDARIES),
})


def valid_connection_observation(observation, *, request_wall=None):
    """Validate an actual hook observation; absence is not zero connect time."""
    if type(observation) is not dict or set(observation) != CONNECTION_OBSERVATION_FIELDS:
        return False
    if request_wall is not None and (type(request_wall) not in (int, float)
            or not math.isfinite(request_wall) or request_wall < 0):
        return False
    idle, wall = observation['idle_since_previous_terminal_seconds'], observation['connect_wall_seconds']
    attempts, completed = observation['connect_attempts'], observation['connect_completed']
    previous = observation['previous_request_count']
    return (type(observation['contract_version']) is int
        and observation['contract_version'] == CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION
        and type(observation['socket_open_at_checkout']) is bool
        and type(previous) is int and 0 <= previous < 2 ** 63
        and (idle is None and previous == 0 or previous > 0
             and type(idle) in (int, float) and math.isfinite(idle) and idle >= 0)
        and type(attempts) is int and type(completed) is int and 0 <= completed <= attempts < 2 ** 63
        and type(wall) in (int, float) and math.isfinite(wall) and wall >= 0
        and (attempts > 0 or wall == 0)
        and (request_wall is None or wall <= request_wall + 1e-6))


def connection_observation_bucket(observation):
    if observation is None:
        return 'unobserved'
    if observation['connect_attempts']:
        return 'newly_connected' if observation['connect_completed'] else 'failed_connect'
    if not observation['socket_open_at_checkout']:
        return 'not_connected'
    idle = observation['idle_since_previous_terminal_seconds']
    if idle is None:
        return 'open_reused_idle_unknown'
    for boundary, label in IDLE_BOUNDARIES:
        if idle < boundary:
            return 'open_reused_idle_' + label
    return 'open_reused_idle_ge_300s'


class _ObserveConnect:
    _news_connect_attempts = 0
    _news_connect_completed = 0
    _news_connect_wall = 0.0
    _news_terminal_count = 0
    _news_terminal_at = None

    def connect(self):
        self._news_connect_attempts += 1
        started = time.monotonic()
        try:
            result = super().connect()
            self._news_connect_completed += 1
            return result
        finally:
            self._news_connect_wall += time.monotonic() - started


class _ObservedHTTPConnection(_ObserveConnect, HTTPConnection):
    pass


class _ObservedHTTPSConnection(_ObserveConnect, HTTPSConnection):
    pass


@dataclass
class _CheckoutObservation:
    connection: _ObserveConnect
    socket_open: bool
    previous: int
    idle: float | None
    attempts_before: int
    completed_before: int
    connect_wall_before: float
    terminal: dict | None = None

    @classmethod
    def checkout(cls, connection):
        previous = connection._news_terminal_at
        return cls(connection, connection.sock is not None, connection._news_terminal_count,
                   None if previous is None else max(0.0, time.monotonic() - previous),
                   connection._news_connect_attempts, connection._news_connect_completed,
                   connection._news_connect_wall)

    def finish(self):
        if self.terminal is None:
            connection = self.connection
            self.terminal = {
                'contract_version': CONNECTION_REUSE_OBSERVATION_CONTRACT_VERSION,
                'socket_open_at_checkout': self.socket_open,
                'previous_request_count': self.previous,
                'idle_since_previous_terminal_seconds': self.idle,
                'connect_attempts': connection._news_connect_attempts - self.attempts_before,
                'connect_completed': connection._news_connect_completed - self.completed_before,
                'connect_wall_seconds': max(0.0, connection._news_connect_wall - self.connect_wall_before),
            }
            connection._news_terminal_count += 1
            connection._news_terminal_at = time.monotonic()
        return dict(self.terminal)


class _ObserveCheckout:
    def _get_conn(self, timeout=None):
        # The native checkout has already checked for a dropped connection.
        connection = super()._get_conn(timeout=timeout)
        limit = self._news_adapter.max_idle_seconds
        terminal = connection._news_terminal_at
        if (limit is not None and connection.sock is not None and terminal is not None
                and time.monotonic() - terminal >= limit):
            # Reconnect the same validated origin before sending anything.
            # No replay, new request slot, cookie change or TLS override.
            connection.close()
            self._news_adapter._news_idle_expirations += 1
        self._news_adapter._news_checkout = _CheckoutObservation.checkout(connection)
        return connection


class _ObservedHTTPPool(_ObserveCheckout, HTTPConnectionPool):
    ConnectionCls = _ObservedHTTPConnection


class _ObservedHTTPSPool(_ObserveCheckout, HTTPSConnectionPool):
    ConnectionCls = _ObservedHTTPSConnection


class _ObservationPoolManager(PoolManager):
    def __init__(self, adapter, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._news_adapter = adapter
        # Never mutate urllib3's shared scheme map or another Session's pools.
        self.pool_classes_by_scheme = {'http': _ObservedHTTPPool, 'https': _ObservedHTTPSPool}

    def _new_pool(self, scheme, host, port, request_context=None):
        pool = super()._new_pool(scheme, host, port, request_context=request_context)
        pool._news_adapter = self._news_adapter
        return pool


class ConnectionObservingAdapter(requests.adapters.HTTPAdapter):
    """One serial origin adapter; optional idle lifetime, no additional retry."""

    def __init__(self, *args, max_idle_seconds=None, **kwargs):
        if (max_idle_seconds is not None and (type(max_idle_seconds) not in {int, float}
                or not math.isfinite(max_idle_seconds) or max_idle_seconds <= 0)):
            raise ValueError('invalid_idle_connection_lifetime')
        self.max_idle_seconds = max_idle_seconds
        self._news_idle_expirations = 0
        self._news_checkout = None
        self._news_tls_origin = None
        self._news_tls_context = None
        super().__init__(*args, **kwargs)

    def bind_verified_tls_context(self, origin, context):
        """Bind before first use, without changing unrelated origin pools."""
        if self._news_tls_origin is not None or len(self.poolmanager.pools):
            raise ValueError('origin_tls_context_requires_unused_pool')
        self._require_verified_tls_context(context)
        self._news_tls_origin, self._news_tls_context = origin, context

    @staticmethod
    def _require_verified_tls_context(context):
        if (not isinstance(context, ssl.SSLContext) or context.verify_mode != ssl.CERT_REQUIRED
                or not context.check_hostname or context.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN):
            raise requests.exceptions.SSLError('origin_tls_requires_existing_root_and_hostname')

    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        if self._news_tls_context is not None:
            parts = urlsplit(request.url)
            if (verify is not True or cert is not None
                    or f'{parts.scheme}://{parts.netloc}' != self._news_tls_origin):
                raise requests.exceptions.SSLError('reviewed_origin_tls_scope_mismatch')
            self._require_verified_tls_context(self._news_tls_context)
        host, options = super().build_connection_pool_key_attributes(request, verify, cert)
        if self._news_tls_context is not None:
            options['ssl_context'] = self._news_tls_context
            options['cert_reqs'] = 'CERT_REQUIRED'
        return host, options

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self._pool_connections, self._pool_maxsize, self._pool_block = connections, maxsize, block
        self.poolmanager = _ObservationPoolManager(self, num_pools=connections, maxsize=maxsize,
                                                  block=block, **pool_kwargs)

    def send(self, request, **kwargs):
        self._news_checkout = None
        return super().send(request, **kwargs)

    def finish_connection_observation(self):
        return self._news_checkout.finish() if self._news_checkout is not None else None

    def take_idle_expirations(self):
        value, self._news_idle_expirations = self._news_idle_expirations, 0
        return value

    def close(self):
        self._news_checkout = None
        super().close()
