"""Bounded per-worker connection pools, isolated by the exact source origin.

The Session still owns the same cookie jar and request semantics. Only the
connection-pool failure domain changes; no transparent retries are introduced.
"""

from __future__ import annotations

from collections import OrderedDict

import requests

from downloader.news_fulltext_connection_observation import ConnectionObservingAdapter, MAX_IDLE_CONNECTION_SECONDS
from downloader.news_fulltext_robots import policy_origin
from downloader.news_fulltext_metrics import CONNECTION_ORIGIN_ISOLATION_CONTRACT_VERSION
from downloader.news_fulltext_tls import reviewed_origin_context


MAX_CONNECTION_ORIGINS_PER_WORKER = 10


class OriginConnectionSession(requests.Session):
    """One serial worker's Session, not shared between concurrently sending threads.

    Requests' normal adapter caches ten origin pools per scheme. Cache at most
    ten in total here, with one reusable connection per origin: a worker sends
    only one request at a time. Eviction and shutdown close the displaced pool.
    """

    def __init__(self):
        super().__init__()
        self.origin_adapters = OrderedDict()
        self.origin_pool_evictions = 0
        self.idle_connection_expirations = 0

    def _new_origin_adapter(self):
        return ConnectionObservingAdapter(pool_connections=1, pool_maxsize=1, max_retries=0,
                                          max_idle_seconds=MAX_IDLE_CONNECTION_SECONDS)

    def finish_connection_observation(self, url):
        # Do not create/refresh pools merely to observe an injected transport.
        adapter = self.origin_adapters.get(policy_origin(url))
        if isinstance(adapter, ConnectionObservingAdapter):
            observation = adapter.finish_connection_observation()
            self.idle_connection_expirations += adapter.take_idle_expirations()
            return observation
        return None

    def get_adapter(self, url):
        # Use the canonical origin grammar, not a URL-prefix match which can
        # accidentally include a different host. Query/path never create pools.
        origin = policy_origin(url)
        adapter = self.origin_adapters.pop(origin, None)
        if adapter is None:
            if len(self.origin_adapters) == MAX_CONNECTION_ORIGINS_PER_WORKER:
                _, displaced = self.origin_adapters.popitem(last=False)
                displaced.close()
                self.origin_pool_evictions += 1
            adapter = self._new_origin_adapter()
            try:
                context = reviewed_origin_context(origin)
                if context is not None:
                    adapter.bind_verified_tls_context(origin, context)
            except (OSError, ValueError) as error:
                adapter.close()
                raise requests.exceptions.SSLError('reviewed_origin_tls_configuration_invalid') from error
            except Exception:
                adapter.close()
                raise
        self.origin_adapters[origin] = adapter
        return adapter

    def reset_origin(self, url):
        """Discard only this failed origin's pool; preserve other pools/cookies."""
        adapter = self.origin_adapters.pop(policy_origin(url), None)
        if adapter is None:
            return False
        adapter.close()
        return True

    def close(self):
        # Clear before closing so repeated shutdown cannot leave cached closed
        # pools. Default Session adapters never send here, but still need close.
        adapters = tuple(self.origin_adapters.values())
        self.origin_adapters.clear()
        try:
            for adapter in adapters:
                adapter.close()
        finally:
            super().close()
