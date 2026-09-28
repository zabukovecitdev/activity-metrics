from __future__ import annotations

import asyncio
import logging

from zeroconf import IPVersion, ServiceListener, Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from core.discovery import METRICS_PATH, SERVICE_TYPE

logger = logging.getLogger(__name__)

RESOLVE_TIMEOUT_MS = 3000


def metrics_url(info: AsyncServiceInfo) -> str | None:
    addresses = info.parsed_addresses(IPVersion.V4Only)
    if not addresses or not info.port:
        return None
    path = (info.properties.get(b"path") or METRICS_PATH.encode()).decode()
    return f"http://{addresses[0]}:{info.port}{path}"


class ServiceDiscovery(ServiceListener):
    """Tracks the metrics URLs of every client advertising SERVICE_TYPE on the LAN."""

    def __init__(self):
        self._urls: dict[str, str] = {}
        self._resolving: dict[str, asyncio.Task] = {}
        self._zeroconf: AsyncZeroconf | None = None
        self._browser: AsyncServiceBrowser | None = None

    def urls(self) -> set[str]:
        return set(self._urls.values())

    # The browser calls these from the event loop thread, so resolving can be
    # scheduled as a task directly.
    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        self._schedule_resolve(type_, name)

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        # Fires when a client's address or port changes, e.g. after a DHCP renewal.
        self._schedule_resolve(type_, name)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        # Cancel an in-flight resolve, or it would re-add the client after it left.
        task = self._resolving.pop(name, None)
        if task:
            task.cancel()
        url = self._urls.pop(name, None)
        logger.info("Client %s left (%s)", name, url)

    def _schedule_resolve(self, type_: str, name: str) -> None:
        previous = self._resolving.pop(name, None)
        if previous:
            previous.cancel()
        self._resolving[name] = asyncio.create_task(self._resolve(type_, name))

    async def _resolve(self, type_: str, name: str) -> None:
        try:
            info = AsyncServiceInfo(type_, name)
            if not await info.async_request(self._zeroconf.zeroconf, RESOLVE_TIMEOUT_MS):
                logger.warning("Could not resolve client %s", name)
                return
            url = metrics_url(info)
            if url is None:
                logger.warning("Client %s advertises no IPv4 address or port", name)
                return
            self._urls[name] = url
            logger.info("Discovered client %s at %s", name, url)
        finally:
            if self._resolving.get(name) is asyncio.current_task():
                del self._resolving[name]

    async def __aenter__(self) -> ServiceDiscovery:
        self._zeroconf = AsyncZeroconf()
        self._browser = AsyncServiceBrowser(self._zeroconf.zeroconf, SERVICE_TYPE, listener=self)
        return self

    async def __aexit__(self, *exc) -> None:
        for task in self._resolving.values():
            task.cancel()
        await self._browser.async_cancel()
        await self._zeroconf.async_close()
