import platform
import socket

import machineid
from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

from core.discovery import METRICS_PATH, SERVICE_TYPE


def lan_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("10.255.255.255", 1))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"


def build_service_info(port: int, machine_id: str, hostname: str, ip: str) -> ServiceInfo:
    short_id = machine_id.replace("-", "")[:12]
    label = hostname.split(".")[0][:40]
    return ServiceInfo(
        SERVICE_TYPE,
        f"{label}-{short_id}.{SERVICE_TYPE}",
        server=f"activityreporter-{short_id}.local.",
        port=port,
        parsed_addresses=[ip],
        properties={"path": METRICS_PATH, "machine_id": machine_id, "hostname": hostname},
    )


class ServiceAdvertiser:
    def __init__(self, port: int):
        self._info = build_service_info(port, machineid.id(), platform.node(), lan_ip())
        self._zeroconf: AsyncZeroconf | None = None

    async def __aenter__(self) -> ServiceInfo:
        self._zeroconf = AsyncZeroconf()
        await (await self._zeroconf.async_register_service(self._info, allow_name_change=True))
        return self._info

    async def __aexit__(self, *exc) -> None:
        await (await self._zeroconf.async_unregister_service(self._info))
        await self._zeroconf.async_close()
