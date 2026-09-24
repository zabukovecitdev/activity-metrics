from zeroconf.asyncio import AsyncServiceInfo

from collector.discovery import metrics_url
from core.discovery import SERVICE_TYPE

NAME = f"laptop-abc.{SERVICE_TYPE}"


def test_metrics_url_uses_advertised_address_port_and_path():
    info = AsyncServiceInfo(SERVICE_TYPE, NAME, port=8080, parsed_addresses=["192.168.1.10"],
                            properties={"path": "/metrics/"})
    assert metrics_url(info) == "http://192.168.1.10:8080/metrics/"


def test_metrics_url_skips_client_without_ipv4_address():
    info = AsyncServiceInfo(SERVICE_TYPE, NAME, port=8080, parsed_addresses=["fe80::1"])
    assert metrics_url(info) is None
