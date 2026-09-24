from client.discovery import build_service_info
from core.discovery import SERVICE_TYPE


def test_clients_with_same_hostname_get_unique_names():
    a = build_service_info(8080, "aaaaaaaa-1111-2222-3333-444444444444", "laptop.lan", "192.168.1.10")
    b = build_service_info(8080, "bbbbbbbb-1111-2222-3333-444444444444", "laptop.lan", "192.168.1.11")

    assert a.type == b.type == SERVICE_TYPE
    assert a.name != b.name
    assert a.server != b.server
    assert a.name == f"laptop-aaaaaaaa1111.{SERVICE_TYPE}"
    assert a.parsed_addresses() == ["192.168.1.10"]
    assert a.properties[b"path"] == b"/metrics/"
