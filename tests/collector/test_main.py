from collector.main import static_endpoints


def test_static_endpoints_are_split_and_trimmed():
    assert static_endpoints("http://a:8080/metrics/, http://b:8080/metrics/,") == {
        "http://a:8080/metrics/",
        "http://b:8080/metrics/",
    }


def test_static_endpoints_default_to_empty():
    assert static_endpoints("") == set()
