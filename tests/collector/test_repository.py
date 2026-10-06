import re

import pytest

from activityreporter.collector.repository import load_agents, metrics_url


def write(tmp_path, content: str) -> str:
    path = tmp_path / "collector.toml"
    path.write_text(content)
    return str(path)


def test_agents_are_read_in_order(tmp_path):
    path = write(tmp_path, 'agents = ["http://192.168.1.20:8080", "https://b.example"]')
    assert load_agents(path) == ["http://192.168.1.20:8080", "https://b.example"]


def test_the_repo_default_is_valid():
    assert load_agents("collector.toml")


def test_missing_file_raises_os_error(tmp_path):
    with pytest.raises(OSError):
        load_agents(str(tmp_path / "missing.toml"))


@pytest.mark.parametrize(
    "content, message",
    [
        ("agents = [", "Invalid"),
        ("", "non-empty list"),
        ("agents = []", "non-empty list"),
        ('agents = "http://a:8080"', "non-empty list"),
        ('agents = ["http://a:8080", "192.168.1.20:8080"]', "agents[1] '192.168.1.20:8080' must start with http://"),
        ('agents = ["localhost:8080"]', "agents[0]"),
        ('agents = ["ftp://a"]', "agents[0]"),
        ("agents = [8080]", "agents[0] 8080"),
    ],
)
def test_invalid_file_raises_a_value_error_naming_the_problem(tmp_path, content, message):
    path = write(tmp_path, content)
    with pytest.raises(ValueError, match=re.escape(path)) as error:
        load_agents(path)
    assert message in str(error.value)


def test_metrics_url_is_under_the_agent_url():
    assert metrics_url("http://192.168.1.20:8080") == "http://192.168.1.20:8080/v1/metrics"
    assert metrics_url("http://192.168.1.20:8080/") == "http://192.168.1.20:8080/v1/metrics"
