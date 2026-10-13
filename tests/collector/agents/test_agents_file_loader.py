import re

import pytest

from activityreporter.collector.agents.agents_file_loader import AgentsFileLoader


def write(tmp_path, content: str) -> str:
    path = tmp_path / "collector.toml"
    path.write_text(content)
    return str(path)


def test_agents_are_read_in_order(tmp_path):
    path = write(tmp_path, 'agents = ["http://192.168.1.20:8080", "https://b.example"]')
    assert AgentsFileLoader.load(path) == ["http://192.168.1.20:8080", "https://b.example"]


def test_the_repo_default_is_valid():
    assert AgentsFileLoader.load("collector.toml")


def test_missing_file_raises_os_error(tmp_path):
    with pytest.raises(OSError):
        AgentsFileLoader.load(str(tmp_path / "missing.toml"))


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
        AgentsFileLoader.load(path)
    assert message in str(error.value)
