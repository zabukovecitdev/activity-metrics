import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_generator():
    spec = importlib.util.spec_from_file_location("behaviour_catalogue", ROOT / "scripts" / "behaviour_catalogue.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_behaviour_catalogue_is_up_to_date():
    expected = load_generator().render(ROOT / "tests")
    actual = (ROOT / "docs" / "behaviour.md").read_text()
    assert actual == expected, "docs/behaviour.md is out of date; run `make docs` and commit the result."


def test_a_test_name_becomes_a_sentence():
    assert load_generator().sentence("test_an_unreachable_agent_is_not_dead_lettered") == (
        "An unreachable agent is not dead lettered"
    )


def test_tests_are_grouped_by_app_and_capability(tmp_path):
    scrape = tmp_path / "collector" / "scrape_agents"
    scrape.mkdir(parents=True)
    (scrape / "test_one.py").write_text("def test_first_rule():\n    pass\n\n\nclass TestGroup:\n    def test_second_rule(self):\n        pass\n")
    (tmp_path / "test_top.py").write_text("async def test_whole_thing_works():\n    pass\n")
    (tmp_path / "support").mkdir()
    (tmp_path / "support" / "test_ignored.py").write_text("def test_ignored():\n    pass\n")

    catalogue = load_generator().render(tmp_path)

    assert "## collector\n\n### scrape agents\n\n- First rule ([test_one.py:1](../" in catalogue
    assert "- Second rule ([test_one.py:6]" in catalogue
    assert catalogue.index("## whole system") > catalogue.index("## collector")
    assert "Whole thing works" in catalogue
    assert "Ignored" not in catalogue
