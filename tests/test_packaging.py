import tomllib
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_console_scripts_point_at_importable_callables():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    scripts = pyproject["project"]["scripts"]

    assert set(scripts) == {"activityreporter", "collector", "metrics-writer"}
    for target in scripts.values():
        module_name, func_name = target.split(":")
        module = import_module(module_name)
        assert callable(getattr(module, func_name))


def test_wheel_packages_exist():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    packages = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]

    assert packages == ["src/activityreporter"]
    for package in packages:
        assert (ROOT / package / "__init__.py").is_file()
