"""Generate docs/behaviour.md from the names of the tests under tests/.

Usage: python scripts/behaviour_catalogue.py [--check]
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WHOLE_SYSTEM = "whole system"

INTRO = (
    "This catalogue is generated from the test names by `make docs`; do not edit it by hand. "
    "Each line is a rule the test suite proves, grouped by application and capability."
)


def sentence(test_name: str) -> str:
    text = test_name.removeprefix("test_").replace("_", " ")
    return text[:1].upper() + text[1:]


def tests_in(path: Path) -> list[tuple[str, int]]:
    """Test names and line numbers in file order, including methods of Test* classes."""
    found = []
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            found += [m for m in node.body if isinstance(m, ast.FunctionDef | ast.AsyncFunctionDef)]
        else:
            found.append(node)
    return [
        (n.name, n.lineno)
        for n in found
        if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef) and n.name.startswith("test_")
    ]


def section_of(relative: Path) -> tuple[str, str]:
    """(application, capability) of a test file path relative to tests/; capability is empty for the app root."""
    parts = relative.parts
    if len(parts) == 1:
        return WHOLE_SYSTEM, ""
    capability = parts[1].replace("_", " ") if len(parts) > 2 else ""
    return parts[0], capability


def collect(tests_dir: Path) -> dict[tuple[str, str], dict[str, list[str]]]:
    """Bullets per test file (named without `test_`), per (application, capability)."""
    sections: dict[tuple[str, str], dict[str, list[str]]] = {}
    paths = [p for p in tests_dir.rglob("test_*.py") if "support" not in p.relative_to(tests_dir).parts]
    for path in sorted(paths, key=lambda p: p.relative_to(tests_dir).parts):
        relative = path.relative_to(tests_dir)
        link = f"../{tests_dir.name}/{relative.as_posix()}"
        subject = path.stem.removeprefix("test_").replace("_", " ")
        files = sections.setdefault(section_of(relative), {})
        for name, line in tests_in(path):
            files.setdefault(subject, []).append(f"- {sentence(name)} ([{path.name}:{line}]({link}#L{line}))")
    return sections


def render(tests_dir: Path) -> str:
    sections = collect(tests_dir)
    apps = sorted({app for app, _ in sections}, key=lambda a: (a == WHOLE_SYSTEM, a))
    lines = ["# Behaviour catalogue", "", INTRO]
    for app in apps:
        lines += ["", f"## {app}"]
        for (section_app, capability), files in sorted(sections.items()):
            if section_app != app:
                continue
            if capability:
                lines += ["", f"### {capability}"]
            lines += _file_blocks(files)
    return "\n".join(lines) + "\n"


def _file_blocks(files: dict[str, list[str]]) -> list[str]:
    """The bullets of each file; under a `####` heading per file when the section has more than one."""
    if len(files) == 1:
        return [""] + next(iter(files.values()))
    lines: list[str] = []
    for subject, bullets in files.items():
        lines += ["", f"#### {subject}", ""] + bullets
    return lines


def main(argv: list[str]) -> int:
    target = ROOT / "docs" / "behaviour.md"
    content = render(ROOT / "tests")
    if "--check" in argv:
        if not target.exists() or target.read_text() != content:
            print("docs/behaviour.md is out of date; run `make docs`.")
            return 1
        return 0
    target.write_text(content)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
