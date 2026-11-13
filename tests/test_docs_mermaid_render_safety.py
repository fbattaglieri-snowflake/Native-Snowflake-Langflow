"""Guard the Mermaid diagrams in the public docs against syntax that GitHub cannot render."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = sorted([REPO_ROOT / "README.md", *(REPO_ROOT / "docs").rglob("*.md")])
MERMAID_BLOCK = re.compile(r"```mermaid\n(.*?)```", re.S)
SEQUENCE_MESSAGE = re.compile(r"^\s*[\w ]+?\s*(?:--?>>?|--?[x)])\s*[\w ]+?\s*:\s*(.*)$")


def _blocks() -> list[tuple[str, int, str]]:
    found = []
    for path in DOCS:
        for index, match in enumerate(MERMAID_BLOCK.finditer(path.read_text(encoding="utf-8")), 1):
            found.append((str(path.relative_to(REPO_ROOT)), index, match.group(1)))
    return found


def test_docs_contain_mermaid_diagrams() -> None:
    assert len(_blocks()) >= 9


@pytest.mark.parametrize(("doc", "index", "block"), _blocks(), ids=lambda v: str(v)[:40])
def test_mermaid_block_is_render_safe(doc: str, index: int, block: str) -> None:
    where = f"{doc} diagram #{index}"
    # `<NAME>` is parsed as an HTML tag and silently dropped from the rendered label.
    assert not re.search(r"<[A-Z][A-Z_]*>", block), f"{where}: angle-bracket placeholder"
    # A literal backslash-n is printed as text; use <br/>.
    assert "\\n" not in block, f"{where}: literal \\n in a label"
    # A label starting with `1. ` is rendered as `Unsupported markdown: list`.
    assert not re.search(r"""[\[(]["']\s*\d+\.\s""", block), f"{where}: label starts with a numbered list"
    if block.lstrip().startswith("sequenceDiagram"):
        for line in block.splitlines():
            message = SEQUENCE_MESSAGE.match(line)
            # `;` ends a statement in a sequence diagram and breaks the parser.
            assert not (message and ";" in message.group(1)), f"{where}: `;` in message: {line.strip()}"
