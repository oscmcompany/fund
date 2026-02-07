"""Shared test fixtures and helpers."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MASKFILE = REPO_ROOT / "maskfile.md"


def read_maskfile() -> str:
    """Read the maskfile.md content."""
    return MASKFILE.read_text()


def extract_command(section: str) -> str:
    """Extract a named bash block from maskfile."""
    content = read_maskfile()
    pattern = rf"#### {re.escape(section)}\n.*?```bash\n(.*?)```"
    match = re.search(pattern, content, re.DOTALL)
    assert match is not None, f"Could not find '{section}' command in maskfile"
    return match.group(1)
