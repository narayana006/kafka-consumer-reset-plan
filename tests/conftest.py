from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


@pytest.fixture
def examples() -> Path:
    return EXAMPLES


def read(name: str) -> str:
    return (EXAMPLES / name).read_text(encoding="utf-8")
