"""
Közös kulcs-betöltő: a `keys_urls.json` a repository gyökerében van,
és abszolút path-on kell elérni — nem cwd-relatív, mert a script futtatási
helyétől nem szabad függjön.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parent.parent


def keys_path() -> Path:
    return REPO_ROOT / "keys_urls.json"


@lru_cache(maxsize=1)
def load_keys() -> dict:
    p = keys_path()
    if not p.exists():
        raise FileNotFoundError(
            f"keys_urls.json nem található itt: {p}. "
            "Hozd létre vagy állítsd vissza a repo gyökerébe."
        )
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def get(name: str, default: Optional[str] = None) -> Optional[str]:
    return load_keys().get(name, default)
