from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from config import BASE_DIR
from services.language_prefs import (
    DEFAULT_INTERFACE_LOCALE,
    get_user_interface_language,
    normalize_interface_locale,
)

LOCALES_DIR = BASE_DIR / "locales"


@lru_cache(maxsize=8)
def _load_locale(locale: str) -> dict[str, Any]:
    resolved = normalize_interface_locale(locale)
    path = LOCALES_DIR / f"{resolved}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if resolved != DEFAULT_INTERFACE_LOCALE:
            return _load_locale(DEFAULT_INTERFACE_LOCALE)
        return {}
    return data if isinstance(data, dict) else {}


def _lookup(data: dict[str, Any], key: str) -> Any:
    current: Any = data
    for part in key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def t_user(user_id: int | str | None, key: str, **kwargs: Any) -> str:
    return t(get_user_interface_language(user_id), key, **kwargs)


def t(lang: str | None, key: str, **kwargs: Any) -> str:
    resolved = normalize_interface_locale(lang)
    value = _lookup(_load_locale(resolved), key)
    if value is None and resolved != DEFAULT_INTERFACE_LOCALE:
        value = _lookup(_load_locale(DEFAULT_INTERFACE_LOCALE), key)
    if value is None:
        value = key
    text = str(value)
    try:
        return text.format(**kwargs)
    except Exception:
        return text
