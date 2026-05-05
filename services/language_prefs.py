from __future__ import annotations

import json
import time
from threading import Lock
from typing import Any

from config import DATA_DIR, PREFERRED_CHAPTER_LANG

PREFS_PATH = DATA_DIR / "language_preferences.json"

_LOCK = Lock()

LANGUAGE_LABELS: dict[str, str] = {
    "pt-br": "Português BR",
    "pt": "Português",
    "en": "Inglês",
    "es": "Espanhol",
    "es-la": "Espanhol LATAM",
    "fr": "Francês",
    "de": "Alemão",
    "it": "Italiano",
    "ru": "Russo",
    "ja": "Japonês",
    "ko": "Coreano",
    "zh": "Chinês",
    "zh-cn": "Chinês simplificado",
    "zh-tw": "Chinês tradicional",
    "id": "Indonesio",
    "th": "Tailandes",
    "vi": "Vietnamita",
    "tr": "Turco",
    "pl": "Polonês",
}

LANGUAGE_FLAGS: dict[str, str] = {
    "pt-br": "BR",
    "pt": "PT",
    "en": "EN",
    "es": "ES",
    "es-la": "LATAM",
    "fr": "FR",
    "de": "DE",
    "it": "IT",
    "ru": "RU",
    "ja": "JP",
    "ko": "KR",
    "zh": "CN",
    "zh-cn": "CN",
    "zh-tw": "TW",
    "id": "ID",
    "th": "TH",
    "vi": "VI",
    "tr": "TR",
    "pl": "PL",
}


def normalize_language(value: Any) -> str:
    lang = str(value or "").strip().lower().replace("_", "-")
    aliases = {
        "br": "pt-br",
        "ptbr": "pt-br",
        "pt_br": "pt-br",
        "portugues": "pt-br",
        "portuguese": "pt-br",
        "eng": "en",
        "english": "en",
        "espanol": "es",
        "spanish": "es",
    }
    return aliases.get(lang, lang)


def language_label(lang: str) -> str:
    normalized = normalize_language(lang)
    return LANGUAGE_LABELS.get(normalized, normalized.upper() if normalized else "Padrao")


def language_badge(lang: str) -> str:
    normalized = normalize_language(lang)
    flag = LANGUAGE_FLAGS.get(normalized, normalized.upper())
    label = language_label(normalized)
    return f"{flag} {label}".strip()


def language_option(raw: Any) -> dict[str, str] | None:
    if isinstance(raw, dict):
        code = normalize_language(
            raw.get("code")
            or raw.get("language")
            or raw.get("language_code")
            or raw.get("locale")
            or raw.get("lang")
            or raw.get("slug")
            or raw.get("value")
            or raw.get("id")
        )
        label = str(raw.get("name") or raw.get("label") or "").strip()
    else:
        code = normalize_language(raw)
        label = ""

    if not code:
        return None

    return {
        "code": code,
        "label": label or language_label(code),
        "badge": language_badge(code),
    }


def language_options(raw_languages: list[Any] | None, *, include_default: bool = True) -> list[dict[str, str]]:
    seen: set[str] = set()
    options: list[dict[str, str]] = []

    if include_default:
        default = language_option(PREFERRED_CHAPTER_LANG)
        if default:
            options.append(default)
            seen.add(default["code"])

    for raw in raw_languages or []:
        option = language_option(raw)
        if not option or option["code"] in seen:
            continue
        options.append(option)
        seen.add(option["code"])

    return options


def collect_language_sources(bundle: dict[str, Any] | None) -> list[Any]:
    if not isinstance(bundle, dict):
        return []

    sources: list[Any] = []
    sources.extend(bundle.get("languages") or [])

    for chapter in bundle.get("chapters") or []:
        if not isinstance(chapter, dict):
            continue
        if chapter.get("chapter_language"):
            sources.append(chapter.get("chapter_language"))
        if chapter.get("language"):
            sources.append(chapter.get("language"))
        preferred = chapter.get("preferred_translation")
        if isinstance(preferred, dict):
            sources.append(preferred)
        for translation in chapter.get("translations") or []:
            if isinstance(translation, dict):
                sources.append(translation)

    return sources


def bundle_language_options(bundle: dict[str, Any] | None, *, include_default: bool = True) -> list[dict[str, str]]:
    return language_options(collect_language_sources(bundle), include_default=include_default)


def _load_data() -> dict[str, Any]:
    if not PREFS_PATH.exists():
        return {"users": {}}
    try:
        data = json.loads(PREFS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"users": {}}
    if not isinstance(data, dict):
        return {"users": {}}
    if not isinstance(data.get("users"), dict):
        data["users"] = {}
    return data


def _save_data(data: dict[str, Any]) -> None:
    PREFS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PREFS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_user_language(user_id: int | str | None, fallback: str | None = None) -> str:
    default = normalize_language(fallback or PREFERRED_CHAPTER_LANG) or PREFERRED_CHAPTER_LANG
    key = str(user_id or "").strip()
    if not key:
        return default

    with _LOCK:
        data = _load_data()
        user = (data.get("users") or {}).get(key) or {}
        lang = normalize_language(user.get("chapter_language"))
    return lang or default


def set_user_language(user_id: int | str, lang: str) -> dict[str, Any]:
    key = str(user_id or "").strip()
    normalized = normalize_language(lang)
    if not key:
        raise ValueError("user_id obrigatorio")
    if not normalized:
        raise ValueError("idioma obrigatorio")

    now = int(time.time() * 1000)
    with _LOCK:
        data = _load_data()
        users = data.setdefault("users", {})
        current = users.setdefault(key, {})
        current["chapter_language"] = normalized
        current["updated_at"] = now
        _save_data(data)

    return {
        "user_id": key,
        "chapter_language": normalized,
        "label": language_label(normalized),
        "badge": language_badge(normalized),
        "updated_at": now,
    }
