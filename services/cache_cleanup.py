from __future__ import annotations

import asyncio
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from config import (
    CACHE_CLEANUP_ENABLED,
    CACHE_CLEANUP_INTERVAL_SECONDS,
    CACHE_CLEANUP_MIN_ENTRY_AGE_MINUTES,
    DATA_DIR,
    EPUB_CACHE_DIR,
    EPUB_CACHE_MAX_AGE_HOURS,
    EPUB_CACHE_MAX_MB,
    IMAGE_CACHE_ORIGINAL_MAX_AGE_HOURS,
    IMAGE_CACHE_ORIGINAL_MAX_MB,
    IMAGE_CACHE_TELEGRAPH_MAX_AGE_HOURS,
    IMAGE_CACHE_TELEGRAPH_MAX_MB,
    PDF_CACHE_DIR,
    PDF_CACHE_MAX_AGE_HOURS,
    PDF_CACHE_MAX_MB,
)


BYTES_PER_MB = 1024 * 1024
_cleanup_task: asyncio.Task | None = None


@dataclass(frozen=True)
class CacheTarget:
    name: str
    path: Path
    max_mb: int
    max_age_hours: int


@dataclass
class CacheEntry:
    path: Path
    size: int
    mtime: float


def _cache_targets() -> list[CacheTarget]:
    image_root = Path(DATA_DIR) / "image_cache"
    return [
        CacheTarget(
            name="imagens_originais",
            path=image_root / "original",
            max_mb=IMAGE_CACHE_ORIGINAL_MAX_MB,
            max_age_hours=IMAGE_CACHE_ORIGINAL_MAX_AGE_HOURS,
        ),
        CacheTarget(
            name="imagens_processadas",
            path=image_root / "telegraph",
            max_mb=IMAGE_CACHE_TELEGRAPH_MAX_MB,
            max_age_hours=IMAGE_CACHE_TELEGRAPH_MAX_AGE_HOURS,
        ),
        CacheTarget(
            name="pdfs",
            path=Path(PDF_CACHE_DIR),
            max_mb=PDF_CACHE_MAX_MB,
            max_age_hours=PDF_CACHE_MAX_AGE_HOURS,
        ),
        CacheTarget(
            name="epubs",
            path=Path(EPUB_CACHE_DIR),
            max_mb=EPUB_CACHE_MAX_MB,
            max_age_hours=EPUB_CACHE_MAX_AGE_HOURS,
        ),
    ]


def _is_inside_data(path: Path) -> bool:
    try:
        resolved = path.resolve()
        data_root = Path(DATA_DIR).resolve()
        return resolved == data_root or data_root in resolved.parents
    except Exception:
        return False


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def _dir_size(path: Path) -> int:
    total = 0
    try:
        for child in path.rglob("*"):
            if child.is_file():
                total += _file_size(child)
    except OSError:
        return total
    return total


def _latest_mtime(path: Path) -> float:
    latest = 0.0
    try:
        latest = path.stat().st_mtime
    except OSError:
        return latest

    if path.is_file():
        return latest

    try:
        for child in path.rglob("*"):
            try:
                latest = max(latest, child.stat().st_mtime)
            except OSError:
                continue
    except OSError:
        pass
    return latest


def _entry_size(path: Path) -> int:
    if path.is_file():
        return _file_size(path)
    if path.is_dir():
        return _dir_size(path)
    return 0


def _iter_entries(root: Path) -> list[CacheEntry]:
    if not root.exists():
        return []

    entries: list[CacheEntry] = []
    try:
        children = list(root.iterdir())
    except OSError:
        return entries

    for child in children:
        if not _is_inside_data(child):
            continue
        size = _entry_size(child)
        if size <= 0 and not child.is_dir():
            continue
        entries.append(CacheEntry(path=child, size=size, mtime=_latest_mtime(child)))

    return entries


def _remove_entry(path: Path) -> int:
    if not _is_inside_data(path):
        return 0

    size = _entry_size(path)
    try:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
    except FileNotFoundError:
        return size
    except OSError as exc:
        print(f"[CACHE_CLEANUP][SKIP] {path}: {exc!r}")
        return 0

    return size


def _cleanup_target(target: CacheTarget, now: float) -> dict[str, int | str]:
    target.path.mkdir(parents=True, exist_ok=True)

    removed = 0
    freed = 0
    max_age_seconds = max(0, target.max_age_hours) * 3600
    min_entry_age_seconds = max(0, CACHE_CLEANUP_MIN_ENTRY_AGE_MINUTES) * 60

    entries = _iter_entries(target.path)

    if max_age_seconds:
        for entry in entries:
            age = now - entry.mtime
            if age >= max_age_seconds and age >= min_entry_age_seconds:
                freed += _remove_entry(entry.path)
                removed += 1

    entries = _iter_entries(target.path)
    total = sum(entry.size for entry in entries)
    max_bytes = max(0, target.max_mb) * BYTES_PER_MB

    if max_bytes and total > max_bytes:
        for entry in sorted(entries, key=lambda item: item.mtime):
            age = now - entry.mtime
            if age < min_entry_age_seconds:
                continue

            freed_size = _remove_entry(entry.path)
            if freed_size <= 0:
                continue

            total -= freed_size
            freed += freed_size
            removed += 1

            if total <= max_bytes:
                break

    remaining = sum(entry.size for entry in _iter_entries(target.path))
    return {
        "name": target.name,
        "removed": removed,
        "freed": freed,
        "remaining": remaining,
    }


def _cleanup_sync() -> list[dict[str, int | str]]:
    if not CACHE_CLEANUP_ENABLED:
        return []

    now = time.time()
    return [_cleanup_target(target, now) for target in _cache_targets()]


async def cleanup_cache_once() -> list[dict[str, int | str]]:
    results = await asyncio.to_thread(_cleanup_sync)
    if not results:
        return results

    removed = sum(int(item["removed"]) for item in results)
    freed = sum(int(item["freed"]) for item in results)
    remaining = sum(int(item["remaining"]) for item in results)

    if removed or freed:
        print(
            "[CACHE_CLEANUP] "
            f"removidos={removed} "
            f"liberado={freed / BYTES_PER_MB:.1f}MB "
            f"restante={remaining / BYTES_PER_MB:.1f}MB"
        )

    return results


async def cache_cleanup_loop(first_delay: float = 0.0) -> None:
    if not CACHE_CLEANUP_ENABLED:
        return

    interval = max(300, CACHE_CLEANUP_INTERVAL_SECONDS)
    if first_delay > 0:
        await asyncio.sleep(first_delay)

    while True:
        try:
            await cleanup_cache_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print("[CACHE_CLEANUP][ERRO]", repr(exc))

        await asyncio.sleep(interval)


def start_cache_cleanup_loop(first_delay: float = 0.0) -> asyncio.Task | None:
    global _cleanup_task

    if not CACHE_CLEANUP_ENABLED:
        return None

    if _cleanup_task and not _cleanup_task.done():
        return _cleanup_task

    _cleanup_task = asyncio.create_task(cache_cleanup_loop(first_delay=first_delay))
    return _cleanup_task


async def stop_cache_cleanup_loop() -> None:
    global _cleanup_task

    task = _cleanup_task
    if not task:
        return

    _cleanup_task = None
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
