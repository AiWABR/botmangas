from __future__ import annotations

import asyncio
import hashlib
import json
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageChops, ImageOps

from config import DATA_DIR, PROMO_BANNER_URL
from core.http_client import get_http_client

IMAGE_CACHE_DIR = DATA_DIR / "image_cache"
ORIGINAL_CACHE_DIR = IMAGE_CACHE_DIR / "original"
TELEGRAPH_CACHE_DIR = IMAGE_CACHE_DIR / "telegraph"
TELEGRAPH_PIPELINE_VERSION = "v6"

ANALYSIS_MAX_WIDTH = 96
ANALYSIS_QUIET_BAND_MIN = 4
TELEGRAPH_TARGET_WIDTH = 1280
TELEGRAPH_MIN_WIDTH = 1180
TELEGRAPH_MAX_WIDTH = 1280
TELEGRAPH_MAX_UPSCALE = 3.4
TELEGRAPH_TARGET_HEIGHT = 860
TELEGRAPH_MAX_HEIGHT = 960
TELEGRAPH_MIN_SEGMENT_HEIGHT = 240
TELEGRAPH_SIDE_MARGIN = 18
TELEGRAPH_VERTICAL_MARGIN = 22
TELEGRAPH_SEGMENT_OVERLAP = 28
TELEGRAPH_CANVAS_HEIGHT = 960

ORIGINAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
TELEGRAPH_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(12)
_DOWNLOAD_INFLIGHT: dict[str, asyncio.Task] = {}
_TELEGRAPH_ASSET_INFLIGHT: dict[str, asyncio.Task] = {}
_TELEGRAPH_PROCESS_SEMAPHORE = asyncio.Semaphore(4)


def _cache_path(url: str) -> Path:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return ORIGINAL_CACHE_DIR / f"{digest}.bin"


def _telegraph_asset_key(chapter_id: str, images: list[str]) -> str:
    normalized = f"{TELEGRAPH_PIPELINE_VERSION}|{chapter_id}|{'|'.join(images or [])}"
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:24]


def _telegraph_asset_dir(asset_key: str) -> Path:
    return TELEGRAPH_CACHE_DIR / asset_key


def _telegraph_manifest_path(asset_key: str) -> Path:
    return _telegraph_asset_dir(asset_key) / "manifest.json"


async def _download_bytes(url: str) -> bytes:
    cache_path = _cache_path(url)
    if cache_path.exists():
        return cache_path.read_bytes()

    task = _DOWNLOAD_INFLIGHT.get(url)
    if task:
        return await task

    async def _runner() -> bytes:
        client = await get_http_client()
        async with _DOWNLOAD_SEMAPHORE:
            response = await client.get(url)
        response.raise_for_status()
        content = response.content
        cache_path.write_bytes(content)
        return content

    task = asyncio.create_task(_runner())
    _DOWNLOAD_INFLIGHT[url] = task
    try:
        return await task
    finally:
        _DOWNLOAD_INFLIGHT.pop(url, None)


def _load_image(content: bytes) -> Image.Image:
    image = Image.open(BytesIO(content))
    image = ImageOps.exif_transpose(image)
    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")
    elif image.mode == "L":
        image = image.convert("RGB")
    return image


def _resize_to_max_width(image: Image.Image, max_width: int) -> Image.Image:
    if image.width <= max_width:
        return image
    ratio = max_width / float(image.width)
    return image.resize((max_width, max(1, int(image.height * ratio))), Image.Resampling.LANCZOS)


def _prepare_pdf_image(content: bytes) -> Image.Image:
    image = _load_image(content)
    return _resize_to_max_width(image, 2200)


def _fit_width(image: Image.Image, *, target_width: int = TELEGRAPH_TARGET_WIDTH) -> Image.Image:
    width = image.width
    if width <= 0:
        return image

    desired_width = width
    if width < TELEGRAPH_MIN_WIDTH:
        desired_width = int(min(target_width, width * TELEGRAPH_MAX_UPSCALE))
    elif width > TELEGRAPH_MAX_WIDTH:
        desired_width = TELEGRAPH_MAX_WIDTH

    if desired_width == width:
        return image

    ratio = desired_width / float(width)
    desired_height = max(1, int(image.height * ratio))
    return image.resize((desired_width, desired_height), Image.Resampling.LANCZOS)


def _analysis_image(image: Image.Image) -> Image.Image:
    width = max(1, min(ANALYSIS_MAX_WIDTH, image.width))
    height = max(1, int(image.height * (width / float(max(image.width, 1)))))
    return image.convert("L").resize((width, height), Image.Resampling.BILINEAR)


def _estimate_background_color(image: Image.Image) -> tuple[int, int, int]:
    sample = max(6, min(24, min(image.width, image.height) // 10))
    boxes = [
        (0, 0, sample, sample),
        (max(0, image.width - sample), 0, image.width, sample),
        (0, max(0, image.height - sample), sample, image.height),
        (max(0, image.width - sample), max(0, image.height - sample), image.width, image.height),
    ]

    values: list[tuple[int, int, int]] = []
    for box in boxes:
        reduced = image.crop(box).resize((1, 1), Image.Resampling.BOX)
        values.append(tuple(int(channel) for channel in reduced.getpixel((0, 0))))

    channels = list(zip(*values))
    return tuple(int(sum(channel_values) / max(1, len(channel_values))) for channel_values in channels)


def _background_bbox(image: Image.Image, threshold: int = 16) -> tuple[int, int, int, int] | None:
    background = _estimate_background_color(image)
    matte = Image.new("RGB", image.size, background)
    diff = ImageChops.difference(image, matte).convert("L")
    mask = diff.point(lambda value: 255 if value >= threshold else 0)
    return mask.getbbox()


def _line_profile(gray: Image.Image) -> list[tuple[float, float, float]]:
    width, height = gray.size
    data = gray.tobytes()
    previous: bytes | None = None
    profile: list[tuple[float, float, float]] = []

    for index in range(height):
        row = data[index * width : (index + 1) * width]
        mean = sum(row) / max(width, 1)
        spread = sum(abs(pixel - mean) for pixel in row) / max(width, 1)
        delta = 0.0
        if previous is not None:
            delta = sum(abs(pixel - previous[pos]) for pos, pixel in enumerate(row)) / max(width, 1)
        profile.append((mean, spread, delta))
        previous = row

    return profile


def _is_quiet_band(mean: float, spread: float, delta: float) -> bool:
    return spread < 9.0 and delta < 7.0 and (mean < 18.0 or mean > 237.0)


def _find_quiet_spans(profile: list[tuple[float, float, float]]) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    start: int | None = None

    for index, (mean, spread, delta) in enumerate(profile):
        if _is_quiet_band(mean, spread, delta):
            if start is None:
                start = index
            continue
        if start is not None:
            if index - start >= ANALYSIS_QUIET_BAND_MIN:
                spans.append((start, index - 1))
            start = None

    if start is not None:
        end = len(profile) - 1
        if end - start + 1 >= ANALYSIS_QUIET_BAND_MIN:
            spans.append((start, end))
    return spans


def _trim_vertical_bounds(image: Image.Image) -> Image.Image:
    gray = _analysis_image(image)
    profile = _line_profile(gray)
    if not profile:
        return image

    quiet_spans = _find_quiet_spans(profile)
    first_content = 0
    last_content = len(profile) - 1
    if quiet_spans and quiet_spans[0][0] == 0:
        first_content = quiet_spans[0][1] + 1
    if quiet_spans and quiet_spans[-1][1] == len(profile) - 1:
        last_content = quiet_spans[-1][0] - 1
    if last_content <= first_content:
        return image

    scale = image.height / float(max(gray.height, 1))
    top = max(0, int(first_content * scale) - TELEGRAPH_VERTICAL_MARGIN)
    bottom = min(image.height, int((last_content + 1) * scale) + TELEGRAPH_VERTICAL_MARGIN)
    if bottom - top < TELEGRAPH_MIN_SEGMENT_HEIGHT:
        return image
    return image.crop((0, top, image.width, bottom))


def _trim_horizontal_bounds(image: Image.Image) -> Image.Image:
    bbox = _background_bbox(image)
    if bbox:
        left = max(0, bbox[0] - TELEGRAPH_SIDE_MARGIN)
        right = min(image.width, bbox[2] + TELEGRAPH_SIDE_MARGIN)
        if right - left >= max(220, image.width // 4) and (right - left) <= int(image.width * 0.94):
            return image.crop((left, 0, right, image.height))

    gray = _analysis_image(image).transpose(Image.Transpose.ROTATE_90)
    profile = _line_profile(gray)
    if not profile:
        return image

    quiet_spans = _find_quiet_spans(profile)
    first_content = 0
    last_content = len(profile) - 1
    if quiet_spans and quiet_spans[0][0] == 0:
        first_content = quiet_spans[0][1] + 1
    if quiet_spans and quiet_spans[-1][1] == len(profile) - 1:
        last_content = quiet_spans[-1][0] - 1
    if last_content <= first_content:
        return image

    scale = image.width / float(max(gray.height, 1))
    left = max(0, int(first_content * scale) - TELEGRAPH_SIDE_MARGIN)
    right = min(image.width, int((last_content + 1) * scale) + TELEGRAPH_SIDE_MARGIN)
    if right - left < max(280, image.width // 3):
        return image
    return image.crop((left, 0, right, image.height))


def _split_by_content(image: Image.Image) -> list[Image.Image]:
    trimmed = _trim_vertical_bounds(image)
    gray = _analysis_image(trimmed)
    profile = _line_profile(gray)
    quiet_spans = _find_quiet_spans(profile)
    if not quiet_spans:
        return [trimmed]

    blocks: list[tuple[int, int]] = []
    cursor = 0
    for quiet_start, quiet_end in quiet_spans:
        if quiet_start > cursor:
            blocks.append((cursor, quiet_start - 1))
        cursor = quiet_end + 1
    if cursor < len(profile):
        blocks.append((cursor, len(profile) - 1))

    if not blocks:
        return [trimmed]

    scale = trimmed.height / float(max(gray.height, 1))
    pixel_blocks = [
        (
            max(0, int(start * scale) - TELEGRAPH_VERTICAL_MARGIN),
            min(trimmed.height, int((end + 1) * scale) + TELEGRAPH_VERTICAL_MARGIN),
        )
        for start, end in blocks
    ]

    segments: list[Image.Image] = []
    current_top, current_bottom = pixel_blocks[0]

    for block_top, block_bottom in pixel_blocks[1:]:
        proposed_height = block_bottom - current_top
        gap = block_top - current_bottom
        if proposed_height <= TELEGRAPH_MAX_HEIGHT and (
            current_bottom - current_top < TELEGRAPH_TARGET_HEIGHT or gap <= 80
        ):
            current_bottom = block_bottom
            continue

        segment = trimmed.crop((0, current_top, trimmed.width, current_bottom))
        if segment.height >= TELEGRAPH_MIN_SEGMENT_HEIGHT:
            segments.append(segment)
        current_top = max(0, block_top - TELEGRAPH_SEGMENT_OVERLAP)
        current_bottom = block_bottom

    final_segment = trimmed.crop((0, current_top, trimmed.width, current_bottom))
    if final_segment.height >= TELEGRAPH_MIN_SEGMENT_HEIGHT:
        segments.append(final_segment)

    return segments or [trimmed]


def _split_tall_segment(segment: Image.Image) -> list[Image.Image]:
    if segment.height <= TELEGRAPH_MAX_HEIGHT:
        return [segment]

    parts: list[Image.Image] = []
    top = 0
    while top < segment.height:
        bottom = min(top + TELEGRAPH_TARGET_HEIGHT, segment.height)
        if segment.height - bottom < TELEGRAPH_MIN_SEGMENT_HEIGHT and parts:
            bottom = segment.height
        parts.append(segment.crop((0, top, segment.width, bottom)))
        top = bottom
    return parts


def _normalize_segment_canvas(image: Image.Image) -> Image.Image:
    if image.width != TELEGRAPH_TARGET_WIDTH:
        image = _fit_width(image)

    if image.height > TELEGRAPH_CANVAS_HEIGHT:
        image = image.crop((0, 0, image.width, TELEGRAPH_CANVAS_HEIGHT))

    if image.width == TELEGRAPH_TARGET_WIDTH and image.height == TELEGRAPH_CANVAS_HEIGHT:
        return image

    background = _estimate_background_color(image)
    canvas = Image.new("RGB", (TELEGRAPH_TARGET_WIDTH, TELEGRAPH_CANVAS_HEIGHT), background)
    x = max(0, (TELEGRAPH_TARGET_WIDTH - image.width) // 2)
    y = 0 if image.height >= int(TELEGRAPH_CANVAS_HEIGHT * 0.82) else max(0, (TELEGRAPH_CANVAS_HEIGHT - image.height) // 2)
    canvas.paste(image, (x, y))
    return canvas


def _prepare_telegraph_banner(content: bytes) -> list[Image.Image]:
    image = _resize_to_max_width(_load_image(content), TELEGRAPH_MAX_WIDTH)
    return [image]


def _prepare_telegraph_segments(content: bytes) -> list[Image.Image]:
    image = _resize_to_max_width(_load_image(content), 1800)
    raw_segments = _split_by_content(image)

    processed: list[Image.Image] = []
    for raw_segment in raw_segments:
        initial_crop = _trim_horizontal_bounds(raw_segment)
        fitted = _fit_width(initial_crop)
        for part in _split_tall_segment(fitted):
            processed.append(_normalize_segment_canvas(part))

    return processed or [_normalize_segment_canvas(_fit_width(image))]


def _encode_jpeg(image: Image.Image, *, quality: int = 80, max_bytes: int = 4_700_000) -> bytes:
    current_quality = quality
    while current_quality >= 70:
        buffer = BytesIO()
        image.save(buffer, format="JPEG", quality=current_quality, subsampling=2)
        payload = buffer.getvalue()
        if len(payload) <= max_bytes:
            return payload
        current_quality -= 8
    return payload


def _build_telegraph_payloads_from_content(content: bytes, *, is_banner: bool) -> list[bytes]:
    segments = _prepare_telegraph_banner(content) if is_banner else _prepare_telegraph_segments(content)
    return [_encode_jpeg(segment) for segment in segments]


def _load_telegraph_manifest(asset_key: str) -> list[str] | None:
    manifest_path = _telegraph_manifest_path(asset_key)
    if not manifest_path.exists():
        return None

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    file_names = [str(item).strip() for item in (payload.get("files") or []) if str(item).strip()]
    if not file_names:
        return None

    asset_dir = _telegraph_asset_dir(asset_key)
    if any(not (asset_dir / file_name).exists() for file_name in file_names):
        return None

    return file_names


def resolve_telegraph_asset_path(asset_key: str, asset_name: str) -> Path:
    safe_key = Path(str(asset_key or "").strip()).name
    safe_name = Path(str(asset_name or "").strip()).name
    if not safe_key or not safe_name or safe_key != asset_key or safe_name != asset_name:
        raise FileNotFoundError("Caminho de asset invalido.")

    asset_path = (_telegraph_asset_dir(safe_key) / safe_name).resolve()
    root = TELEGRAPH_CACHE_DIR.resolve()
    if root not in asset_path.parents or not asset_path.exists():
        raise FileNotFoundError("Asset do Telegraph nao encontrado.")
    return asset_path


async def get_pdf_page_images(images: list[str], progress_cb=None) -> list[Image.Image]:
    urls = [PROMO_BANNER_URL, *(images or [])]
    if not urls:
        return []

    download_tasks = [asyncio.create_task(_download_bytes(url)) for url in urls]
    raw_pages: list[bytes] = []
    total = len(download_tasks)

    for index, task in enumerate(download_tasks, start=1):
        raw_pages.append(await task)
        if progress_cb:
            await progress_cb(index, total)

    return [await asyncio.to_thread(_prepare_pdf_image, page) for page in raw_pages]


async def get_telegraph_image_payloads(images: list[str]) -> list[bytes]:
    urls = [PROMO_BANNER_URL, *(images or [])]
    if not urls:
        return []

    async def _runner(url: str, *, is_banner: bool) -> list[bytes]:
        raw = await _download_bytes(url)
        async with _TELEGRAPH_PROCESS_SEMAPHORE:
            return await asyncio.to_thread(_build_telegraph_payloads_from_content, raw, is_banner=is_banner)

    payload_groups = await asyncio.gather(
        *[
            asyncio.create_task(_runner(url, is_banner=index == 0))
            for index, url in enumerate(urls)
        ]
    )

    payloads: list[bytes] = []
    for group in payload_groups:
        payloads.extend(group)
    return payloads


async def get_telegraph_asset_files(chapter_id: str, images: list[str]) -> tuple[str, list[str]]:
    asset_key = _telegraph_asset_key(chapter_id, images)
    cached = _load_telegraph_manifest(asset_key)
    if cached is not None:
        return asset_key, cached

    task = _TELEGRAPH_ASSET_INFLIGHT.get(asset_key)
    if task:
        file_names = await task
        return asset_key, file_names

    async def _runner() -> list[str]:
        urls = [PROMO_BANNER_URL, *(images or [])]
        if not urls:
            return []

        asset_dir = _telegraph_asset_dir(asset_key)
        asset_dir.mkdir(parents=True, exist_ok=True)

        async def _process_url(url: str, *, is_banner: bool) -> list[bytes]:
            raw = await _download_bytes(url)
            async with _TELEGRAPH_PROCESS_SEMAPHORE:
                return await asyncio.to_thread(_build_telegraph_payloads_from_content, raw, is_banner=is_banner)

        payload_groups = await asyncio.gather(
            *[
                asyncio.create_task(_process_url(url, is_banner=index == 0))
                for index, url in enumerate(urls)
            ]
        )

        file_names: list[str] = []
        file_index = 1
        for payload_group in payload_groups:
            for payload in payload_group:
                file_name = f"{file_index:04d}.jpg"
                await asyncio.to_thread((asset_dir / file_name).write_bytes, payload)
                file_names.append(file_name)
                file_index += 1

        manifest_path = _telegraph_manifest_path(asset_key)
        manifest_payload = {
            "asset_key": asset_key,
            "version": TELEGRAPH_PIPELINE_VERSION,
            "files": file_names,
        }
        await asyncio.to_thread(
            manifest_path.write_text,
            json.dumps(manifest_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return file_names

    task = asyncio.create_task(_runner())
    _TELEGRAPH_ASSET_INFLIGHT[asset_key] = task
    try:
        file_names = await task
        return asset_key, file_names
    finally:
        _TELEGRAPH_ASSET_INFLIGHT.pop(asset_key, None)
