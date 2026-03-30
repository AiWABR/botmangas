import asyncio
import hashlib
import re
from pathlib import Path

from PIL import Image

from config import DISTRIBUTION_TAG, PDF_CACHE_DIR, PDF_NAME_PATTERN
from services.media_pipeline import get_pdf_page_images

PDF_CACHE_PATH = Path(PDF_CACHE_DIR)
PDF_CACHE_PATH.mkdir(parents=True, exist_ok=True)


def _safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "", str(value or "").strip())
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Manga"


def _pdf_name(title_name: str, chapter_number: str) -> str:
    base_name = PDF_NAME_PATTERN.format(
        title=_safe_filename(title_name),
        chapter=_safe_filename(chapter_number),
    )
    if DISTRIBUTION_TAG.lower() not in base_name.lower():
        stem = base_name[:-4] if base_name.lower().endswith(".pdf") else base_name
        base_name = f"{stem} - {DISTRIBUTION_TAG}.pdf"
    return base_name


def _pdf_path(chapter_id: str) -> Path:
    safe = hashlib.sha1(chapter_id.encode("utf-8")).hexdigest()
    return PDF_CACHE_PATH / f"{safe}.pdf"


def _save_pdf(pdf_path: Path, images: list[Image.Image]) -> None:
    first = images[0]
    rest = images[1:]
    first.save(pdf_path, save_all=True, append_images=rest)


async def get_or_build_pdf(
    chapter_id: str,
    chapter_number: str,
    title_name: str,
    images: list[str],
    progress_cb=None,
) -> tuple[str, str]:
    pdf_path = _pdf_path(chapter_id)
    pdf_name = _pdf_name(title_name, chapter_number)

    if pdf_path.exists():
        return str(pdf_path), pdf_name

    if not images:
        raise RuntimeError("Nenhuma imagem encontrada para gerar o PDF.")

    pil_pages = await get_pdf_page_images(images, progress_cb=progress_cb)
    await asyncio.to_thread(_save_pdf, pdf_path, pil_pages)
    return str(pdf_path), pdf_name
