from __future__ import annotations

import asyncio
import hashlib
import html
import re
import uuid
import zipfile
from pathlib import Path

from config import DISTRIBUTION_TAG, EPUB_CACHE_DIR, EPUB_NAME_PATTERN
from services.media_pipeline import get_telegraph_asset_files, resolve_telegraph_asset_path

EPUB_CACHE_PATH = Path(EPUB_CACHE_DIR)
EPUB_CACHE_PATH.mkdir(parents=True, exist_ok=True)


def _safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*]+', "", str(value or "").strip())
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Manga"


def _epub_name(title_name: str, chapter_number: str) -> str:
    base_name = EPUB_NAME_PATTERN.format(
        title=_safe_filename(title_name),
        chapter=_safe_filename(chapter_number),
    )
    if not base_name.lower().endswith(".epub"):
        base_name = f"{base_name}.epub"
    if DISTRIBUTION_TAG.lower() not in base_name.lower():
        stem = base_name[:-5]
        base_name = f"{stem} - {DISTRIBUTION_TAG}.epub"
    return base_name


def _epub_path(chapter_id: str) -> Path:
    safe = hashlib.sha1(chapter_id.encode("utf-8")).hexdigest()
    return EPUB_CACHE_PATH / f"{safe}.epub"


def _zip_info(name: str, *, compress_type: int = zipfile.ZIP_DEFLATED) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name)
    info.compress_type = compress_type
    return info


def _page_xhtml(title: str, page_number: int, image_name: str) -> str:
    safe_title = html.escape(title)
    return f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>{safe_title} - {page_number}</title>
  <style>
    body {{ margin: 0; padding: 0; background: #111; text-align: center; }}
    img {{ display: block; width: 100%; height: auto; margin: 0 auto; }}
  </style>
</head>
<body>
  <img src="../images/{image_name}" alt="Pagina {page_number}" />
</body>
</html>"""


def _nav_xhtml(title: str, page_count: int) -> str:
    safe_title = html.escape(title)
    links = "\n".join(
        f'    <li><a href="pages/page_{index:04d}.xhtml">Pagina {index}</a></li>'
        for index in range(1, page_count + 1)
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>{safe_title}</title></head>
<body>
  <nav epub:type="toc" id="toc">
    <h1>{safe_title}</h1>
    <ol>
{links}
    </ol>
  </nav>
</body>
</html>"""


def _content_opf(title: str, identifier: str, page_count: int) -> str:
    safe_title = html.escape(title)
    image_items = "\n".join(
        f'    <item id="img{index}" href="images/{index:04d}.jpg" media-type="image/jpeg" />'
        for index in range(1, page_count + 1)
    )
    page_items = "\n".join(
        f'    <item id="page{index}" href="pages/page_{index:04d}.xhtml" media-type="application/xhtml+xml" />'
        for index in range(1, page_count + 1)
    )
    spine_items = "\n".join(
        f'    <itemref idref="page{index}" />'
        for index in range(1, page_count + 1)
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" unique-identifier="bookid" version="3.0">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="bookid">urn:uuid:{identifier}</dc:identifier>
    <dc:title>{safe_title}</dc:title>
    <dc:language>pt-BR</dc:language>
  </metadata>
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav" />
{page_items}
{image_items}
  </manifest>
  <spine>
{spine_items}
  </spine>
</package>"""


def _build_epub(epub_path: Path, title: str, image_paths: list[Path]) -> None:
    identifier = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{title}:{epub_path.name}"))
    page_count = len(image_paths)

    with zipfile.ZipFile(epub_path, "w") as zf:
        zf.writestr(_zip_info("mimetype", compress_type=zipfile.ZIP_STORED), "application/epub+zip")
        zf.writestr(
            _zip_info("META-INF/container.xml"),
            """<?xml version="1.0" encoding="utf-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml" />
  </rootfiles>
</container>""",
        )
        zf.writestr(_zip_info("OEBPS/content.opf"), _content_opf(title, identifier, page_count))
        zf.writestr(_zip_info("OEBPS/nav.xhtml"), _nav_xhtml(title, page_count))

        for index, image_path in enumerate(image_paths, start=1):
            image_name = f"{index:04d}.jpg"
            zf.write(image_path, f"OEBPS/images/{image_name}")
            zf.writestr(
                _zip_info(f"OEBPS/pages/page_{index:04d}.xhtml"),
                _page_xhtml(title, index, image_name),
            )


async def get_or_build_epub(
    chapter_id: str,
    chapter_number: str,
    title_name: str,
    images: list[str],
    progress_cb=None,
) -> tuple[str, str]:
    epub_path = _epub_path(chapter_id)
    epub_name = _epub_name(title_name, chapter_number)

    if epub_path.exists():
        return str(epub_path), epub_name

    if not images:
        raise RuntimeError("Nenhuma imagem encontrada para gerar o EPUB.")

    asset_key, file_names = await get_telegraph_asset_files(chapter_id, images)
    image_paths = [resolve_telegraph_asset_path(asset_key, file_name) for file_name in file_names]
    if not image_paths:
        raise RuntimeError("Nenhuma pagina ficou disponivel para gerar o EPUB.")

    if progress_cb:
        await progress_cb(1, 2)
    await asyncio.to_thread(_build_epub, epub_path, f"{title_name} - Capitulo {chapter_number}", image_paths)
    if progress_cb:
        await progress_cb(2, 2)
    return str(epub_path), epub_name
