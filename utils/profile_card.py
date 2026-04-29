from __future__ import annotations

import math
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

CARD_SIZE = (1600, 900)

ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets"
TEMPLATE_PATH = ASSETS_DIR / "perfil_template.png"

# Ajustado para o template atual do Mangás Brasil.
# No arquivo original 2048x1152, o círculo fica centralizado perto de x=380 / y=565.
# Como o bot gera em 1600x900, esses valores encaixam melhor.
AVATAR_SIZE = 390
AVATAR_XY = (102, 246)


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/seguisb.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return ImageFont.truetype(candidate, size=size)

    return ImageFont.load_default()


def _fit_cover(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    image = image.convert("RGBA")

    src_w, src_h = image.size
    dst_w, dst_h = size

    scale = max(dst_w / max(1, src_w), dst_h / max(1, src_h))

    resized = image.resize(
        (math.ceil(src_w * scale), math.ceil(src_h * scale)),
        Image.Resampling.LANCZOS,
    )

    left = max(0, (resized.width - dst_w) // 2)
    top = max(0, (resized.height - dst_h) // 2)

    return resized.crop((left, top, left + dst_w, top + dst_h))


def _circle_mask(size: int) -> Image.Image:
    scale = 4
    big_size = size * scale

    mask = Image.new("L", (big_size, big_size), 0)
    draw = ImageDraw.Draw(mask)

    draw.ellipse(
        (0, 0, big_size - 1, big_size - 1),
        fill=255,
    )

    return mask.resize((size, size), Image.Resampling.LANCZOS)


def _avatar_image(avatar_bytes: bytes | None, size: int, initial: str) -> Image.Image:
    if avatar_bytes:
        try:
            avatar = Image.open(BytesIO(avatar_bytes)).convert("RGBA")
            return _fit_cover(avatar, (size, size))
        except Exception:
            pass

    avatar = Image.new("RGBA", (size, size), (8, 8, 10, 255))
    draw = ImageDraw.Draw(avatar, "RGBA")

    for y in range(size):
        ratio = y / max(1, size - 1)
        draw.line(
            (0, y, size, y),
            fill=(int(165 - 45 * ratio), 8, 18, 255),
        )

    draw.ellipse(
        (14, 14, size - 14, size - 14),
        outline=(255, 45, 55, 120),
        width=5,
    )

    font = _font(max(48, size // 3), bold=True)
    letter = (initial or "?")[:1].upper()

    bbox = draw.textbbox((0, 0), letter, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]

    draw.text(
        ((size - text_w) / 2, (size - text_h) / 2 - 12),
        letter,
        font=font,
        fill=(255, 255, 255, 255),
    )

    return avatar


def _paste_circle(base: Image.Image, image: Image.Image, xy: tuple[int, int], size: int) -> None:
    avatar = _fit_cover(image, (size, size))
    mask = _circle_mask(size)

    shadow = Image.new("RGBA", (size + 24, size + 24), (0, 0, 0, 0))
    shadow_mask = _circle_mask(size)
    shadow.paste((0, 0, 0, 90), (12, 12), shadow_mask)

    base.alpha_composite(shadow, (xy[0] - 12, xy[1] - 8))
    base.paste(avatar, xy, mask)


def _load_template() -> Image.Image:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template do perfil não encontrado: {TEMPLATE_PATH}")

    try:
        template = Image.open(TEMPLATE_PATH).convert("RGBA")
        return _fit_cover(template, CARD_SIZE)
    except Exception as exc:
        raise RuntimeError(f"Não foi possível carregar o template do perfil: {TEMPLATE_PATH}") from exc


def build_profile_card(
    *,
    user_id: int | str,
    name: str,
    username: str = "",
    avatar_bytes: bytes | None = None,
    brand: str = "Mangas Brasil",
    following_count: int = 0,
    favorites_count: int = 0,
    chapters_read_count: int = 0,
    recent_reads: list[dict[str, Any]] | None = None,
) -> BytesIO:
    base = _load_template()

    avatar = _avatar_image(
        avatar_bytes=avatar_bytes,
        size=AVATAR_SIZE,
        initial=(name or username or "?")[:1],
    )

    _paste_circle(base, avatar, AVATAR_XY, AVATAR_SIZE)

    out = BytesIO()
    out.name = "perfil.png"
    base.convert("RGB").save(out, "PNG", optimize=True, quality=95)
    out.seek(0)

    return out
