from __future__ import annotations

import math
import random
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFilter, ImageFont

CARD_SIZE = (1600, 900)


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
    resized = image.resize((math.ceil(src_w * scale), math.ceil(src_h * scale)), Image.Resampling.LANCZOS)
    left = max(0, (resized.width - dst_w) // 2)
    top = max(0, (resized.height - dst_h) // 2)
    return resized.crop((left, top, left + dst_w, top + dst_h))


def _avatar_image(avatar_bytes: bytes | None, size: int, initial: str) -> Image.Image:
    if avatar_bytes:
        try:
            avatar = Image.open(BytesIO(avatar_bytes))
            return _fit_cover(avatar, (size, size))
        except Exception:
            pass

    avatar = Image.new("RGBA", (size, size), (16, 16, 20, 255))
    draw = ImageDraw.Draw(avatar)
    for y in range(size):
        ratio = y / max(1, size - 1)
        red = int(180 - 70 * ratio)
        draw.line((0, y, size, y), fill=(red, 10, 18, 255))
    font = _font(max(36, size // 3), bold=True)
    letter = (initial or "?")[:1].upper()
    bbox = draw.textbbox((0, 0), letter, font=font)
    draw.text(
        ((size - (bbox[2] - bbox[0])) / 2, (size - (bbox[3] - bbox[1])) / 2 - 10),
        letter,
        font=font,
        fill=(255, 255, 255, 255),
    )
    return avatar


def _paste_circle(base: Image.Image, image: Image.Image, xy: tuple[int, int], size: int) -> None:
    mask = Image.new("L", (size, size), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    base.paste(image, xy, mask)


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def _truncate(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    text = str(text or "").strip()
    if _text_width(draw, text, font) <= max_width:
        return text
    suffix = "..."
    while text and _text_width(draw, text + suffix, font) > max_width:
        text = text[:-1].rstrip()
    return (text + suffix) if text else suffix


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    label: str,
    value: str,
    fill: tuple[int, int, int, int],
    accent: tuple[int, int, int, int],
) -> None:
    x, y = xy
    w, h = 330, 86
    draw.rounded_rectangle((x, y, x + w, y + h), radius=28, fill=fill, outline=(255, 255, 255, 36), width=1)
    draw.rounded_rectangle((x, y, x + 7, y + h), radius=4, fill=accent)
    draw.text((x + 30, y + 13), value, font=_font(34, bold=True), fill=(255, 255, 255, 255))
    draw.text((x + 30, y + 53), label.upper(), font=_font(17, bold=True), fill=(210, 214, 224, 210))


def _draw_recent_reads(
    draw: ImageDraw.ImageDraw,
    items: list[dict[str, Any]],
    x: int,
    y: int,
) -> None:
    title_font = _font(31, bold=True)
    row_font = _font(24, bold=True)
    sub_font = _font(19)
    draw.text((x, y), "ULTIMOS CAPS LIDOS", font=title_font, fill=(255, 255, 255, 245))
    draw.line((x, y + 45, x + 520, y + 45), fill=(220, 16, 24, 180), width=4)

    if not items:
        draw.text((x, y + 78), "Nenhum capitulo lido ainda.", font=row_font, fill=(220, 226, 235, 210))
        return

    row_y = y + 78
    for index, item in enumerate(items[:4], start=1):
        title = item.get("title_name") or item.get("title") or "Manga"
        chapter = item.get("chapter_number") or "?"
        title = _truncate(draw, str(title), row_font, 590)
        draw.rounded_rectangle((x, row_y, x + 650, row_y + 72), radius=18, fill=(255, 255, 255, 18))
        draw.ellipse((x + 18, row_y + 18, x + 54, row_y + 54), fill=(210, 0, 20, 235))
        draw.text((x + 29, row_y + 20), str(index), font=_font(18, bold=True), fill=(255, 255, 255, 255))
        draw.text((x + 72, row_y + 11), title, font=row_font, fill=(255, 255, 255, 242))
        draw.text((x + 72, row_y + 43), f"Capitulo {chapter}", font=sub_font, fill=(235, 83, 83, 238))
        row_y += 86


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
    width, height = CARD_SIZE
    seed = str(user_id or name or "perfil")
    rng = random.Random(seed)

    base = Image.new("RGBA", CARD_SIZE, (5, 5, 7, 255))
    draw = ImageDraw.Draw(base, "RGBA")

    for y in range(height):
        shade = int(8 + 22 * (y / height))
        draw.line((0, y, width, y), fill=(shade, 4, 7, 255))

    bg = Image.new("RGBA", CARD_SIZE, (0, 0, 0, 0))
    bg_draw = ImageDraw.Draw(bg, "RGBA")
    for _ in range(90):
        x = rng.randint(-80, width + 80)
        y = rng.randint(-60, height + 60)
        length = rng.randint(80, 420)
        angle = rng.uniform(-0.35, 0.35)
        x2 = int(x + length * math.cos(angle))
        y2 = int(y + length * math.sin(angle))
        color = (180 + rng.randint(0, 60), rng.randint(0, 25), rng.randint(0, 18), rng.randint(22, 90))
        bg_draw.line((x, y, x2, y2), fill=color, width=rng.randint(2, 8))

    for _ in range(240):
        x = rng.randint(0, width)
        y = rng.randint(0, height)
        r = rng.randint(1, 4)
        bg_draw.ellipse((x - r, y - r, x + r, y + r), fill=(235, 18, 26, rng.randint(40, 150)))

    for _ in range(22):
        x = rng.randint(0, width)
        y = rng.randint(0, height)
        w = rng.randint(140, 260)
        h = rng.randint(90, 180)
        bg_draw.rectangle((x, y, x + w, y + h), outline=(255, 255, 255, 18), width=2)

    base.alpha_composite(bg.filter(ImageFilter.GaussianBlur(0.3)))

    draw = ImageDraw.Draw(base, "RGBA")
    draw.rectangle((0, 0, width, height), fill=(0, 0, 0, 54))
    draw.ellipse((1040, -250, 1760, 470), fill=(165, 0, 16, 95))
    draw.ellipse((-190, 180, 660, 1030), outline=(235, 12, 26, 190), width=8)
    draw.ellipse((-170, 200, 640, 1010), outline=(255, 50, 60, 75), width=3)

    avatar_size = 330
    avatar_xy = (155, 235)
    avatar = _avatar_image(avatar_bytes, avatar_size, (name or username or "?")[:1])
    ring_pad = 18
    ring_box = (
        avatar_xy[0] - ring_pad,
        avatar_xy[1] - ring_pad,
        avatar_xy[0] + avatar_size + ring_pad,
        avatar_xy[1] + avatar_size + ring_pad,
    )
    draw.ellipse(ring_box, fill=(0, 0, 0, 220), outline=(238, 16, 28, 255), width=10)
    draw.ellipse(
        (ring_box[0] - 9, ring_box[1] - 9, ring_box[2] + 9, ring_box[3] + 9),
        outline=(238, 16, 28, 80),
        width=4,
    )
    _paste_circle(base, avatar, avatar_xy, avatar_size)

    title_font = _font(106, bold=True)
    title2_font = _font(128, bold=True)
    brand_font = _font(42, bold=True)
    small_font = _font(25, bold=True)
    name_font = _font(40, bold=True)

    draw.text((625, 215), "SEU", font=title_font, fill=(248, 248, 250, 255))
    draw.text((625, 318), "PERFIL", font=title2_font, fill=(225, 4, 22, 255))
    draw.rounded_rectangle((622, 497, 1078, 572), radius=6, fill=(180, 10, 18, 230))
    draw.text((660, 510), brand.upper(), font=brand_font, fill=(255, 255, 255, 245))

    public_name = (name or "Usuario").strip()
    user_label = f"@{username}" if username else f"ID {user_id}"
    draw.text((130, 620), _truncate(draw, public_name, name_font, 430), font=name_font, fill=(255, 255, 255, 245))
    draw.text((132, 670), user_label, font=small_font, fill=(225, 70, 78, 240))

    _draw_pill(draw, (130, 750), "Acompanhando", str(following_count), (255, 255, 255, 22), (225, 8, 20, 255))
    _draw_pill(draw, (486, 750), "Favoritos", str(favorites_count), (255, 255, 255, 22), (255, 198, 62, 255))
    _draw_pill(draw, (842, 750), "Caps lidos", str(chapters_read_count), (255, 255, 255, 22), (64, 156, 255, 255))

    _draw_recent_reads(draw, recent_reads or [], 900, 122)

    draw.line((620, 604, 1390, 604), fill=(255, 255, 255, 30), width=2)
    draw.text((626, 626), "O melhor destino para os melhores mangas", font=_font(28, bold=True), fill=(255, 255, 255, 210))

    out = BytesIO()
    out.name = "perfil.png"
    base.convert("RGB").save(out, "PNG", optimize=True)
    out.seek(0)
    return out
