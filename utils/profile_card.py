from __future__ import annotations

import math
import random
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFilter, ImageFont

CARD_SIZE = (1600, 900)
ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets"
TEMPLATE_PATH = ASSETS_DIR / "perfil_template.png"
AVATAR_SIZE = 410
AVATAR_XY = (106, 205)


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/impact.ttf" if bold else "",
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
            return _fit_cover(Image.open(BytesIO(avatar_bytes)), (size, size))
        except Exception:
            pass

    avatar = Image.new("RGBA", (size, size), (8, 8, 10, 255))
    draw = ImageDraw.Draw(avatar, "RGBA")
    for y in range(size):
        ratio = y / max(1, size - 1)
        draw.line((0, y, size, y), fill=(int(190 - 60 * ratio), 8, 18, 255))
    draw.ellipse((16, 16, size - 16, size - 16), outline=(255, 55, 65, 90), width=5)
    font = _font(max(48, size // 3), bold=True)
    letter = (initial or "?")[:1].upper()
    bbox = draw.textbbox((0, 0), letter, font=font)
    draw.text(
        ((size - (bbox[2] - bbox[0])) / 2, (size - (bbox[3] - bbox[1])) / 2 - 12),
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


def _load_template() -> Image.Image | None:
    if not TEMPLATE_PATH.exists():
        return None
    try:
        return _fit_cover(Image.open(TEMPLATE_PATH), CARD_SIZE)
    except Exception:
        return None


def _text_center(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], text: str, font: ImageFont.ImageFont, fill) -> None:
    left, top, right, bottom = box
    bbox = draw.textbbox((0, 0), text, font=font)
    x = left + ((right - left) - (bbox[2] - bbox[0])) / 2
    y = top + ((bottom - top) - (bbox[3] - bbox[1])) / 2
    draw.text((x, y), text, font=font, fill=fill)


def _draw_anime_figure(draw: ImageDraw.ImageDraw) -> None:
    hair_dark = (28, 8, 10, 255)
    hair_red = (160, 12, 22, 255)
    skin = (222, 132, 118, 255)
    shadow = (20, 3, 6, 210)

    draw.polygon([(1220, 130), (1445, 84), (1525, 254), (1466, 560), (1220, 640), (1058, 430)], fill=shadow)
    for points in [
        [(1190, 132), (1260, 40), (1290, 190)],
        [(1398, 92), (1510, 30), (1468, 230)],
        [(1370, 462), (1585, 322), (1435, 690)],
        [(1142, 250), (1008, 312), (1165, 382)],
    ]:
        draw.polygon(points, fill=hair_dark, outline=(238, 52, 60, 190))

    draw.ellipse((1188, 148, 1445, 405), fill=skin, outline=(70, 8, 12, 150), width=4)
    draw.polygon([(1205, 160), (1300, 95), (1426, 150), (1400, 250), (1250, 225)], fill=hair_dark)
    draw.polygon([(1180, 212), (1298, 130), (1260, 282)], fill=hair_dark)
    draw.polygon([(1320, 112), (1447, 162), (1385, 292)], fill=hair_dark)
    draw.arc((1224, 265, 1320, 326), start=192, end=340, fill=(68, 16, 18, 255), width=4)
    draw.arc((1335, 266, 1410, 322), start=200, end=330, fill=(68, 16, 18, 255), width=4)
    draw.ellipse((1264, 283, 1290, 306), fill=(224, 18, 24, 255))
    draw.ellipse((1360, 283, 1386, 306), fill=(224, 18, 24, 255))
    draw.line((1288, 350, 1350, 355), fill=(96, 16, 16, 180), width=3)
    draw.polygon([(1110, 492), (1420, 420), (1505, 748), (1050, 782)], fill=(30, 8, 12, 245), outline=(230, 20, 35, 150))
    draw.line((1160, 510, 1480, 745), fill=(214, 12, 24, 180), width=8)
    draw.line((1445, 420, 1515, 260), fill=(214, 12, 24, 140), width=7)


def _draw_fallback_template() -> Image.Image:
    width, height = CARD_SIZE
    rng = random.Random("mangas-brasil-perfil-template")
    base = Image.new("RGBA", CARD_SIZE, (3, 3, 5, 255))
    draw = ImageDraw.Draw(base, "RGBA")

    for y in range(height):
        r = int(4 + 16 * (y / height))
        draw.line((0, y, width, y), fill=(r, 2, 3, 255))

    texture = Image.new("RGBA", CARD_SIZE, (0, 0, 0, 0))
    tex = ImageDraw.Draw(texture, "RGBA")
    for _ in range(45):
        x = rng.randint(-60, width + 40)
        y = rng.randint(-40, height + 40)
        w = rng.randint(120, 260)
        h = rng.randint(70, 180)
        tex.rectangle((x, y, x + w, y + h), outline=(255, 255, 255, rng.randint(8, 24)), width=2)
        tex.line((x, y + h, x + w, y), fill=(230, 18, 28, rng.randint(8, 32)), width=2)

    for _ in range(115):
        x = rng.randint(-120, width + 120)
        y = rng.randint(-80, height + 80)
        length = rng.randint(90, 430)
        angle = rng.uniform(-0.45, 0.3)
        tex.line(
            (x, y, int(x + length * math.cos(angle)), int(y + length * math.sin(angle))),
            fill=(rng.randint(120, 245), rng.randint(0, 22), rng.randint(0, 18), rng.randint(28, 115)),
            width=rng.randint(2, 9),
        )

    for _ in range(360):
        x = rng.randint(0, width)
        y = rng.randint(0, height)
        radius = rng.randint(1, 5)
        tex.ellipse((x - radius, y - radius, x + radius, y + radius), fill=(235, 18, 28, rng.randint(35, 155)))

    base.alpha_composite(texture.filter(ImageFilter.GaussianBlur(0.2)))
    draw = ImageDraw.Draw(base, "RGBA")
    draw.rectangle((0, 0, width, height), fill=(0, 0, 0, 65))

    draw.ellipse((72, 172, 558, 658), fill=(2, 2, 3, 232), outline=(235, 16, 28, 255), width=8)
    draw.ellipse((87, 187, 543, 643), outline=(255, 52, 58, 92), width=3)
    draw.ellipse((255, 620, 412, 777), fill=(10, 10, 12, 255), outline=(235, 16, 28, 255), width=8)
    draw.line((295, 675, 372, 675), fill=(235, 16, 28, 255), width=10)
    draw.line((310, 686, 310, 735), fill=(235, 16, 28, 255), width=8)
    draw.line((357, 686, 357, 735), fill=(235, 16, 28, 255), width=8)

    draw.ellipse((742, 58, 948, 264), fill=(205, 0, 16, 210), outline=(255, 31, 42, 100), width=5)
    draw.rectangle((782, 142, 910, 170), fill=(15, 6, 6, 240))
    draw.rectangle((800, 172, 820, 220), fill=(15, 6, 6, 240))
    draw.rectangle((870, 172, 890, 220), fill=(15, 6, 6, 240))

    _draw_anime_figure(draw)

    title_white = _font(110, bold=True)
    title_red = _font(142, bold=True)
    ribbon_font = _font(44, bold=True)
    tag_font = _font(34, bold=True)

    draw.text((650, 248), "SEU", font=title_white, fill=(246, 246, 246, 255))
    draw.text((650, 350), "PERFIL", font=title_red, fill=(224, 8, 22, 255))
    draw.rounded_rectangle((608, 588, 1138, 672), radius=8, fill=(196, 12, 22, 235))
    _text_center(draw, (608, 588, 1138, 672), "MANGÁS BRASIL +", ribbon_font, (255, 255, 255, 245))
    draw.text((752, 738), "O MELHOR DESTINO", font=tag_font, fill=(255, 255, 255, 240))
    draw.text((686, 780), "PARA OS MELHORES MANGÁS!", font=tag_font, fill=(255, 255, 255, 240))
    draw.text((1000, 780), "MANGÁS!", font=tag_font, fill=(238, 18, 28, 255))
    return base


def _profile_template() -> Image.Image:
    return _load_template() or _draw_fallback_template()


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
    base = _profile_template()
    avatar = _avatar_image(avatar_bytes, AVATAR_SIZE, (name or username or "?")[:1])
    _paste_circle(base, avatar, AVATAR_XY, AVATAR_SIZE)

    out = BytesIO()
    out.name = "perfil.png"
    base.convert("RGB").save(out, "PNG", optimize=True)
    out.seek(0)
    return out
