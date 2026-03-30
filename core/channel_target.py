from typing import Union


async def ensure_channel_target(bot, target: Union[str, int, None]):
    if target is None:
        raise RuntimeError("Canal de destino nao configurado.")

    if isinstance(target, str):
        target = target.strip()
        if not target:
            raise RuntimeError("Canal de destino vazio.")

        if target.startswith("https://t.me/"):
            target = "@" + target.rstrip("/").split("/")[-1]
        elif target.startswith("t.me/"):
            target = "@" + target.rstrip("/").split("/")[-1]

    chat = await bot.get_chat(target)
    return chat.id
