import asyncio

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    filters,
)

from config import BOT_TOKEN
from core.http_client import close_http_client
from core.pdf_queue import start_pdf_workers, stop_pdf_workers
from handlers.broadcast import (
    broadcast_callbacks,
    broadcast_command,
    broadcast_message_router,
)
from handlers.callbacks import callbacks
from handlers.help import ajuda
from handlers.inline import inline_query
from handlers.metricas import metricas, metricas_limpar
from handlers.novoseps import auto_post_new_eps_job, postnovoseps
from handlers.postmanga import postmanga
from handlers.referral import indicacoes, referral_button
from handlers.referral_admin import auto_referral_check_job, refstats
from handlers.search import buscar
from handlers.start import start
from services.catalog_client import schedule_warm_catalog_cache, warm_catalog_cache
from services.metrics import init_metrics_db
from services.referral_db import init_referral_db

init_metrics_db()
init_referral_db()

MAX_CONCURRENT_UPDATES = 128
BOT_API_CONNECTION_POOL = 64
BOT_API_POOL_TIMEOUT = 30.0
BOT_API_CONNECT_TIMEOUT = 10.0
BOT_API_READ_TIMEOUT = 25.0
BOT_API_WRITE_TIMEOUT = 25.0


async def post_init(app: Application) -> None:
    await start_pdf_workers(app)
    schedule_warm_catalog_cache()


async def post_shutdown(app: Application) -> None:
    await stop_pdf_workers(app)
    await close_http_client()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    print("ERRO:", repr(context.error))
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("Ocorreu um erro ao processar sua solicitacao.")
    except Exception:
        pass


async def warm_catalog_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await warm_catalog_cache()


def _register_jobs(app: Application) -> None:
    if not app.job_queue:
        print("JobQueue nao disponivel. Instale python-telegram-bot[job-queue]==22.6")
        return

    app.job_queue.run_repeating(
        auto_post_new_eps_job,
        interval=600,
        first=20,
        name="auto_post_new_chapters",
    )
    app.job_queue.run_repeating(
        auto_referral_check_job,
        interval=3600,
        first=60,
        name="auto_referral_check",
    )
    app.job_queue.run_repeating(
        warm_catalog_job,
        interval=600,
        first=5,
        name="warm_catalog_cache",
    )


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Configure BOT_TOKEN nas variaveis de ambiente.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(MAX_CONCURRENT_UPDATES)
        .connection_pool_size(BOT_API_CONNECTION_POOL)
        .pool_timeout(BOT_API_POOL_TIMEOUT)
        .connect_timeout(BOT_API_CONNECT_TIMEOUT)
        .read_timeout(BOT_API_READ_TIMEOUT)
        .write_timeout(BOT_API_WRITE_TIMEOUT)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buscar", buscar))
    app.add_handler(CommandHandler("ajuda", ajuda))
    app.add_handler(CommandHandler("postnovoseps", postnovoseps))
    app.add_handler(CommandHandler("postnovoscaps", postnovoseps))
    app.add_handler(CommandHandler("postmanga", postmanga))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("indicacoes", indicacoes))
    app.add_handler(CommandHandler("refstats", refstats))
    app.add_handler(CommandHandler("metricas", metricas))
    app.add_handler(CommandHandler("metricaslimpar", metricas_limpar))
    app.add_handler(InlineQueryHandler(inline_query))

    app.add_handler(CallbackQueryHandler(broadcast_callbacks, pattern=r"^bc\|"))
    app.add_handler(CallbackQueryHandler(referral_button, pattern=r"^noop_indicar$"))
    app.add_handler(CallbackQueryHandler(callbacks, pattern=r"^mb\|"))

    app.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_message_router),
        group=99,
    )

    _register_jobs(app)
    app.add_error_handler(error_handler)

    print("Bot de mangas rodando...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
