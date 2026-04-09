import os

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8000")


async def register_user(telegram_id: int):
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(
            f"{USER_SERVICE_URL}/users/register",
            json={"telegram_id": telegram_id},
        )
        response.raise_for_status()
        return response.json()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None:
        return
    telegram_id = update.effective_user.id
    try:
        data = await register_user(telegram_id)
        if data.get("created"):
            text = "Регистрация выполнена. Можешь продолжить позже."
        else:
            text = "Ты уже зарегистрирован."
    except Exception:
        text = "Не удалось зарегистрировать. Попробуй позже."
    await update.message.reply_text(text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    await update.message.reply_text("Команды: /start")


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.run_polling()


if __name__ == "__main__":
    main()
