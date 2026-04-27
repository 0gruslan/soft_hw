import os

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8000")
PROFILE_SERVICE_URL = os.getenv("PROFILE_SERVICE_URL", "http://localhost:8001")
INTERACTION_SERVICE_URL = os.getenv("INTERACTION_SERVICE_URL", "http://localhost:8002")
RANKING_SERVICE_URL = os.getenv("RANKING_SERVICE_URL", "http://localhost:8003")

SKIP_VALUES = {
    "",
    "-",
    "_",
    "none",
    "null",
    "пропустить",
    "нет",
    "не важно",
    "неважно",
    "любой",
}

PROFILE_STEPS = [
    ("age", "Сколько тебе лет?", "required_int"),
    ("gender", "Укажи пол (муж, жен, другое):", "required_text"),
    ("city", "Из какого ты города?", "required_text"),
    ("bio", "Расскажи коротко о себе:", "required_text"),
    ("interests", "Перечисли интересы через запятую:", "required_text"),
    ("photo_count", "Сколько фото загрузил(а)?", "required_non_negative_int"),
    (
        "preferred_gender",
        "Какой пол хочешь видеть в ленте? (муж, жен, другой) или напиши «пропустить»:",
        "optional_text",
    ),
    ("preferred_age_min", "Минимальный возраст в ленте? или «пропустить»:", "optional_int"),
    ("preferred_age_max", "Максимальный возраст в ленте? или «пропустить»:", "optional_int"),
    ("preferred_city", "Предпочитаемый город? или «пропустить»:", "optional_text"),
]


def command_args(text: str, command: str) -> str:
    if text.startswith(command):
        return text[len(command):].strip()
    return ""


def is_skip_value(value: str) -> bool:
    return value.strip().lower() in SKIP_VALUES


def normalize_gender(value: str):
    cleaned = value.strip().lower()
    mapping = {
        "м": "муж",
        "мужчина": "муж",
        "парень": "муж",
        "ж": "жен",
        "женщина": "жен",
        "девушка": "жен",
        "другой": "другое",
    }
    return mapping.get(cleaned, value.strip())


def display_value(value):
    if value is None:
        return "не указано"
    if isinstance(value, str) and value.strip() == "":
        return "не указано"
    return str(value)


def parse_profile_pipe_format(raw: str):
    parts = [part.strip() for part in raw.split("|")]
    if len(parts) < 6:
        return None
    try:
        age = int(parts[0])
        photo_count = int(parts[5])
    except ValueError:
        return None
    preferred_gender = parts[6].strip() if len(parts) > 6 and not is_skip_value(parts[6]) else None
    preferred_age_min = parts[7].strip() if len(parts) > 7 and not is_skip_value(parts[7]) else None
    preferred_age_max = parts[8].strip() if len(parts) > 8 and not is_skip_value(parts[8]) else None
    preferred_city = parts[9].strip() if len(parts) > 9 and not is_skip_value(parts[9]) else None
    try:
        preferred_age_min_int = int(preferred_age_min) if preferred_age_min is not None else None
        preferred_age_max_int = int(preferred_age_max) if preferred_age_max is not None else None
    except ValueError:
        return None
    payload = {
        "age": age,
        "gender": normalize_gender(parts[1]),
        "city": parts[2],
        "bio": parts[3],
        "interests": parts[4],
        "photo_count": max(0, photo_count),
        "preferred_gender": normalize_gender(preferred_gender) if preferred_gender is not None else None,
        "preferred_age_min": preferred_age_min_int,
        "preferred_age_max": preferred_age_max_int,
        "preferred_city": preferred_city,
    }
    return payload


def profile_to_text(profile: dict) -> str:
    return (
        "Твоя анкета:\n"
        f"Возраст: {display_value(profile.get('age'))}\n"
        f"Пол: {display_value(profile.get('gender'))}\n"
        f"Город: {display_value(profile.get('city'))}\n"
        f"О себе: {display_value(profile.get('bio'))}\n"
        f"Интересы: {display_value(profile.get('interests'))}\n"
        f"Количество фото: {display_value(profile.get('photo_count'))}\n"
        f"Предпочитаемый пол: {display_value(profile.get('preferred_gender'))}\n"
        f"Возраст от: {display_value(profile.get('preferred_age_min'))}\n"
        f"Возраст до: {display_value(profile.get('preferred_age_max'))}\n"
        f"Предпочитаемый город: {display_value(profile.get('preferred_city'))}"
    )


def ranked_profile_to_text(payload: dict) -> str:
    profile = payload["profile"]
    return (
        "Анкета из ленты:\n"
        f"Возраст: {display_value(profile.get('age'))}\n"
        f"Пол: {display_value(profile.get('gender'))}\n"
        f"Город: {display_value(profile.get('city'))}\n"
        f"О себе: {display_value(profile.get('bio'))}\n"
        f"Интересы: {display_value(profile.get('interests'))}\n"
        f"Итоговый рейтинг: {display_value(profile.get('total_score'))}\n"
        "Действия: /like или /skip"
    )


def begin_profile_wizard(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["profile_wizard"] = {
        "step_index": 0,
        "data": {},
    }


def get_profile_wizard(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("profile_wizard")


def clear_profile_wizard(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("profile_wizard", None)


def parse_step_value(step_type: str, value: str):
    cleaned = value.strip()
    if step_type == "required_text":
        if cleaned == "":
            return False, "Это поле нельзя оставлять пустым."
        return True, cleaned
    if step_type == "required_int":
        try:
            parsed = int(cleaned)
        except ValueError:
            return False, "Нужно ввести число."
        if parsed <= 0:
            return False, "Число должно быть больше 0."
        return True, parsed
    if step_type == "required_non_negative_int":
        try:
            parsed = int(cleaned)
        except ValueError:
            return False, "Нужно ввести число."
        if parsed < 0:
            return False, "Число не может быть отрицательным."
        return True, parsed
    if step_type == "optional_text":
        if is_skip_value(cleaned):
            return True, None
        return True, cleaned
    if step_type == "optional_int":
        if is_skip_value(cleaned):
            return True, None
        try:
            parsed = int(cleaned)
        except ValueError:
            return False, "Нужно число или слово «пропустить»."
        if parsed <= 0:
            return False, "Число должно быть больше 0."
        return True, parsed
    return False, "Ошибка формата."


async def post_json(url: str, payload: dict):
    async with httpx.AsyncClient(timeout=8.0) as client:
        response = await client.post(url, json=payload)
        return response


async def put_json(url: str, payload: dict):
    async with httpx.AsyncClient(timeout=8.0) as client:
        response = await client.put(url, json=payload)
        return response


async def get_json(url: str):
    async with httpx.AsyncClient(timeout=8.0) as client:
        response = await client.get(url)
        return response


async def delete_request(url: str):
    async with httpx.AsyncClient(timeout=8.0) as client:
        response = await client.delete(url)
        return response


async def get_or_register_user_id(telegram_id: int) -> str:
    response = await post_json(f"{USER_SERVICE_URL}/users/register", {"telegram_id": telegram_id})
    response.raise_for_status()
    data = response.json()
    return data["id"]


async def save_profile_for_user(user_id: str, payload: dict) -> str:
    update_response = await put_json(f"{PROFILE_SERVICE_URL}/profiles/by-user/{user_id}", payload)
    if update_response.status_code == 404:
        create_payload = {"user_id": user_id, **payload}
        create_response = await post_json(f"{PROFILE_SERVICE_URL}/profiles", create_payload)
        create_response.raise_for_status()
        return "created"
    update_response.raise_for_status()
    return "updated"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None:
        return
    telegram_id = update.effective_user.id
    try:
        user_id = await get_or_register_user_id(telegram_id)
        await update.message.reply_text(
            "Готово, ты зарегистрирован(а).\n"
            f"Твой ID: {user_id}\n"
            "Давай заполним анкету: /set_profile"
        )
    except httpx.RequestError:
        await update.message.reply_text("Ошибка регистрации: сервис пользователей недоступен.")
    except httpx.HTTPStatusError as exc:
        await update.message.reply_text(
            f"Ошибка регистрации: сервис пользователей вернул код {exc.response.status_code}."
        )
    except Exception:
        await update.message.reply_text("Ошибка регистрации. Попробуй еще раз.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    await update.message.reply_text(
        "Вот что я умею:\n"
        "/start - регистрация\n"
        "/set_profile - заполнение анкеты по шагам\n"
        "/cancel_profile - прервать заполнение анкеты\n"
        "/my_profile - показать мою анкету\n"
        "/delete_profile - удалить мою анкету\n"
        "/feed - показать следующую анкету\n"
        "/like - поставить лайк\n"
        "/skip - пропустить\n\n"
        "Если удобно, можно заполнить анкету одной строкой:\n"
        "/set_profile 25|муж|Москва|Люблю спорт|спорт,кино|3|жен|20|30|Москва"
    )


async def set_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None or update.message.text is None:
        return

    raw_args = command_args(update.message.text, "/set_profile")
    if raw_args != "":
        payload = parse_profile_pipe_format(raw_args)
        if payload is None:
            await update.message.reply_text(
                "Не получилось распознать данные.\n"
                "Проще так: отправь просто /set_profile, и я задам вопросы по шагам."
            )
            return
        if payload.get("preferred_age_min") and payload.get("preferred_age_max"):
            if payload["preferred_age_min"] > payload["preferred_age_max"]:
                await update.message.reply_text("Возраст «от» не может быть больше возраста «до».")
                return
        try:
            user_id = await get_or_register_user_id(update.effective_user.id)
            result = await save_profile_for_user(user_id, payload)
            if result == "created":
                await update.message.reply_text("Анкета создана.")
            else:
                await update.message.reply_text("Анкета обновлена.")
        except Exception:
            await update.message.reply_text("Не удалось сохранить анкету.")
        return

    begin_profile_wizard(context)
    first_question = PROFILE_STEPS[0][1]
    await update.message.reply_text(
        "Класс, начинаем заполнение анкеты.\n"
        "Я задам несколько коротких вопросов.\n"
        "Если захочешь остановиться, просто отправь /cancel_profile.\n\n"
        f"{first_question}"
    )


async def cancel_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    wizard = get_profile_wizard(context)
    if wizard is None:
        await update.message.reply_text("Сейчас анкета не заполняется.")
        return
    clear_profile_wizard(context)
    await update.message.reply_text("Ок, остановил заполнение анкеты.")


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None or update.message.text is None:
        return
    wizard = get_profile_wizard(context)
    if wizard is None:
        return

    step_index = wizard["step_index"]
    step_key, step_question, step_type = PROFILE_STEPS[step_index]
    ok, parsed_or_error = parse_step_value(step_type, update.message.text)
    if not ok:
        await update.message.reply_text(f"{parsed_or_error}\n{step_question}")
        return

    value = parsed_or_error
    if step_key in {"gender", "preferred_gender"} and value is not None:
        value = normalize_gender(value)

    wizard["data"][step_key] = value
    wizard["step_index"] = step_index + 1

    if wizard["step_index"] < len(PROFILE_STEPS):
        next_question = PROFILE_STEPS[wizard["step_index"]][1]
        await update.message.reply_text(next_question)
        return

    payload = wizard["data"]
    min_age = payload.get("preferred_age_min")
    max_age = payload.get("preferred_age_max")
    if min_age is not None and max_age is not None and min_age > max_age:
        wizard["step_index"] = 8
        await update.message.reply_text(
            "Возраст «от» не может быть больше возраста «до».\n"
            "Введи максимальный возраст еще раз:"
        )
        return

    try:
        user_id = await get_or_register_user_id(update.effective_user.id)
        result = await save_profile_for_user(user_id, payload)
        clear_profile_wizard(context)
        if result == "created":
            await update.message.reply_text("Отлично, анкета создана.")
        else:
            await update.message.reply_text("Супер, анкета обновлена.")
    except Exception:
        clear_profile_wizard(context)
        await update.message.reply_text("Не удалось сохранить анкету. Попробуй снова с /set_profile.")


async def my_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None:
        return
    try:
        user_id = await get_or_register_user_id(update.effective_user.id)
        response = await get_json(f"{PROFILE_SERVICE_URL}/profiles/by-user/{user_id}")
        if response.status_code == 404:
            await update.message.reply_text("Анкета не найдена. Создай ее через /set_profile.")
            return
        response.raise_for_status()
        await update.message.reply_text(profile_to_text(response.json()))
    except Exception:
        await update.message.reply_text("Не удалось получить анкету.")


async def delete_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None:
        return
    try:
        user_id = await get_or_register_user_id(update.effective_user.id)
        response = await delete_request(f"{PROFILE_SERVICE_URL}/profiles/by-user/{user_id}")
        if response.status_code == 404:
            await update.message.reply_text("Анкета не найдена.")
            return
        response.raise_for_status()
        await update.message.reply_text("Анкета удалена.")
    except Exception:
        await update.message.reply_text("Не удалось удалить анкету.")


async def feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user is None or update.message is None:
        return
    try:
        user_id = await get_or_register_user_id(update.effective_user.id)
        response = await get_json(f"{RANKING_SERVICE_URL}/ranking/next/{user_id}")
        if response.status_code == 404:
            await update.message.reply_text("Пока некого показывать. Заполни анкету и загляни чуть позже.")
            return
        response.raise_for_status()
        payload = response.json()
        context.user_data["last_candidate_user_id"] = payload["profile"]["user_id"]
        await update.message.reply_text(ranked_profile_to_text(payload))
    except Exception:
        await update.message.reply_text("Не удалось получить ленту.")


async def save_action(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    if update.effective_user is None or update.message is None:
        return
    try:
        candidate_user_id = context.user_data.get("last_candidate_user_id")
        if not candidate_user_id:
            await update.message.reply_text("Сначала открой анкету командой /feed.")
            return
        user_id = await get_or_register_user_id(update.effective_user.id)
        response = await post_json(
            f"{INTERACTION_SERVICE_URL}/interactions",
            {"from_user_id": user_id, "to_user_id": candidate_user_id, "action": action},
        )
        response.raise_for_status()
        result = response.json()
        if result.get("is_match"):
            await update.message.reply_text("Есть мэтч! Лайк сохранен.")
        else:
            if action == "like":
                await update.message.reply_text("Лайк поставлен.")
            else:
                await update.message.reply_text("Пропустили, показываю дальше по /feed.")
    except Exception:
        await update.message.reply_text("Не удалось сохранить действие.")


async def like(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_action(update, context, "like")


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_action(update, context, "skip")


def main():
    if not TOKEN:
        raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN в .env")
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("set_profile", set_profile))
    app.add_handler(CommandHandler("cancel_profile", cancel_profile))
    app.add_handler(CommandHandler("my_profile", my_profile))
    app.add_handler(CommandHandler("delete_profile", delete_profile))
    app.add_handler(CommandHandler("feed", feed))
    app.add_handler(CommandHandler("like", like))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    app.run_polling()


if __name__ == "__main__":
    main()
