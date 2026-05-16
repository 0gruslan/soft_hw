import os

import httpx
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

load_dotenv()

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
CELERY_QUEUE_NAME = os.getenv("CELERY_QUEUE_NAME", "ranking_updates")
RANKING_SERVICE_URL = os.getenv("RANKING_SERVICE_URL", "http://localhost:8003")
PROFILE_SERVICE_URL = os.getenv("PROFILE_SERVICE_URL", "http://localhost:8001")
REFRESH_ALL_INTERVAL_MINUTES = int(os.getenv("REFRESH_ALL_INTERVAL_MINUTES", "5"))
REFRESH_ALL_LIMIT = int(os.getenv("REFRESH_ALL_LIMIT", "1000"))

celery_app = Celery(
    "celery_worker",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Europe/Moscow",
    enable_utc=True,
    task_default_queue=CELERY_QUEUE_NAME,
)

celery_app.conf.beat_schedule = {
    "refresh-all-rankings": {
        "task": "celery_worker.refresh_all_rankings",
        "schedule": crontab(minute=f"*/{REFRESH_ALL_INTERVAL_MINUTES}"),
    }
}


@celery_app.task(name="celery_worker.refresh_user_ranking")
def refresh_user_ranking(user_id: str):
    with httpx.Client(timeout=8.0) as client:
        response = client.post(f"{RANKING_SERVICE_URL}/ranking/refresh/{user_id}")
        if response.status_code in (200, 404):
            return {"ok": True, "user_id": user_id, "status": response.status_code}
        response.raise_for_status()
        return {"ok": True, "user_id": user_id, "status": response.status_code}


@celery_app.task(name="celery_worker.refresh_all_rankings")
def refresh_all_rankings():
    with httpx.Client(timeout=12.0) as client:
        profiles_response = client.get(f"{PROFILE_SERVICE_URL}/profiles?limit={REFRESH_ALL_LIMIT}")
        if profiles_response.status_code == 404:
            return {"refreshed": 0, "status": "no_profiles"}
        profiles_response.raise_for_status()
        profiles = profiles_response.json()
        if not isinstance(profiles, list) or len(profiles) == 0:
            return {"refreshed": 0, "status": "empty"}

        user_ids = []
        seen = set()
        for profile in profiles:
            user_id = profile.get("user_id")
            if user_id and user_id not in seen:
                seen.add(user_id)
                user_ids.append(user_id)

        refreshed = 0
        for user_id in user_ids:
            response = client.post(f"{RANKING_SERVICE_URL}/ranking/refresh/{user_id}")
            if response.status_code in (200, 404):
                refreshed += 1
                continue
            response.raise_for_status()
            refreshed += 1

        return {"refreshed": refreshed, "total": len(user_ids)}
