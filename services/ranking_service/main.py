import json
import os
from typing import Any, Optional

import httpx
import redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

PROFILE_SERVICE_URL = os.getenv("PROFILE_SERVICE_URL", "http://localhost:8001")
INTERACTION_SERVICE_URL = os.getenv("INTERACTION_SERVICE_URL", "http://localhost:8002")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "10"))
QUEUE_TTL_SECONDS = int(os.getenv("QUEUE_TTL_SECONDS", "1800"))

app = FastAPI()
memory_cache: dict[str, list[str]] = {}


class RankedProfile(BaseModel):
    profile_id: str
    user_id: str
    age: int
    gender: str
    bio: str
    city: str
    interests: str
    photo_count: int
    base_score: float
    behavior_score: float
    total_score: float


class NextProfileOut(BaseModel):
    profile: RankedProfile
    source: str
    remaining_in_cache: int


def get_redis_client():
    try:
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        client.ping()
        return client
    except Exception:
        return None


def queue_key(viewer_user_id: str) -> str:
    return f"ranking:queue:{viewer_user_id}"


def cache_pop(viewer_user_id: str) -> Optional[dict[str, Any]]:
    client = get_redis_client()
    key = queue_key(viewer_user_id)
    if client:
        raw = client.lpop(key)
        if raw is None:
            return None
        return json.loads(raw)
    queue = memory_cache.get(key, [])
    if len(queue) == 0:
        return None
    raw = queue.pop(0)
    memory_cache[key] = queue
    return json.loads(raw)


def cache_length(viewer_user_id: str) -> int:
    client = get_redis_client()
    key = queue_key(viewer_user_id)
    if client:
        return int(client.llen(key))
    return len(memory_cache.get(key, []))


def cache_replace(viewer_user_id: str, profiles: list[dict[str, Any]]):
    key = queue_key(viewer_user_id)
    values = [json.dumps(profile) for profile in profiles]
    client = get_redis_client()
    if client:
        pipe = client.pipeline()
        pipe.delete(key)
        if len(values) > 0:
            pipe.rpush(key, *values)
            pipe.expire(key, QUEUE_TTL_SECONDS)
        pipe.execute()
        return
    memory_cache[key] = values


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def calc_profile_completeness(profile: dict[str, Any]) -> float:
    checks = [
        profile.get("age") is not None,
        bool(profile.get("gender")),
        bool(profile.get("bio")),
        bool(profile.get("city")),
        bool(profile.get("interests")),
    ]
    return sum(1 for check in checks if check) / len(checks)


def calc_preference_match(viewer: dict[str, Any], candidate: dict[str, Any]) -> float:
    checks = []

    preferred_gender = viewer.get("preferred_gender")
    if preferred_gender:
        checks.append(str(candidate.get("gender", "")).lower() == str(preferred_gender).lower())

    preferred_age_min = viewer.get("preferred_age_min")
    preferred_age_max = viewer.get("preferred_age_max")
    if preferred_age_min is not None or preferred_age_max is not None:
        age = candidate.get("age")
        if age is None:
            checks.append(False)
        else:
            ok_min = True if preferred_age_min is None else age >= preferred_age_min
            ok_max = True if preferred_age_max is None else age <= preferred_age_max
            checks.append(ok_min and ok_max)

    preferred_city = viewer.get("preferred_city")
    if preferred_city:
        checks.append(str(candidate.get("city", "")).lower() == str(preferred_city).lower())

    if len(checks) == 0:
        return 1.0
    return sum(1 for check in checks if check) / len(checks)


def calc_base_score(viewer: dict[str, Any], candidate: dict[str, Any]) -> float:
    completeness = calc_profile_completeness(candidate)
    photo_score = clamp(float(candidate.get("photo_count", 0)) / 5.0)
    preference_match = calc_preference_match(viewer, candidate)
    return clamp(0.5 * completeness + 0.2 * photo_score + 0.3 * preference_match)


def calc_behavior_score(stats: dict[str, Any]) -> float:
    received_likes = int(stats.get("received_likes", 0))
    received_skips = int(stats.get("received_skips", 0))
    given_likes = int(stats.get("given_likes", 0))
    mutual_likes = int(stats.get("mutual_likes", 0))
    hour_activity_ratio = float(stats.get("hour_activity_ratio", 0.0))

    total_received = received_likes + received_skips
    if total_received == 0:
        like_ratio = 0.5
    else:
        like_ratio = received_likes / total_received

    if given_likes == 0:
        mutual_rate = 0.0
    else:
        mutual_rate = mutual_likes / given_likes

    return clamp(0.5 * like_ratio + 0.3 * clamp(mutual_rate) + 0.2 * clamp(hour_activity_ratio))


def calc_total_score(base_score: float, behavior_score: float) -> float:
    return 0.4 * base_score + 0.6 * behavior_score


def fetch_json(method: str, url: str, payload: Optional[dict[str, Any]] = None):
    with httpx.Client(timeout=8.0) as client:
        if method == "GET":
            response = client.get(url)
        elif method == "POST":
            response = client.post(url, json=payload)
        else:
            raise RuntimeError("Unsupported HTTP method")
    if response.status_code == 404:
        return None, 404
    response.raise_for_status()
    return response.json(), response.status_code


def build_ranked_profiles(viewer_user_id: str) -> list[dict[str, Any]]:
    viewer_profile, viewer_status = fetch_json("GET", f"{PROFILE_SERVICE_URL}/profiles/by-user/{viewer_user_id}")
    if viewer_status == 404 or viewer_profile is None:
        raise HTTPException(status_code=404, detail="Viewer profile not found")

    candidates, _ = fetch_json("GET", f"{PROFILE_SERVICE_URL}/profiles/candidates/{viewer_user_id}?limit=500")
    if candidates is None or len(candidates) == 0:
        return []

    seen_data, seen_status = fetch_json("GET", f"{INTERACTION_SERVICE_URL}/interactions/seen/{viewer_user_id}")
    seen_targets = set(seen_data["targets"]) if seen_status != 404 and seen_data else set()
    unseen_candidates = [profile for profile in candidates if profile["user_id"] not in seen_targets]
    if len(unseen_candidates) == 0:
        unseen_candidates = candidates

    user_ids = [profile["user_id"] for profile in unseen_candidates]
    stats_rows, _ = fetch_json(
        "POST",
        f"{INTERACTION_SERVICE_URL}/interactions/stats/bulk",
        {"user_ids": user_ids},
    )
    stats_map = {row["user_id"]: row for row in stats_rows}

    ranked = []
    for candidate in unseen_candidates:
        stats = stats_map.get(candidate["user_id"], {})
        base_score = calc_base_score(viewer_profile, candidate)
        behavior_score = calc_behavior_score(stats)
        total_score = calc_total_score(base_score, behavior_score)
        ranked.append(
            {
                "profile_id": candidate["id"],
                "user_id": candidate["user_id"],
                "age": candidate["age"],
                "gender": candidate["gender"],
                "bio": candidate["bio"],
                "city": candidate["city"],
                "interests": candidate["interests"],
                "photo_count": candidate["photo_count"],
                "base_score": round(base_score, 4),
                "behavior_score": round(behavior_score, 4),
                "total_score": round(total_score, 4),
            }
        )

    ranked.sort(key=lambda row: row["total_score"], reverse=True)
    return ranked


def refill_cache(viewer_user_id: str) -> int:
    ranked = build_ranked_profiles(viewer_user_id)
    if len(ranked) == 0:
        cache_replace(viewer_user_id, [])
        return 0
    cache_replace(viewer_user_id, ranked[:PREFETCH_COUNT])
    return min(len(ranked), PREFETCH_COUNT)


@app.get("/ranking/next/{viewer_user_id}", response_model=NextProfileOut)
def get_next_profile(viewer_user_id: str):
    data = cache_pop(viewer_user_id)
    source = "cache"
    if data is None:
        added = refill_cache(viewer_user_id)
        if added == 0:
            raise HTTPException(status_code=404, detail="No profiles available")
        data = cache_pop(viewer_user_id)
        source = "rebuild"
        if data is None:
            raise HTTPException(status_code=404, detail="No profiles available")

    remaining = cache_length(viewer_user_id)
    if remaining == 0:
        refill_cache(viewer_user_id)
        remaining = cache_length(viewer_user_id)

    return NextProfileOut(profile=RankedProfile(**data), source=source, remaining_in_cache=remaining)


@app.post("/ranking/refresh/{viewer_user_id}")
def refresh_ranking(viewer_user_id: str):
    added = refill_cache(viewer_user_id)
    return {"refreshed": True, "cached_count": added}


@app.get("/health")
def health():
    return {"status": "ok"}
