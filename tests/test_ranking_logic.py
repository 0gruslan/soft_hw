from services.ranking_service import main as ranking_main


def test_base_and_total_score_ranges():
    viewer = {
        "preferred_gender": "female",
        "preferred_age_min": 20,
        "preferred_age_max": 30,
        "preferred_city": "Moscow",
    }
    candidate = {
        "age": 25,
        "gender": "female",
        "bio": "Bio",
        "city": "Moscow",
        "interests": "music",
        "photo_count": 4,
    }
    base_score = ranking_main.calc_base_score(viewer, candidate)
    behavior_score = ranking_main.calc_behavior_score(
        {
            "received_likes": 20,
            "received_skips": 5,
            "given_likes": 10,
            "mutual_likes": 5,
            "hour_activity_ratio": 0.4,
        }
    )
    total_score = ranking_main.calc_total_score(base_score, behavior_score)

    assert 0.0 <= base_score <= 1.0
    assert 0.0 <= behavior_score <= 1.0
    assert 0.0 <= total_score <= 1.0


def test_build_ranked_profiles_sorted(monkeypatch):
    viewer_profile = {
        "id": "p1",
        "user_id": "viewer",
        "age": 25,
        "gender": "male",
        "bio": "viewer",
        "city": "Moscow",
        "interests": "sport",
        "photo_count": 2,
        "preferred_gender": "female",
        "preferred_age_min": 20,
        "preferred_age_max": 30,
        "preferred_city": "Moscow",
    }
    candidates = [
        {
            "id": "c1",
            "user_id": "u1",
            "age": 24,
            "gender": "female",
            "bio": "A",
            "city": "Moscow",
            "interests": "books",
            "photo_count": 5,
        },
        {
            "id": "c2",
            "user_id": "u2",
            "age": 28,
            "gender": "female",
            "bio": "B",
            "city": "Moscow",
            "interests": "travel",
            "photo_count": 1,
        },
    ]
    stats_rows = [
        {
            "user_id": "u1",
            "received_likes": 30,
            "received_skips": 5,
            "given_likes": 10,
            "given_skips": 2,
            "mutual_likes": 8,
            "hour_activity_ratio": 0.5,
        },
        {
            "user_id": "u2",
            "received_likes": 5,
            "received_skips": 15,
            "given_likes": 6,
            "given_skips": 8,
            "mutual_likes": 1,
            "hour_activity_ratio": 0.1,
        },
    ]

    def fake_fetch_json(method, url, payload=None):
        if "/profiles/by-user/" in url:
            return viewer_profile, 200
        if "/profiles/candidates/" in url:
            return candidates, 200
        if "/interactions/seen/" in url:
            return {"from_user_id": "viewer", "targets": []}, 200
        if "/interactions/stats/bulk" in url:
            return stats_rows, 200
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(ranking_main, "fetch_json", fake_fetch_json)
    ranked = ranking_main.build_ranked_profiles("viewer")

    assert len(ranked) == 2
    assert ranked[0]["total_score"] >= ranked[1]["total_score"]
    assert ranked[0]["user_id"] == "u1"
