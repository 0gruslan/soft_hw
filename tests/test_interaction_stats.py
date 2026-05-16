from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from services.interaction_service import main as interaction_main


def test_compute_stats_from_interactions():
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    interaction_main.Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    one_hour_ago = now - timedelta(hours=1)

    with SessionLocal() as db:
        rows = [
            interaction_main.Interaction(
                id="1",
                from_user_id="u1",
                to_user_id="u2",
                action="like",
                created_at=now,
            ),
            interaction_main.Interaction(
                id="2",
                from_user_id="u1",
                to_user_id="u3",
                action="skip",
                created_at=one_hour_ago,
            ),
            interaction_main.Interaction(
                id="3",
                from_user_id="u2",
                to_user_id="u1",
                action="like",
                created_at=now,
            ),
            interaction_main.Interaction(
                id="4",
                from_user_id="u4",
                to_user_id="u1",
                action="skip",
                created_at=now,
            ),
        ]
        db.add_all(rows)
        db.commit()

        stats = interaction_main.compute_stats(db, "u1")

    assert stats.received_likes == 1
    assert stats.received_skips == 1
    assert stats.given_likes == 1
    assert stats.given_skips == 1
    assert stats.mutual_likes == 1
    assert 0.0 <= stats.hour_activity_ratio <= 1.0
