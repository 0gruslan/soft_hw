import os
import uuid
from datetime import datetime, timezone
from typing import Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, String, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./interaction_service.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Interaction(Base):
    __tablename__ = "interactions"
    id = Column(String, primary_key=True, index=True)
    from_user_id = Column(String, index=True, nullable=False)
    to_user_id = Column(String, index=True, nullable=False)
    action = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)


class InteractionIn(BaseModel):
    from_user_id: str
    to_user_id: str
    action: Literal["like", "skip"]


class InteractionOut(BaseModel):
    id: str
    from_user_id: str
    to_user_id: str
    action: str
    created_at: datetime
    is_match: bool


class UserStats(BaseModel):
    user_id: str
    received_likes: int
    received_skips: int
    given_likes: int
    given_skips: int
    mutual_likes: int
    hour_activity_ratio: float


class BulkStatsIn(BaseModel):
    user_ids: list[str]


class SeenTargetsOut(BaseModel):
    from_user_id: str
    targets: list[str]


app = FastAPI()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


def compute_stats(db, user_id: str) -> UserStats:
    received_rows = db.execute(select(Interaction).where(Interaction.to_user_id == user_id)).scalars().all()
    given_rows = db.execute(select(Interaction).where(Interaction.from_user_id == user_id)).scalars().all()

    received_likes = sum(1 for row in received_rows if row.action == "like")
    received_skips = sum(1 for row in received_rows if row.action == "skip")
    given_likes_rows = [row for row in given_rows if row.action == "like"]
    given_skips = sum(1 for row in given_rows if row.action == "skip")

    liked_by_user = {row.to_user_id for row in given_likes_rows}
    likes_to_user = {
        row.from_user_id
        for row in received_rows
        if row.action == "like"
    }
    mutual_likes = len(liked_by_user.intersection(likes_to_user))

    current_hour = datetime.now(timezone.utc).hour
    if len(given_rows) == 0:
        hour_activity_ratio = 0.0
    else:
        hour_rows = sum(1 for row in given_rows if row.created_at.hour == current_hour)
        hour_activity_ratio = hour_rows / len(given_rows)

    return UserStats(
        user_id=user_id,
        received_likes=received_likes,
        received_skips=received_skips,
        given_likes=len(given_likes_rows),
        given_skips=given_skips,
        mutual_likes=mutual_likes,
        hour_activity_ratio=hour_activity_ratio,
    )


@app.post("/interactions", response_model=InteractionOut)
def create_interaction(payload: InteractionIn):
    if payload.from_user_id == payload.to_user_id:
        raise HTTPException(status_code=400, detail="Cannot interact with own profile")

    with SessionLocal() as db:
        interaction = Interaction(
            id=str(uuid.uuid4()),
            from_user_id=payload.from_user_id,
            to_user_id=payload.to_user_id,
            action=payload.action,
            created_at=datetime.now(timezone.utc),
        )
        db.add(interaction)
        db.commit()
        db.refresh(interaction)

        is_match = False
        if payload.action == "like":
            reverse_like = db.execute(
                select(Interaction).where(
                    Interaction.from_user_id == payload.to_user_id,
                    Interaction.to_user_id == payload.from_user_id,
                    Interaction.action == "like",
                )
            ).scalar_one_or_none()
            is_match = reverse_like is not None

        return InteractionOut(
            id=interaction.id,
            from_user_id=interaction.from_user_id,
            to_user_id=interaction.to_user_id,
            action=interaction.action,
            created_at=interaction.created_at,
            is_match=is_match,
        )


@app.get("/interactions/stats/{user_id}", response_model=UserStats)
def get_user_stats(user_id: str):
    with SessionLocal() as db:
        return compute_stats(db, user_id)


@app.post("/interactions/stats/bulk", response_model=list[UserStats])
def get_bulk_stats(payload: BulkStatsIn):
    with SessionLocal() as db:
        unique_ids = list(dict.fromkeys(payload.user_ids))
        return [compute_stats(db, user_id) for user_id in unique_ids]


@app.get("/interactions/seen/{from_user_id}", response_model=SeenTargetsOut)
def get_seen_targets(from_user_id: str):
    with SessionLocal() as db:
        rows = db.execute(select(Interaction.to_user_id).where(Interaction.from_user_id == from_user_id)).all()
        targets = list(dict.fromkeys([row[0] for row in rows]))
        return SeenTargetsOut(from_user_id=from_user_id, targets=targets)


@app.get("/health")
def health():
    return {"status": "ok"}
