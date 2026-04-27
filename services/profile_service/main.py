import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Integer, String, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./profile_service.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Profile(Base):
    __tablename__ = "profiles"
    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True, nullable=False)
    age = Column(Integer, nullable=False)
    gender = Column(String, nullable=False)
    bio = Column(String, nullable=False, default="")
    city = Column(String, nullable=False)
    interests = Column(String, nullable=False, default="")
    photo_count = Column(Integer, nullable=False, default=0)
    preferred_gender = Column(String, nullable=True)
    preferred_age_min = Column(Integer, nullable=True)
    preferred_age_max = Column(Integer, nullable=True)
    preferred_city = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class ProfileCreate(BaseModel):
    user_id: str
    age: int
    gender: str
    bio: str = ""
    city: str
    interests: str = ""
    photo_count: int = 0
    preferred_gender: Optional[str] = None
    preferred_age_min: Optional[int] = None
    preferred_age_max: Optional[int] = None
    preferred_city: Optional[str] = None


class ProfileUpdate(BaseModel):
    age: int
    gender: str
    bio: str = ""
    city: str
    interests: str = ""
    photo_count: int = 0
    preferred_gender: Optional[str] = None
    preferred_age_min: Optional[int] = None
    preferred_age_max: Optional[int] = None
    preferred_city: Optional[str] = None


class ProfileOut(BaseModel):
    id: str
    user_id: str
    age: int
    gender: str
    bio: str
    city: str
    interests: str
    photo_count: int
    preferred_gender: Optional[str]
    preferred_age_min: Optional[int]
    preferred_age_max: Optional[int]
    preferred_city: Optional[str]
    created_at: datetime
    updated_at: datetime


app = FastAPI()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


def to_out(profile: Profile) -> ProfileOut:
    return ProfileOut(
        id=profile.id,
        user_id=profile.user_id,
        age=profile.age,
        gender=profile.gender,
        bio=profile.bio,
        city=profile.city,
        interests=profile.interests,
        photo_count=profile.photo_count,
        preferred_gender=profile.preferred_gender,
        preferred_age_min=profile.preferred_age_min,
        preferred_age_max=profile.preferred_age_max,
        preferred_city=profile.preferred_city,
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


@app.post("/profiles", response_model=ProfileOut)
def create_profile(payload: ProfileCreate):
    with SessionLocal() as db:
        existing = db.execute(select(Profile).where(Profile.user_id == payload.user_id)).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail="Profile already exists")
        now = datetime.now(timezone.utc)
        profile = Profile(
            id=str(uuid.uuid4()),
            user_id=payload.user_id,
            age=payload.age,
            gender=payload.gender,
            bio=payload.bio,
            city=payload.city,
            interests=payload.interests,
            photo_count=max(0, payload.photo_count),
            preferred_gender=payload.preferred_gender,
            preferred_age_min=payload.preferred_age_min,
            preferred_age_max=payload.preferred_age_max,
            preferred_city=payload.preferred_city,
            created_at=now,
            updated_at=now,
        )
        db.add(profile)
        db.commit()
        db.refresh(profile)
        return to_out(profile)


@app.get("/profiles/{profile_id}", response_model=ProfileOut)
def get_profile(profile_id: str):
    with SessionLocal() as db:
        profile = db.execute(select(Profile).where(Profile.id == profile_id)).scalar_one_or_none()
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        return to_out(profile)


@app.get("/profiles/by-user/{user_id}", response_model=ProfileOut)
def get_profile_by_user(user_id: str):
    with SessionLocal() as db:
        profile = db.execute(select(Profile).where(Profile.user_id == user_id)).scalar_one_or_none()
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        return to_out(profile)


@app.put("/profiles/by-user/{user_id}", response_model=ProfileOut)
def update_profile_by_user(user_id: str, payload: ProfileUpdate):
    with SessionLocal() as db:
        profile = db.execute(select(Profile).where(Profile.user_id == user_id)).scalar_one_or_none()
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        profile.age = payload.age
        profile.gender = payload.gender
        profile.bio = payload.bio
        profile.city = payload.city
        profile.interests = payload.interests
        profile.photo_count = max(0, payload.photo_count)
        profile.preferred_gender = payload.preferred_gender
        profile.preferred_age_min = payload.preferred_age_min
        profile.preferred_age_max = payload.preferred_age_max
        profile.preferred_city = payload.preferred_city
        profile.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(profile)
        return to_out(profile)


@app.delete("/profiles/by-user/{user_id}")
def delete_profile_by_user(user_id: str):
    with SessionLocal() as db:
        profile = db.execute(select(Profile).where(Profile.user_id == user_id)).scalar_one_or_none()
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        db.delete(profile)
        db.commit()
        return {"deleted": True}


@app.get("/profiles", response_model=list[ProfileOut])
def list_profiles(limit: int = Query(default=100, ge=1, le=500)):
    with SessionLocal() as db:
        profiles = db.execute(select(Profile).limit(limit)).scalars().all()
        return [to_out(profile) for profile in profiles]


@app.get("/profiles/candidates/{viewer_user_id}", response_model=list[ProfileOut])
def list_candidates(viewer_user_id: str, limit: int = Query(default=200, ge=1, le=1000)):
    with SessionLocal() as db:
        profiles = (
            db.execute(select(Profile).where(Profile.user_id != viewer_user_id).limit(limit))
            .scalars()
            .all()
        )
        return [to_out(profile) for profile in profiles]


@app.get("/health")
def health():
    return {"status": "ok"}
