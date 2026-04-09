import os
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, DateTime, String, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./user_service.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    created_at = Column(DateTime, nullable=False)


class UserIn(BaseModel):
    telegram_id: int


class UserOut(BaseModel):
    id: str
    telegram_id: int
    created_at: datetime
    created: bool


app = FastAPI()


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


def get_or_create_user(telegram_id: int):
    with SessionLocal() as db:
        stmt = select(User).where(User.telegram_id == telegram_id)
        user = db.execute(stmt).scalar_one_or_none()
        if user:
            return user, False
        new_user = User(
            id=str(uuid.uuid4()),
            telegram_id=telegram_id,
            created_at=datetime.now(timezone.utc),
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        return new_user, True


@app.post("/users/register", response_model=UserOut)
def register_user(payload: UserIn):
    user, created = get_or_create_user(payload.telegram_id)
    return UserOut(
        id=user.id,
        telegram_id=user.telegram_id,
        created_at=user.created_at,
        created=created,
    )


@app.get("/users/by-telegram/{telegram_id}", response_model=UserOut)
def get_user(telegram_id: int):
    with SessionLocal() as db:
        stmt = select(User).where(User.telegram_id == telegram_id)
        user = db.execute(stmt).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return UserOut(
            id=user.id,
            telegram_id=user.telegram_id,
            created_at=user.created_at,
            created=False,
        )


@app.get("/health")
def health():
    return {"status": "ok"}
