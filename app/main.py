from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import Base
from app.db.session import engine, get_db

# Importing the models package registers every ORM table on Base.metadata,
# which is what lets create_all() actually create them.
import app.models  # noqa: F401


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Dev-mode table creation. In production, swap this for Alembic migrations.
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "FastAPI project is running"}


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, str]:
    """End-to-end check: if this returns ok, the app can reach the database."""
    db.execute(text("SELECT 1"))
    return {"status": "ok", "database": "reachable"}
