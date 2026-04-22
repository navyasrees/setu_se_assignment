"""Declarative base for all ORM models. Importing model modules anywhere will
register their tables with this Base's metadata."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
