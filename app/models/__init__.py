"""Importing this package makes sure every model is registered on Base.metadata
before we call create_all()."""

from app.models.merchant import Merchant  # noqa: F401
from app.models.transaction import Transaction, TransactionStatus  # noqa: F401
from app.models.event import Event, EventType  # noqa: F401
