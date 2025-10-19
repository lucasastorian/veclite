"""Core client and database functionality."""

from veclite.core.errors import (
    DatabaseError,
    ConstraintError,
    ForeignKeyError,
    UniqueConstraintError,
    NotNullViolation,
    CheckConstraintError,
)

__all__ = [
    "DatabaseError",
    "ConstraintError",
    "ForeignKeyError",
    "UniqueConstraintError",
    "NotNullViolation",
    "CheckConstraintError",
]
