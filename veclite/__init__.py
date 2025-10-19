"""
VecLite: A schema-first SQLite ORM with built-in vector embeddings and hybrid search.
"""

from veclite.__version__ import __version__

from veclite.core.async_client import AsyncClient
from veclite.core.client import Client
from veclite.core.errors import (
    DatabaseError,
    ConstraintError,
    ForeignKeyError,
    UniqueConstraintError,
    NotNullViolation,
    CheckConstraintError,
)
from veclite.schema.schema import Schema
from veclite.schema.table import Table
from veclite.schema.view import View
from veclite.schema.fields import (
    FieldDescriptor,
    Serial,
    Text,
    Integer,
    Float,
    Boolean,
    JSONField,
    Date,
    Timestamp,
    Enum,
)

__all__ = [
    "__version__",
    "Client",
    "AsyncClient",
    "Schema",
    "Table",
    "View",
    # Field types
    "FieldDescriptor",
    "Serial",
    "Text",
    "Integer",
    "Float",
    "Boolean",
    "JSONField",
    "Date",
    "Timestamp",
    "Enum",
    # Errors
    "DatabaseError",
    "ConstraintError",
    "ForeignKeyError",
    "UniqueConstraintError",
    "NotNullViolation",
    "CheckConstraintError",
]
