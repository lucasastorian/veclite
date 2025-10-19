"""Shared validation helpers for query compilers."""

from typing import Dict, Any
from veclite.schema.schema import Schema


def check_vector_embedder_required(table_name: str, schema: Schema, embedder, operation: str = "operate on"):
    """Check if embedder is required for vector fields and raise if missing."""
    from veclite.core.errors import DatabaseError

    table_cls = schema.get_table(table_name)
    if not table_cls:
        return False

    has_vector_fields = any(
        getattr(field, 'vector', False)
        for field in table_cls.get_fields().values()
    )

    if has_vector_fields and not embedder:
        raise DatabaseError(
            f"Cannot {operation} table '{table_name}' with vector fields without an embedder. "
            f"Set VOYAGE_API_KEY environment variable."
        )

    return has_vector_fields


def get_vector_fields(table_name: str, schema: Schema, filter_fields: set = None):
    """Get list of vector-enabled field names from table schema."""
    table_cls = schema.get_table(table_name)
    if not table_cls:
        return []

    vector_fields = []
    for field_name, field in table_cls.get_fields().items():
        if getattr(field, 'vector', False):
            if filter_fields is None or field_name in filter_fields:
                vector_fields.append(field_name)

    return vector_fields
