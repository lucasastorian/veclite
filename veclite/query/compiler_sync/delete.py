"""DELETE query builder - sync version."""
from typing import List, Dict, Any
from veclite.query.compiler.mixins import PredMixin
from veclite.query.ir import DeleteIR
from veclite.query.binder import bind_delete
from veclite.query.sqlgen import generate_delete
from veclite.query.dialect.sqlite import SQLiteDialect
from veclite.core.results import Result


class SyncDeleteBuilder(PredMixin):
    """Fluent builder for DELETE queries - sync version."""

    def __init__(self, client, schema, table: str):
        self.db = client
        self.schema = schema
        self.table = table
        self.dialect = SQLiteDialect()
        self._pred = None

        if self.table in schema.views:
            raise ValueError(f"Cannot DELETE from view '{table}'.")

    def execute(self):
        """Execute the DELETE query."""
        ir = DeleteIR(table=self.table, where=self._pred)
        bound = bind_delete(ir, self.schema)
        sql, params = generate_delete(bound, self.dialect)

        rows = self.db._exec(sql, params)

        self._tombstone_vectors(rows)

        return Result(rows)

    def _tombstone_vectors(self, rows: List[Dict[str, Any]]):
        """Tombstone vectors for deleted rows"""
        if not rows:
            return

        table_cls = self.schema.get_table(self.table)
        if not table_cls:
            return

        vector_fields = []
        for field_name, field in table_cls.get_fields().items():
            if getattr(field, 'vector', False):
                vector_fields.append(field_name)

        if not vector_fields:
            return

        deleted_ids = [row['id'] for row in rows]

        for field_name in vector_fields:
            if (self.table, field_name) in self.db.vector_stores:
                vector_store = self.db.vector_stores[(self.table, field_name)]
                vector_store.tombstone_batch(deleted_ids)
