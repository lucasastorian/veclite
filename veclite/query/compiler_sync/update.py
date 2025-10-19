"""UPDATE query builder - sync version."""
import numpy as np
from typing import Dict, Any, List
from veclite.query.compiler.mixins import PredMixin
from veclite.query.ir import UpdateIR
from veclite.query.binder import bind_update
from veclite.query.sqlgen import generate_update
from veclite.query.dialect.sqlite import SQLiteDialect
from veclite.core.results import Result


class SyncUpdateBuilder(PredMixin):
    """Fluent builder for UPDATE queries - sync version."""

    def __init__(self, client, schema, table: str, data: Dict[str, Any]):
        self.db = client
        self.schema = schema
        self.table = table
        self.dialect = SQLiteDialect()
        self.data = data
        self._pred = None

        if self.table in schema.views:
            raise ValueError(f"Cannot UPDATE view '{table}'.")

    def execute(self):
        """Execute the UPDATE query."""
        data_with_auto = self.db._apply_auto_update(self.table, self.data)
        validated_data = self.db._validate_update_data(self.table, data_with_auto)
        serialized_data = self.db._serialize_json_fields(self.table, validated_data)

        from veclite.query.validation import check_vector_embedder_required
        check_vector_embedder_required(
            self.table, self.schema, self.db.embedder, "UPDATE"
        )

        ir = UpdateIR(table=self.table, assign=serialized_data, where=self._pred)
        bound = bind_update(ir, self.schema)
        sql, params = generate_update(bound, self.dialect)

        rows = self.db._exec(sql, params)

        processed = []
        for row in rows:
            row = self.db._deserialize_json_fields(self.table, row)
            processed.append(row)

        self._update_vectors(processed)

        return Result(processed)

    def _update_vectors(self, rows: List[Dict[str, Any]]):
        """Update vectors for any vector-enabled fields that were modified"""
        if not rows or not self.db.embedder:
            return

        table_cls = self.schema.get_table(self.table)
        if not table_cls:
            return

        vector_fields = []
        for field_name, field in table_cls.get_fields().items():
            if getattr(field, 'vector', False) and field_name in self.data:
                vector_fields.append(field_name)

        if not vector_fields:
            return

        for field_name in vector_fields:
            texts = []
            ids = []
            for row in rows:
                if field_name in row and row[field_name]:
                    texts.append(row[field_name])
                    ids.append(row['id'])

            if not texts:
                continue

            vector_store = self.db.get_or_create_vector_store(self.table, field_name)

            existing_ids = [id_ for id_ in ids if vector_store.has_id(id_)]
            if existing_ids:
                vector_store.tombstone_batch(existing_ids)

            embeddings = self.db.embedder.embed(texts)
            vectors = [np.array(emb, dtype=np.float32) for emb in embeddings]

            vector_store.add_batch(ids, vectors)

            if existing_ids:
                vector_store.tombstones.difference_update(existing_ids)
                vector_store._save_tombstones()
