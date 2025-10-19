"""UPSERT query builder."""
import asyncio
import numpy as np
from typing import Dict, List, Union, Any, Optional, Tuple
from veclite.query.ir import UpsertIR
from veclite.query.sqlgen import generate_upsert
from veclite.query.dialect.sqlite import SQLiteDialect
from veclite.core.results import Result


class UpsertBuilder:
    """Fluent builder for UPSERT (INSERT ... ON CONFLICT) queries."""

    def __init__(
        self,
        client,
        schema,
        table: str,
        values: Union[Dict[str, Any], List[Dict[str, Any]]],
        on_conflict: Union[str, List[str]],
        ignore_duplicates: bool = False,
        returning: str = "representation",
        count: Optional[str] = None,
        default_to_null: bool = False,
    ):
        self.db = client
        self.schema = schema
        self.table = table
        self.dialect = SQLiteDialect()
        self.rows = [values] if isinstance(values, dict) else values
        self.do_nothing = ignore_duplicates
        self.returning_all = returning == "representation"

        if self.table in schema.views:
            raise ValueError(f"Cannot UPSERT into view '{table}'.")

        if isinstance(on_conflict, str):
            self.on_conflict_cols = [c.strip() for c in on_conflict.split(",") if c.strip()]
        else:
            self.on_conflict_cols = list(on_conflict)

        tbl = self.schema.get_table(table)
        fields = tbl.get_fields()
        for c in self.on_conflict_cols:
            if c not in fields:
                raise ValueError(f"on_conflict column '{c}' does not exist in table '{table}'")

        if len(self.on_conflict_cols) == 1:
            fd = fields[self.on_conflict_cols[0]]
            if not (fd.primary_key or fd.unique):
                raise ValueError(
                    f"on_conflict column '{self.on_conflict_cols[0]}' must have UNIQUE constraint or be PRIMARY KEY."
                )
        else:
            declared_uniques = [tuple(u) for u in getattr(tbl, '__uniques__', ())]
            if tuple(self.on_conflict_cols) not in declared_uniques:
                raise ValueError(
                    f"on_conflict {tuple(self.on_conflict_cols)} does not match any declared composite UNIQUE."
                )

    def _validate_upsert_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Validate upsert data with smart handling for updates vs inserts

        For primary key upserts (e.g., on_conflict="id"), we:
        - Still validate types and field-level constraints for provided fields
        - Skip the "missing required fields" check since PK upserts are always updates

        For other upserts, we do full validation including required fields.
        """
        if not isinstance(row, dict):
            raise ValueError(f"Upsert data for table '{self.table}' must be a dictionary")

        # Get table class and instantiate it (needed for instance methods)
        table_cls = self.schema.get_table(self.table)
        table_instance = table_cls()
        table_instance._schema = self.schema

        table_fields = table_instance.get_fields()

        # Check if any on_conflict field is a primary key
        is_pk_update = any(
            table_fields.get(col, None) and table_fields[col].primary_key
            for col in self.on_conflict_cols
        )

        validated_data = {}

        # Validate each provided field (type checking, constraints)
        for field_name, value in row.items():
            if field_name in table_fields:
                field_desc = table_fields[field_name]

                # Allow primary key fields if they're in on_conflict
                if field_desc.primary_key and field_name not in self.on_conflict_cols:
                    raise ValueError(
                        f"Cannot manually set primary key field '{field_name}' in table '{self.table}'. "
                        f"Primary keys are auto-generated."
                    )

                # Still do type validation and field-level constraints
                field_desc.set_context(self.table, field_name, self.schema)
                validated_data[field_name] = field_desc.validate(value)
            else:
                raise ValueError(f"Field '{field_name}' not found in table '{self.table}'")

        # Only check for missing required fields if this is a true insert
        # (PK upserts are always updates, so missing fields are fine)
        if not is_pk_update:
            table_instance._validate_required_fields(validated_data, table_fields)

        return validated_data

    def _build_pk_upsert_with_select(
        self,
        row: Dict[str, Any],
        provided_cols: List[str],
        missing_required_cols: set,
        pk_col: str
    ) -> Tuple[str, List[Any]]:
        """Build SELECT-based INSERT for PK upserts with missing required fields.

        When upserting by primary key with partial data, we need to pull
        missing NOT NULL fields from the existing row to avoid constraint violations.
        """
        all_cols = provided_cols + list(missing_required_cols)
        select_parts = []
        params = []

        for col in all_cols:
            if col in provided_cols:
                select_parts.append("?")
                params.append(row[col])
            else:
                select_parts.append(self.dialect.q(col))

        params.append(row[pk_col])

        update_clauses = [
            f"{self.dialect.q(c)} = excluded.{self.dialect.q(c)}"
            for c in provided_cols if c != pk_col
        ]
        action = f"DO UPDATE SET {', '.join(update_clauses)}" if update_clauses else "DO NOTHING"

        sql = (
            f"INSERT INTO {self.dialect.q(self.table)} "
            f"({', '.join(self.dialect.q(c) for c in all_cols)}) "
            f"SELECT {', '.join(select_parts)} "
            f"FROM {self.dialect.q(self.table)} "
            f"WHERE {self.dialect.q(pk_col)} = ? "
            f"ON CONFLICT ({self.dialect.q(pk_col)}) {action}"
        )

        if self.returning_all:
            sql += " RETURNING *"

        return sql, params

    async def execute(self):
        validated_rows = [self._validate_upsert_row(row) for row in self.rows]
        with_defaults = [self.db._apply_runtime_defaults(self.table, row) for row in validated_rows]
        serialized_rows = [self.db._serialize_json_fields(self.table, row) for row in with_defaults]

        if not serialized_rows:
            return Result([])

        table_cls = self.schema.get_table(self.table)
        table_fields = table_cls.get_fields() if table_cls else {}

        is_pk_conflict = any(
            table_fields.get(col, None) and table_fields[col].primary_key
            for col in self.on_conflict_cols
        )

        required_fields = set()
        for field_name, field_desc in table_fields.items():
            if not field_desc.nullable and field_desc.default is None and not field_desc.primary_key:
                required_fields.add(field_name)

        provided_fields = set(serialized_rows[0].keys())
        missing_required = required_fields - provided_fields

        use_select_insert = is_pk_conflict and missing_required and self.returning_all

        cols = list(serialized_rows[0].keys())
        num_cols = len(cols)
        max_vars = self.db._get_max_vars()

        from veclite.query.validation import check_vector_embedder_required
        has_vector_fields = check_vector_embedder_required(
            self.table, self.schema, self.db.embedder, "UPSERT into"
        )

        total_params = num_cols * len(serialized_rows)
        use_executemany = total_params > max_vars and not has_vector_fields and not use_select_insert

        async def _exec_upsert():
            all_results = []

            if use_select_insert:
                for row in serialized_rows:
                    sql, params = self._build_pk_upsert_with_select(
                        row, cols, missing_required, self.on_conflict_cols[0]
                    )
                    rows = await self.db._exec(sql, params)
                    for r in rows:
                        r = self.db._deserialize_json_fields(self.table, r)
                        all_results.append(r)

            elif use_executemany:
                conflict_cols = ", ".join(self.dialect.q(c) for c in self.on_conflict_cols)
                if self.do_nothing:
                    action = "DO NOTHING"
                else:
                    update_clauses = [
                        f"{self.dialect.q(c)} = excluded.{self.dialect.q(c)}"
                        for c in cols if c not in self.on_conflict_cols
                    ]
                    action = f"DO UPDATE SET {', '.join(update_clauses)}" if update_clauses else "DO NOTHING"

                sql = (
                    f"INSERT INTO {self.dialect.q(self.table)} ({', '.join(self.dialect.q(c) for c in cols)}) "
                    f"VALUES ({', '.join(['?'] * num_cols)}) "
                    f"ON CONFLICT ({conflict_cols}) {action}"
                )
                param_rows = [[row[c] for c in cols] for row in serialized_rows]
                with self.db._lock:
                    self.db.conn.executemany(sql, param_rows)
                    self.db.conn.commit()
                return None
            else:
                batch_size = max(1, max_vars // num_cols)
                for i in range(0, len(serialized_rows), batch_size):
                    batch = serialized_rows[i:i + batch_size]
                    ir = UpsertIR(
                        table=self.table,
                        rows=batch,
                        on_conflict=self.on_conflict_cols,
                        do_nothing=self.do_nothing,
                        returning_all=self.returning_all,
                    )
                    sql, params = generate_upsert(ir, self.dialect)
                    rows = await self.db._exec(sql, params)

                    for row in rows:
                        row = self.db._deserialize_json_fields(self.table, row)
                        all_results.append(row)
            return all_results

        all_results = await _exec_upsert()

        if all_results is None:
            return Result([], count=len(serialized_rows))

        await self._embed_vectors(all_results)

        return Result(all_results)

    async def _embed_vectors(self, rows: List[Dict[str, Any]]):
        if not rows or not self.db.embedder:
            return

        table_cls = self.schema.get_table(self.table)
        if not table_cls:
            return

        input_fields = set()
        for row in self.rows:
            input_fields.update(row.keys())

        vector_fields = []
        for field_name, field in table_cls.get_fields().items():
            if getattr(field, 'vector', False) and field_name in input_fields:
                vector_fields.append(field_name)

        if not vector_fields:
            return

        from ...context import emb_queue_var
        queue = emb_queue_var.get()
        in_batch_context = queue is not None

        for field_name in vector_fields:
            field = table_cls.get_fields()[field_name]
            is_contextualized = getattr(field, 'contextualized', False)

            texts = []
            ids = []
            for row in rows:
                if field_name in row and row[field_name]:
                    texts.append(row[field_name])
                    ids.append(row['id'])

            if not texts:
                continue

            if in_batch_context:
                self.db._enqueue_embedding(self.table, field_name, ids, texts)
            else:
                # Use appropriate embedding method
                if is_contextualized:
                    # Contextualized: each text is a separate document
                    # Pass as [[text1], [text2], ...] to preserve document boundaries
                    inputs = [[text] for text in texts]
                    nested_embeddings = await self.db.embedder.contextualized_embed(
                        inputs=inputs,
                        model="voyage-context-3",
                        input_type="document",
                        output_dimension=getattr(field, 'contextualized_dim', 512)
                    )
                    # Flatten nested results: [[emb1], [emb2]] -> [emb1, emb2]
                    embeddings = [doc_embs[0] for doc_embs in nested_embeddings]
                else:
                    # Regular vector field
                    embeddings = await self.db.embedder.embed(texts)

                vectors = [np.array(emb, dtype=np.float32) for emb in embeddings]

                vector_store = self.db.get_or_create_vector_store(self.table, field_name)
                vector_store.add_batch(ids, vectors)
