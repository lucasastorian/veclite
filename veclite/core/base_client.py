import re
import json
import sqlite3
import threading
import numpy as np
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import Union, List, Dict, Any, Tuple, Optional
from veclite.schema.schema import Schema
from veclite.vector.store import VectorStore
import hashlib
from veclite.core.context import emb_queue_var
from veclite.core.errors import (
    DatabaseError, ConstraintError, ForeignKeyError,
    UniqueConstraintError, NotNullViolation, CheckConstraintError
)


class BaseClient(ABC):
    """Abstract base class for SQLite ORM clients with vector embeddings and hybrid search.

    This class contains all shared logic between sync and async clients. Subclasses must
    implement abstract methods for embedder initialization and query builder creation.
    """

    def __init__(
        self,
        schema: Schema,
        base_path: str,
        *,
        _auto_provision: bool = False,
        embedder_model: str = "voyage-3.5-lite",
        embedder_dimensions: int = 512,
        embedder_rerank_model: str = "rerank-2.5",
        embedder_cache: bool = True,
    ):
        """Initialize BaseClient (internal - use subclass.create() or subclass.connect() instead).

        Args:
            schema: Database schema
            base_path: Path to SQLite database file
            _auto_provision: If True, create tables if database is empty (internal use only)
            embedder_model: Voyage AI model name
            embedder_dimensions: Embedding dimensions
            embedder_rerank_model: Reranking model name
            embedder_cache: Whether to cache embeddings (default: True)
        """
        if not base_path or base_path == ":memory:":
            raise ValueError("Provide a filesystem path for SQLite (no in-memory DBs).")

        self.schema = schema
        self.base_path = base_path
        self._lock = threading.RLock()
        self._closed = False
        self._sp_counter = 0

        self._embedder_model = embedder_model
        self._embedder_dimensions = embedder_dimensions
        self._embedder_rerank_model = embedder_rerank_model
        self._embedder_cache = embedder_cache

        self.conn = sqlite3.connect(self.base_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._max_vars = None

        version_str = self.conn.execute('SELECT sqlite_version()').fetchone()[0]
        version_tuple = tuple(int(x) for x in version_str.split('.'))
        if version_tuple < (3, 35, 0):
            raise RuntimeError(
                f"SQLite version {version_str} is too old. "
                f"This database requires SQLite ≥3.35.0 for RETURNING clause support."
            )

        try:
            self.conn.execute("SELECT json_valid('[]')").fetchone()
        except sqlite3.OperationalError as e:
            raise RuntimeError("SQLite was built without JSON1; required for JSON array contains filters.") from e

        def _sqlite_regexp(pattern, text):
            try:
                return 1 if re.search(pattern, text or "", re.IGNORECASE) else 0
            except re.error:
                return 0
        self.conn.create_function("REGEXP", 2, _sqlite_regexp)

        self._has_bm25 = self._check_bm25_support()

        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")

        if _auto_provision:
            self._provision_schema_if_needed()
        self._ensure_fts_objects()
        self._init_vector_stores()
        self._init_embedder()

    @abstractmethod
    def _init_embedder(self):
        """Initialize the embedder (sync or async).

        Subclasses must implement this to create the appropriate embedder instance.
        """
        pass

    @abstractmethod
    def table(self, name: str):
        """Return a query builder bound to a table or view.

        Subclasses must implement this to return the appropriate query builder type
        (sync or async).

        Args:
            name: Table or view name

        Returns:
            Query builder instance (TableQueryBuilder or AsyncTableQueryBuilder)
        """
        pass

    def close(self):
        with self._lock:
            if self._closed:
                return

            if emb_queue_var.get() is not None:
                import logging
                logging.warning(
                    "Closing database connection with active embedding batch. "
                    "Queued embeddings will be lost."
                )
                try:
                    emb_queue_var.set(None)
                except Exception:
                    pass

            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.conn.commit()
            except Exception:
                pass
            finally:
                if hasattr(self, 'vector_stores'):
                    for vs in self.vector_stores.values():
                        try:
                            vs.close()
                        except Exception:
                            pass

                if hasattr(self, 'embedder') and self.embedder and self.embedder.cache:
                    try:
                        self.embedder.cache.close()
                    except Exception:
                        pass
                self._closed = True
                self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _provision_schema_if_needed(self):
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        existing_tables = [row[0] for row in cursor.fetchall()]

        if not existing_tables:
            create_sql = self.schema.generate_all_sql()
            self.conn.executescript(create_sql)
            self.conn.commit()

        self._ensure_metadata_table()
        self._ensure_vector_outbox_table()
        self._save_schema()

    def _ensure_metadata_table(self):
        """Create metadata table for storing schema and other metadata."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS __veclite_metadata__ (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def _save_schema(self):
        """Save schema and embedder config to metadata table."""
        import json

        # Save schema
        schema_dict = self.schema.to_dict()
        schema_json = json.dumps(schema_dict, sort_keys=True)

        # Save embedder config
        embedder_config = {
            'model': self._embedder_model,
            'dimensions': self._embedder_dimensions,
            'rerank_model': self._embedder_rerank_model,
        }
        embedder_json = json.dumps(embedder_config, sort_keys=True)

        self.conn.execute("""
            INSERT OR REPLACE INTO __veclite_metadata__ (key, value)
            VALUES ('schema', ?), ('embedder_config', ?)
        """, (schema_json, embedder_json))
        self.conn.commit()

    def drop_view(self, view_name: str) -> None:
        """Drop a view from the database and remove from schema.

        Args:
            view_name: Name of the view to drop

        Raises:
            ValueError: If view doesn't exist in schema
        """
        with self._lock:
            # Remove from schema first (will raise if not found)
            self.schema.drop_view(view_name)

            # Drop from database
            self.conn.execute(f"DROP VIEW IF EXISTS `{view_name}`")
            self.conn.commit()

            # Save updated schema
            self._save_schema()

    def _validate_schema(self, auto_migrate: bool = False):
        """Validate that stored schema and embedder config match provided schema.

        Args:
            auto_migrate: If True, auto-add new nullable columns. If False, raise error on mismatch.

        Raises:
            ValueError: If schema mismatch and auto_migrate=False
        """
        import json

        # Validate schema
        cursor = self.conn.execute(
            "SELECT value FROM __veclite_metadata__ WHERE key = 'schema'"
        )
        row = cursor.fetchone()

        if not row:
            self._save_schema()
            return

        stored_schema_json = row[0]
        stored_schema_dict = json.loads(stored_schema_json)
        current_schema_dict = self.schema.to_dict()

        if stored_schema_dict != current_schema_dict:
            if not auto_migrate:
                diff = self._compute_schema_diff(stored_schema_dict, current_schema_dict)
                raise ValueError(
                    f"Schema mismatch detected!\n\n"
                    f"The database schema does not match the provided Schema object.\n"
                    f"This likely means the schema was modified externally or the Python Schema changed.\n\n"
                    f"Differences:\n{diff}\n\n"
                    f"Options:\n"
                    f"1. Use auto_migrate=True on connect() to auto-add new nullable columns\n"
                    f"2. Manually migrate the database to match the schema\n"
                    f"3. Update your Python Schema to match the database"
                )

            self._auto_migrate_schema(stored_schema_dict, current_schema_dict)
            self._save_schema()

        # Validate embedder config (warn if mismatch, don't fail)
        cursor = self.conn.execute(
            "SELECT value FROM __veclite_metadata__ WHERE key = 'embedder_config'"
        )
        row = cursor.fetchone()

        if row:
            stored_embedder_json = row[0]
            stored_embedder_config = json.loads(stored_embedder_json)

            current_embedder_config = {
                'model': self._embedder_model,
                'dimensions': self._embedder_dimensions,
                'rerank_model': self._embedder_rerank_model,
            }

            if stored_embedder_config != current_embedder_config:
                import logging
                logging.warning(
                    f"Embedder config mismatch!\n"
                    f"  Stored: model={stored_embedder_config['model']}, "
                    f"dim={stored_embedder_config['dimensions']}\n"
                    f"  Current: model={current_embedder_config['model']}, "
                    f"dim={current_embedder_config['dimensions']}\n"
                    f"Using current config. This may cause issues if vector stores were created with different dimensions."
                )

    def _compute_schema_diff(self, stored: dict, current: dict) -> str:
        """Compute human-readable diff between schemas."""
        diffs = []

        stored_tables = set(stored.get('tables', {}).keys())
        current_tables = set(current.get('tables', {}).keys())

        added_tables = current_tables - stored_tables
        removed_tables = stored_tables - current_tables

        if added_tables:
            diffs.append(f"  + New tables: {', '.join(sorted(added_tables))}")
        if removed_tables:
            diffs.append(f"  - Removed tables: {', '.join(sorted(removed_tables))}")

        for table_name in stored_tables & current_tables:
            stored_fields = stored['tables'][table_name]['fields']
            current_fields = current['tables'][table_name]['fields']

            stored_field_names = set(stored_fields.keys())
            current_field_names = set(current_fields.keys())

            added_fields = current_field_names - stored_field_names
            removed_fields = stored_field_names - current_field_names

            if added_fields:
                diffs.append(f"  + Table '{table_name}': new columns {', '.join(sorted(added_fields))}")
            if removed_fields:
                diffs.append(f"  - Table '{table_name}': removed columns {', '.join(sorted(removed_fields))}")

            for field_name in stored_field_names & current_field_names:
                if stored_fields[field_name] != current_fields[field_name]:
                    diffs.append(f"  ~ Table '{table_name}', column '{field_name}': definition changed")

        return "\n".join(diffs) if diffs else "  (No differences - this is a bug!)"

    def _auto_migrate_schema(self, stored: dict, current: dict):
        """Auto-migrate schema by adding new nullable columns."""
        from veclite.schema.fields import FieldDescriptor

        for table_name in current.get('tables', {}).keys():
            if table_name not in stored.get('tables', {}):
                raise ValueError(
                    f"Cannot auto-migrate: new table '{table_name}' detected. "
                    f"Auto-migration only supports adding nullable columns to existing tables."
                )

            stored_fields = stored['tables'][table_name]['fields']
            current_fields = current['tables'][table_name]['fields']

            for field_name, field_data in current_fields.items():
                if field_name not in stored_fields:
                    if not field_data.get('nullable', True) and field_data.get('default') is None:
                        raise ValueError(
                            f"Cannot auto-migrate: new NOT NULL column '{table_name}.{field_name}' without default. "
                            f"Auto-migration only supports adding nullable columns or columns with defaults."
                        )

                    field = FieldDescriptor.from_dict(field_data)
                    sql_def = field.to_sql(field_name)

                    self.conn.execute(f"ALTER TABLE `{table_name}` ADD COLUMN {sql_def}")
                    self.conn.commit()

                    print(f"Auto-migrated: Added column '{table_name}.{field_name}'")

    def _ensure_vector_outbox_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS vector_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                row_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                text_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_vector_outbox
            ON vector_outbox(table_name, column_name, row_id, text_sha256)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_vector_outbox_created_at
            ON vector_outbox(created_at)
        """)
        self.conn.commit()

    def _ensure_fts_objects(self):
        for table_name, table_cls in self.schema.tables.items():
            fts_sql = table_cls._generate_fts_sql()
            if not fts_sql:
                continue
            self.conn.executescript(fts_sql)
        self.conn.commit()

    def _check_bm25_support(self) -> bool:
        try:
            self.conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _bm25_test USING fts5(content);")
            self.conn.execute("INSERT INTO _bm25_test(rowid, content) VALUES (1, 'test');")
            self.conn.execute("SELECT bm25(_bm25_test) FROM _bm25_test WHERE _bm25_test MATCH 'test';").fetchone()
            self.conn.execute("DROP TABLE _bm25_test;")
            self.conn.commit()
            return True
        except sqlite3.OperationalError:
            try:
                self.conn.execute("DROP TABLE IF EXISTS _bm25_test;")
                self.conn.commit()
            except Exception:
                pass
            return False

    def _init_vector_stores(self):
        self.vector_stores = {}

    def get_or_create_vector_store(self, table: str, column: str) -> VectorStore:
        if table not in self.schema.tables:
            raise ValueError(f"Table '{table}' not found in schema")

        table_cls = self.schema.tables[table]
        fields = table_cls.get_fields()

        if column not in fields:
            raise ValueError(f"Column '{column}' not found in table '{table}'")

        field = fields[column]
        if not getattr(field, 'vector', False):
            raise ValueError(
                f"Column '{table}.{column}' does not have vector=True in schema. "
                f"Cannot create vector store for non-vector column."
            )

        # Get dimensions from field config
        if field.contextualized:
            dim = field.contextualized_dim
        elif field.vector_config:
            dim = field.vector_config.dimensions
        else:
            dim = 512  # fallback default

        key = (table, column)
        if key not in self.vector_stores:
            # Nested storage: keep vectors inside the same folder as the SQLite DB
            base_dir = Path(self.base_path).parent
            vector_dir = base_dir / 'vectors'
            store_path = vector_dir / f'{table}__{column}'
            self.vector_stores[key] = VectorStore(str(store_path), dim=dim)
        return self.vector_stores[key]

    def _now_iso(self) -> str:
        """Return current UTC timestamp in ISO 8601 format."""
        return datetime.now(tz=timezone.utc).isoformat()

    def _apply_runtime_defaults(self, table: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fill in CURRENT_TIMESTAMP defaults for missing fields at runtime."""
        tbl = self.schema.get_table(table)
        out = dict(item)
        for fname, fdesc in tbl.get_fields().items():
            if fname not in out:
                if isinstance(fdesc.default, str) and fdesc.default.upper() == "CURRENT_TIMESTAMP":
                    out[fname] = self._now_iso()
        return out

    def _apply_auto_update(self, table: str, update: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure auto_update fields are set on each UPDATE."""
        tbl = self.schema.get_table(table)
        out = dict(update)
        for fname, fdesc in tbl.get_fields().items():
            if getattr(fdesc, "auto_update", False):
                out[fname] = self._now_iso()
        return out

    def _get_max_vars(self) -> int:
        """Detect SQLite's maximum host parameter limit."""
        if self._max_vars is not None:
            return self._max_vars

        try:
            row = self.conn.execute("PRAGMA compile_options").fetchall()
            for (opt,) in row:
                if opt.startswith("MAX_VARIABLE_NUMBER="):
                    self._max_vars = int(opt.split("=")[1])
                    return self._max_vars
        except sqlite3.Error:
            pass

        self._max_vars = 999
        return self._max_vars

    def _exec_unsafe(self, sql: str, params: Union[List, tuple] = ()) -> List[Dict[str, Any]]:
        """Execute SQL without lock - caller must hold lock. Internal use only."""
        if self._closed:
            raise DatabaseError("Cannot execute query on closed database connection")
        try:
            cursor = self.conn.execute(sql, params)
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
                return rows
            return []
        except sqlite3.IntegrityError as e:
            self.conn.rollback()
            error_name = getattr(e, "sqlite_errorname", "")
            msg = str(e)

            if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY" or "FOREIGN KEY constraint failed" in msg:
                raise ForeignKeyError(msg) from e
            if error_name == "SQLITE_CONSTRAINT_UNIQUE" or "UNIQUE constraint failed" in msg:
                raise UniqueConstraintError(msg) from e
            if error_name == "SQLITE_CONSTRAINT_NOTNULL" or "NOT NULL constraint failed" in msg:
                raise NotNullViolation(msg) from e
            if error_name == "SQLITE_CONSTRAINT_CHECK" or "CHECK constraint failed" in msg:
                raise CheckConstraintError(msg) from e
            raise ConstraintError(msg) from e
        except sqlite3.Error as e:
            self.conn.rollback()
            raise DatabaseError(str(e)) from e

    def fetch_rows_by_primary_key(self, table_name: str, ids: List[Union[int, str]]) -> Dict[Union[int, str], Dict[str, Any]]:
        """Fetch rows by primary key IDs."""
        if not ids:
            return {}

        table_cls = self.schema.get_table(table_name)
        primary_key_field = None
        for field_name, field_desc in table_cls.get_fields().items():
            if field_desc.primary_key:
                primary_key_field = field_name
                break

        if not primary_key_field:
            raise ValueError(f"No primary key field found for table '{table_name}'")

        placeholders = ",".join(["?" for _ in ids])
        sql = f"SELECT * FROM `{table_name}` WHERE `{primary_key_field}` IN ({placeholders})"

        with self._lock:
            rows = self._exec_unsafe(sql, ids)

        result = {}
        for row_dict in rows:
            row_dict = self._deserialize_json_fields(table_name, row_dict)
            primary_key_value = row_dict[primary_key_field]
            result[primary_key_value] = row_dict

        return result

    def _serialize_json_fields(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize JSON fields to strings for SQLite storage."""
        from veclite.schema.fields import JSONField

        if table in self.schema.views:
            vcls = self.schema.views[table]
            tmap = vcls.type_map(self.schema)
            out = data.copy()
            for alias, val in list(out.items()):
                fd = tmap.get(alias)
                if fd is not None and isinstance(fd, JSONField):
                    if val is not None and not isinstance(val, str):
                        try:
                            out[alias] = json.dumps(val)
                        except Exception:
                            out[alias] = str(val)
            return out

        table_obj = self.schema.get_table(table)
        if not table_obj:
            return data

        fields = table_obj.get_fields()
        serialized_data = data.copy()

        for field_name, field_descriptor in fields.items():
            if field_name in data and field_descriptor.sql_type == 'TEXT':
                if isinstance(field_descriptor, JSONField):
                    value = data[field_name]
                    if value is not None and not isinstance(value, str):
                        try:
                            serialized_data[field_name] = json.dumps(value)
                        except (TypeError, ValueError):
                            serialized_data[field_name] = str(value)

        return serialized_data

    def _deserialize_json_fields(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize JSON fields from strings back to Python objects."""
        from veclite.schema.fields import JSONField

        if table in self.schema.views:
            vcls = self.schema.views[table]
            tmap = vcls.type_map(self.schema)
            out = data.copy()
            for alias, fd in tmap.items():
                if alias in out and isinstance(fd, JSONField):
                    v = out[alias]
                    if v is not None and isinstance(v, str):
                        try:
                            out[alias] = json.loads(v)
                        except Exception:
                            pass
            return out

        table_obj = self.schema.get_table(table)
        if not table_obj:
            return data

        fields = table_obj.get_fields()
        deserialized_data = data.copy()

        for field_name, field_descriptor in fields.items():
            if field_name in data and field_descriptor.sql_type == 'TEXT':
                if isinstance(field_descriptor, JSONField):
                    value = data[field_name]
                    if value is not None and isinstance(value, str):
                        try:
                            deserialized_data[field_name] = json.loads(value)
                        except (json.JSONDecodeError, TypeError):
                            pass

        return deserialized_data

    def _validate_insert_data(self, table: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Validate insert data against schema."""
        return self.schema.validate_insert_data(table, data)

    def _validate_update_data(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate update data against schema."""
        return self.schema.validate_update_data(table, data)
