from pathlib import Path
from typing import Union, List, Dict, Any
from veclite.schema.schema import Schema
from veclite.core.base_client import BaseClient


class Client(BaseClient):
    """Sync SQLite ORM client with vector embeddings and hybrid search.

    Use Client.create() to create a new database or Client.connect() to open existing.
    """

    def _init_embedder(self):
        """Initialize the sync embedder."""
        try:
            from veclite.embeddings.providers.voyage_sync import VoyageClient
            self.embedder = VoyageClient(
                model=self._embedder_model,
                dimensions=self._embedder_dimensions,
                rerank_model=self._embedder_rerank_model,
                cache=self._embedder_cache
            )
        except ImportError:
            # voyageai not installed - optional dependency
            self.embedder = None
        except AssertionError as e:
            # VOYAGE_API_KEY not set - check if schema needs it
            has_vector_fields = self._schema_has_vector_fields()

            if has_vector_fields:
                # Schema has vector fields - FAIL IMMEDIATELY
                raise AssertionError(
                    f"VOYAGE_API_KEY environment variable must be set.\n\n"
                    f"Your schema has vector fields but VOYAGE_API_KEY is not set.\n"
                    f"Get your API key from https://www.voyageai.com/\n\n"
                    f"Either:\n"
                    f"  1. Set VOYAGE_API_KEY environment variable, or\n"
                    f"  2. Manually set client.embedder after creation"
                )
            else:
                # No vector fields - embedder not needed
                self.embedder = None

    def _schema_has_vector_fields(self) -> bool:
        """Check if any table in the schema has vector-enabled fields."""
        for table_name in self.schema.tables:
            table_cls = self.schema.get_table(table_name)
            for field_name, field in table_cls.get_fields().items():
                if getattr(field, 'vector', False):
                    return True
        return False

    def table(self, name: str) -> "SyncTableQueryBuilder":
        """Return a sync query builder bound to a table or view."""
        from veclite.query.builder_sync import SyncTableQueryBuilder

        if name in self.schema.views:
            pass
        else:
            self.schema.get_table(name)

        return SyncTableQueryBuilder(self, self.schema, name)

    def _exec(self, sql: str, params: Union[List, tuple] = ()) -> List[Dict[str, Any]]:
        """Execute SQL with error handling and return rows as dicts - sync version."""
        import time
        import os
        t0 = time.time()
        with self._lock:
            rows = self._exec_unsafe(sql, params)
            self.conn.commit()
        elapsed_ms = (time.time() - t0) * 1000
        threshold = int(os.getenv("INTELLIFIN_SLOW_SQL_MS", "2000"))
        if elapsed_ms > threshold:
            import logging
            logging.warning(f"Slow SQL ({elapsed_ms:.0f}ms): {sql[:200]}")
        return rows

    @classmethod
    def create(
        cls,
        schema: Schema,
        path: str,
        *,
        exist_ok: bool = False,
        embedder_model: str = "voyage-3.5-lite",
        embedder_dimensions: int = 512,
        embedder_rerank_model: str = "rerank-2.5",
        embedder_cache: bool = True,
    ) -> "Client":
        """Create a new database with the given schema.

        Args:
            schema: Database schema to create
            path: Path to new database file
            exist_ok: If True, don't fail if database already exists
            embedder_model: Voyage AI model name (default: "voyage-3.5-lite")
            embedder_dimensions: Embedding dimensions (default: 512)
            embedder_rerank_model: Reranking model name (default: "rerank-2.5")
            embedder_cache: Whether to cache embeddings (default: True)

        Returns:
            Client instance

        Raises:
            FileExistsError: If database already exists and exist_ok=False
        """
        path_obj = Path(path)

        storage_dir = path_obj
        if storage_dir.exists() and not storage_dir.is_dir():
            raise FileExistsError(f"Path exists and is not a directory: {storage_dir}")
        storage_dir.mkdir(parents=True, exist_ok=True)
        db_path = storage_dir / "sqlite.db"
        if db_path.exists() and not exist_ok:
            raise FileExistsError(
                f"Database already exists at {db_path}. Use exist_ok=True or choose a new folder."
            )

        return cls(
            schema=schema,
            base_path=str(db_path),
            _auto_provision=True,
            embedder_model=embedder_model,
            embedder_dimensions=embedder_dimensions,
            embedder_rerank_model=embedder_rerank_model,
            embedder_cache=embedder_cache,
        )

    @classmethod
    def connect(
        cls,
        schema: Schema,
        path: str,
        *,
        auto_migrate: bool = False,
        embedder_model: str = "voyage-3.5-lite",
        embedder_dimensions: int = 512,
        embedder_rerank_model: str = "rerank-2.5",
        embedder_cache: bool = True,
    ) -> "Client":
        """Connect to an existing database.

        Args:
            schema: Database schema (must match existing schema)
            path: Path to existing database file
            auto_migrate: If True, auto-add new nullable columns when schema changes (default: False)
            embedder_model: Voyage AI model name (default: "voyage-3.5-lite")
            embedder_dimensions: Embedding dimensions (default: 512)
            embedder_rerank_model: Reranking model name (default: "rerank-2.5")
            embedder_cache: Whether to cache embeddings (default: True)

        Returns:
            Client instance

        Raises:
            FileNotFoundError: If database does not exist
            ValueError: If schema mismatch and auto_migrate=False
        """
        path_obj = Path(path)

        storage_dir = path_obj
        if not storage_dir.exists():
            # Preserve expected error phrasing from tests
            raise FileNotFoundError(
                f"Database does not exist at {path}. "
                f"Use Client.create() to create a new database."
            )
        db_path = storage_dir / "sqlite.db"
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database does not exist at {path}. Use Client.create() to create a new database."
            )

        client = cls(
            schema=schema,
            base_path=str(db_path),
            _auto_provision=False,
            embedder_model=embedder_model,
            embedder_dimensions=embedder_dimensions,
            embedder_rerank_model=embedder_rerank_model,
            embedder_cache=embedder_cache,
        )

        client._ensure_metadata_table()
        client._validate_schema(auto_migrate=auto_migrate)

        return client
