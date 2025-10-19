import asyncio
import numpy as np
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Union, List, Dict, Any, Tuple
from veclite.schema.schema import Schema
from veclite.core.base_client import BaseClient
from veclite.core.context import emb_queue_var, emb_atomic_var
from veclite.core.errors import DatabaseError


class AsyncClient(BaseClient):
    """Async SQLite ORM client with vector embeddings and hybrid search.

    Use AsyncClient.create() to create a new database or AsyncClient.connect() to open existing.
    """

    def _init_embedder(self):
        """Initialize the async embedder."""
        try:
            from veclite.embeddings.providers.voyage_async import AsyncVoyageClient
            self.embedder = AsyncVoyageClient(
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

    def table(self, name: str) -> "TableQueryBuilder":
        """Return an async query builder bound to a table or view."""
        from veclite.query.builder import TableQueryBuilder

        if name in self.schema.views:
            pass
        else:
            self.schema.get_table(name)

        return TableQueryBuilder(self, self.schema, name)

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
    ) -> "AsyncClient":
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
            AsyncClient instance

        Raises:
            FileExistsError: If database already exists and exist_ok=False
        """
        path_obj = Path(path)

        # Treat 'path' as a directory that contains all DB artifacts (sqlite, WAL, vectors)
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
    ) -> "AsyncClient":
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
            AsyncClient instance

        Raises:
            FileNotFoundError: If database does not exist
            ValueError: If schema mismatch and auto_migrate=False
        """
        path_obj = Path(path)

        storage_dir = path_obj
        if not storage_dir.exists():
            raise FileNotFoundError(
                f"Database does not exist at {path}. "
                f"Use AsyncClient.create() to create a new database."
            )
        db_path = storage_dir / "sqlite.db"
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database does not exist at {path}. Use AsyncClient.create() to create a new database."
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

    @asynccontextmanager
    async def batch_embeddings(self, *, atomic: bool = True):
        """Batch all embedding generation within this context.

        Defers embedding generation and vector store writes until context exit,
        allowing hundreds/thousands of texts to be embedded in a single Voyage API call.

        SQL operations run normally (no transaction management). Only embeddings are batched.

        ASYNCIO SAFETY:
        ✓ Safe with: await, asyncio.as_completed(), asyncio.gather(), asyncio.create_task()
        ✓ ContextVars propagate to child tasks - all share the same embedding queue

        Without this context:
        - Each upsert with vector fields generates embeddings immediately
        - Example: 100 filings → 100+ Voyage API calls

        With this context:
        - All upserts queue embeddings, flush at exit
        - Example: 100 filings → ~8 Voyage API calls (batches of 128)

        Example:
            # Batch embeddings across all filings
            async with db.batch_embeddings():
                tasks = [filing.upsert() for filing in filings]
                for coro in asyncio.as_completed(tasks):
                    await coro
            # Exit: generate all embeddings in minimal API calls

        Failed embeddings are written to the outbox for later retry via
        flush_vector_outbox().
        """
        if self._closed:
            raise DatabaseError("Cannot start embedding batch on closed database connection")

        queue = {}
        qtoken = emb_queue_var.set(queue)
        atoken = emb_atomic_var.set(bool(atomic))

        # Start a transaction if atomic batching requested
        if atomic:
            with self._lock:
                self.conn.execute("BEGIN")

        try:
            yield

            failures = await self._flush_embedding_queue(queue, atomic=atomic)

            if failures:
                if atomic:
                    # Rollback DB; vectors weren't written in atomic mode
                    with self._lock:
                        self.conn.rollback()
                else:
                    await self._write_failed_groups_to_outbox(failures)
                    # Non-atomic mode: DB already committed per statement
                return

            # All embeddings succeeded
            if atomic:
                with self._lock:
                    self.conn.commit()

        except asyncio.CancelledError:
            try:
                pending = []
                for (table, column), documents in (queue or {}).items():
                    if not documents:
                        continue
                    ids = [row_id for doc in documents for row_id in doc["ids"]]
                    texts = [text for doc in documents for text in doc["texts"]]
                    if ids and texts:
                        pending.append((table, column, ids, texts))
                if pending:
                    await self._write_failed_groups_to_outbox(pending)
            finally:
                if atomic:
                    with self._lock:
                        self.conn.rollback()
                emb_queue_var.reset(qtoken)
                emb_atomic_var.reset(atoken)
            raise
        except Exception:
            try:
                pending = []
                for (table, column), documents in (queue or {}).items():
                    if not documents:
                        continue
                    ids = [row_id for doc in documents for row_id in doc["ids"]]
                    texts = [text for doc in documents for text in doc["texts"]]
                    if ids and texts:
                        pending.append((table, column, ids, texts))
                if pending:
                    await self._write_failed_groups_to_outbox(pending)
            finally:
                if atomic:
                    with self._lock:
                        self.conn.rollback()
                emb_queue_var.reset(qtoken)
                emb_atomic_var.reset(atoken)
            raise
        finally:
            try:
                emb_queue_var.reset(qtoken)
                emb_atomic_var.reset(atoken)
            except Exception:
                pass

    async def _flush_embedding_queue(self, queue: Dict[Tuple[str, str], List[Dict[str, List]]], *, atomic: bool = False):
        """Embed and store all queued texts, choosing API based on schema.

        For contextualized fields: uses voyage-context-3 with preserved document boundaries
        For standard fields: flattens and uses standard embedding API

        Args:
            queue: Maps (table, column) -> List of documents, where each document is {"ids": [...], "texts": [...]}

        Returns:
            List of failed groups for outbox retry
        """
        if not queue:
            return []

        if not self.embedder:
            raise DatabaseError(
                "Cannot generate embeddings without an embedder. "
                "Set VOYAGE_API_KEY environment variable or avoid using vector fields."
            )

        import logging

        failed_groups = []

        if atomic:
            # Two-phase: compute embeddings for all groups first; only write vectors if all succeed
            staged: List[Tuple[str, str, List[int], List[np.ndarray]]] = []
            for (table, column), documents in queue.items():
                if not documents:
                    continue

                field = self.schema.get_table(table).get_fields()[column]
                is_contextualized = getattr(field, 'contextualized', False)

                try:
                    if is_contextualized:
                        inputs = [doc["texts"] for doc in documents]
                        total_chunks = sum(len(doc["texts"]) for doc in documents)

                        logging.debug(
                            f"Contextualized embed {table}.{column}: "
                            f"{len(documents)} documents, {total_chunks} chunks (atomic)"
                        )

                        nested_embeddings = await self.embedder.contextualized_embed(
                            inputs=inputs,
                            model="voyage-context-3",
                            input_type="document",
                            output_dimension=self.embedder.dimensions
                        )

                        embeddings = [emb for doc_embs in nested_embeddings for emb in doc_embs]
                    else:
                        flat_texts = [text for doc in documents for text in doc["texts"]]

                        logging.debug(
                            f"Standard embed {table}.{column}: "
                            f"{len(documents)} documents, {len(flat_texts)} chunks (atomic)"
                        )

                        embeddings = await self.embedder.embed(flat_texts)

                    all_ids = [row_id for doc in documents for row_id in doc["ids"]]
                    vectors = [np.array(emb, dtype=np.float32) for emb in embeddings]

                    if len(vectors) != len(all_ids):
                        raise ValueError(
                            f"Embedding count mismatch: got {len(vectors)} embeddings "
                            f"for {len(all_ids)} IDs in {table}.{column}"
                        )

                    staged.append((table, column, all_ids, vectors))

                except Exception as e:
                    logging.error(f"Embedding failed for {table}.{column} (atomic): {e}")
                    # Do not write any vectors in atomic mode; return as failure
                    failed_groups.append((table, column, [row_id for doc in documents for row_id in doc["ids"]],
                                          [text for doc in documents for text in doc["texts"]]))
                    break

            if failed_groups:
                return failed_groups

            # All embeddings succeeded → write vectors
            for table, column, all_ids, vectors in staged:
                vs = self.get_or_create_vector_store(table, column)
                with self._lock:
                    vs.add_batch(all_ids, vectors)

            return []

        # Non-atomic: write per group and collect failures into outbox
        for (table, column), documents in queue.items():
            if not documents:
                continue

            field = self.schema.get_table(table).get_fields()[column]
            is_contextualized = getattr(field, 'contextualized', False)

            try:
                if is_contextualized:
                    inputs = [doc["texts"] for doc in documents]
                    total_chunks = sum(len(doc["texts"]) for doc in documents)

                    logging.debug(
                        f"Contextualized embed {table}.{column}: "
                        f"{len(documents)} documents, {total_chunks} chunks"
                    )

                    nested_embeddings = await self.embedder.contextualized_embed(
                        inputs=inputs,
                        model="voyage-context-3",
                        input_type="document",
                        output_dimension=self.embedder.dimensions
                    )

                    embeddings = [emb for doc_embs in nested_embeddings for emb in doc_embs]

                else:
                    flat_texts = [text for doc in documents for text in doc["texts"]]

                    logging.debug(
                        f"Standard embed {table}.{column}: "
                        f"{len(documents)} documents, {len(flat_texts)} chunks"
                    )

                    embeddings = await self.embedder.embed(flat_texts)

                all_ids = [row_id for doc in documents for row_id in doc["ids"]]
                vectors = [np.array(emb, dtype=np.float32) for emb in embeddings]

                if len(vectors) != len(all_ids):
                    raise ValueError(
                        f"Embedding count mismatch: got {len(vectors)} embeddings "
                        f"for {len(all_ids)} IDs in {table}.{column}"
                    )

                vs = self.get_or_create_vector_store(table, column)
                with self._lock:
                    vs.add_batch(all_ids, vectors)

            except Exception as e:
                logging.error(f"Embedding failed for {table}.{column}: {e}")
                all_ids = [row_id for doc in documents for row_id in doc["ids"]]
                all_texts = [text for doc in documents for text in doc["texts"]]
                failed_groups.append((table, column, all_ids, all_texts))

        return failed_groups

    async def _write_failed_groups_to_outbox(self, failed_groups: List[Tuple[str, str, List, List]]):
        """Write failed embedding groups to outbox table for later retry.

        Uses SHA256 hash for idempotent inserts (INSERT OR IGNORE prevents duplicates).

        Args:
            failed_groups: List of tuples (table, column, ids, texts)
        """
        if not failed_groups:
            return

        import logging
        import hashlib
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        outbox_rows = []

        for table, column, ids, texts in failed_groups:
            for row_id, text in zip(ids, texts):
                text_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
                outbox_rows.append((table, column, row_id, text, text_sha256, now))

        if outbox_rows:
            try:
                with self._lock:
                    self.conn.executemany(
                        "INSERT OR IGNORE INTO vector_outbox "
                        "(table_name, column_name, row_id, text, text_sha256, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        outbox_rows
                    )
                    self.conn.commit()
                logging.info(f"Wrote {len(outbox_rows)} failed embeddings to vector_outbox")
            except Exception as e:
                logging.warning(f"Failed to write to vector_outbox: {e}")

    async def flush_vector_outbox(self):
        """Retry embedding generation for all items in the vector outbox.

        Call this periodically or on-demand to process failed embeddings from non-atomic mode.
        Processes groups independently - successful groups are deleted even if others fail.
        """
        if not self.embedder:
            import logging
            logging.warning("Cannot flush vector_outbox without embedder")
            return

        import logging

        with self._lock:
            rows = self._exec_unsafe(
                "SELECT id, table_name, column_name, row_id, text FROM vector_outbox ORDER BY created_at",
                []
            )

        if not rows:
            return

        logging.info(f"Flushing {len(rows)} items from vector_outbox")

        grouped = {}
        for row in rows:
            key = (row["table_name"], row["column_name"])
            if key not in grouped:
                grouped[key] = {"ids": [], "texts": [], "outbox_ids": []}
            grouped[key]["ids"].append(row["row_id"])
            grouped[key]["texts"].append(row["text"])
            grouped[key]["outbox_ids"].append(row["id"])

        succeeded_outbox_ids = []
        for (table, column), payload in grouped.items():
            try:
                logging.debug(f"Retrying {len(payload['texts'])} embeddings for ({table}, {column})")
                embeddings = await self.embedder.embed(payload["texts"])
                vectors = [np.array(emb, dtype=np.float32) for emb in embeddings]

                vector_store = self.get_or_create_vector_store(table, column)
                with self._lock:
                    vector_store.add_batch(payload["ids"], vectors)

                succeeded_outbox_ids.extend(payload["outbox_ids"])
                logging.debug(f"Successfully retried {len(payload['texts'])} embeddings for ({table}, {column})")
            except Exception as e:
                logging.error(f"Failed to flush outbox group ({table}, {column}): {e}")

        if succeeded_outbox_ids:
            placeholders = ",".join("?" for _ in succeeded_outbox_ids)
            with self._lock:
                self.conn.execute(
                    f"DELETE FROM vector_outbox WHERE id IN ({placeholders})",
                    succeeded_outbox_ids
                )
                self.conn.commit()
            logging.info(f"Removed {len(succeeded_outbox_ids)}/{len(rows)} processed items from vector_outbox")

    def _enqueue_embedding(self, table: str, column: str, ids: List, texts: List):
        """Enqueue embeddings for batch processing.

        Each call represents ONE document's worth of chunks. Document boundaries
        are preserved for contextualized embeddings.

        Called by UpsertBuilder when inside batch_embeddings() context.
        If not inside context, embeddings should be generated immediately instead.

        Args:
            table: Table name
            column: Column name (vector field)
            ids: List of row IDs for this document
            texts: List of texts to embed for this document

        Raises:
            DatabaseError: If embedder is None or wrong task context
            ValueError: If ids and texts lengths don't match
        """
        if len(ids) != len(texts):
            raise ValueError(
                f"Mismatch in _enqueue_embedding: {len(ids)} ids but {len(texts)} texts "
                f"for {table}.{column}"
            )

        if not self.embedder:
            raise DatabaseError(
                f"Cannot enqueue embeddings for {table}.{column} without embedder. "
                "Set VOYAGE_API_KEY environment variable."
            )

        queue = emb_queue_var.get()
        if queue is None:
            raise RuntimeError(
                f"Cannot enqueue embeddings for {table}.{column} outside batch_embeddings() context. "
                f"Either use 'async with db.batch_embeddings():' or generate embeddings immediately."
            )

        key = (table, column)
        if key not in queue:
            queue[key] = []

        queue[key].append({"ids": ids, "texts": texts})

    async def _exec(self, sql: str, params: Union[List, tuple] = ()) -> List[Dict[str, Any]]:
        """Execute SQL with error handling and return rows as dicts.

        Includes optional slow-query logging controlled by INTELLIFIN_SLOW_SQL_MS (default 2000ms).
        """
        import time
        import os
        t0 = time.time()
        with self._lock:
            rows = self._exec_unsafe(sql, params)
            # Commit per statement only if not inside atomic batch context
            if not emb_atomic_var.get():
                self.conn.commit()
        elapsed_ms = (time.time() - t0) * 1000.0
        try:
            threshold = int(os.environ.get("INTELLIFIN_SLOW_SQL_MS", "2000"))
        except Exception:
            threshold = 2000
        if elapsed_ms >= threshold:
            import logging
            logging.warning(f"Slow SQL ({elapsed_ms:.0f} ms): {sql[:200]} ... params={params[:5] if isinstance(params, (list, tuple)) else ''}")
        return rows
