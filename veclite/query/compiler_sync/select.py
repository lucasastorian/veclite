"""SELECT query builder - sync version."""
import re
import numpy as np
from typing import Optional, List, Tuple, Dict
from veclite.query.compiler.mixins import PredMixin, SelectMixin
from veclite.query.ir import SelectIR, KeywordFTS, Col, And
from veclite.query.binder import bind_select
from veclite.query.planner import plan_select
from veclite.query.sqlgen import generate_select
from veclite.query.dialect.sqlite import SQLiteDialect
from veclite.core.results import Result

ALNUM_SPLIT = re.compile(r"(?<=\D)(?=\d)|(?<=\d)(?=\D)")


class SyncSelectBuilder(PredMixin, SelectMixin):
    """Fluent builder for SELECT queries - sync version."""

    def __init__(self, client, schema, table: str):
        self.db = client
        self.schema = schema
        self.table = table
        self.dialect = SQLiteDialect()
        self._pred = None
        self.selected = None
        self.order_by = []
        self.limit_n = None

    def keyword_search(self, query: str, column: str, topk: int = 50):
        """Build an FTS5 MATCH query for BM25 ranking.

        Uses OR logic with prefix matching for maximum recall.
        BM25 naturally ranks documents matching more terms higher.

        Args:
            query: Search query string
            column: Column to search
            topk: Maximum number of results to return (default: 50)
        """
        tokens = []
        for tok in query.strip().split():
            if '-' in tok:
                escaped = tok.strip('"').replace('"', '""')
                tokens.append(f'"{escaped}"')
            else:
                for sub in ALNUM_SPLIT.split(tok):
                    sub = sub.strip().strip('"').lower()
                    if not sub:
                        continue
                    if len(sub) >= 3 and '"' not in sub and "*" not in sub:
                        sub = f"{sub}*"
                    tokens.append(sub)

        if not tokens:
            tokens = ["*"]

        fts_query = " OR ".join(tokens)

        self._and(KeywordFTS(Col(column), fts_query))
        self.order_by = []
        self.limit(topk)
        return self

    def fts(self, query: str, column: str, topk: int = 50):
        """Build an FTS5 MATCH query for exact phrase matching.

        Wraps the query in double quotes for exact phrase matching.
        Only returns results containing the exact phrase.

        Args:
            query: Exact phrase to search for
            column: Column to search
            topk: Maximum number of results to return (default: 50)
        """
        escaped_query = query.strip().replace('"', '""')
        fts_query = f'"{escaped_query}"'

        self._and(KeywordFTS(Col(column), fts_query))
        self.order_by = []
        self.limit(topk)
        return self

    def vector_search(self, query: str, column: str, topk: int = 50, embedder=None, return_scores: bool = False):
        """Vector similarity search using brute-force cosine similarity.

        Results are automatically ordered by similarity score (descending) using SQL CASE ORDER BY.
        Scores are attached to each result row as '_score' field.

        Args:
            query: Query text to embed and search
            column: Column name (must have associated vector store)
            topk: Number of results to return from vector search
            embedder: Embedder instance (if None, uses db.embedder)
            return_scores: If True, _score field is guaranteed present (deprecated - always True)

        Returns:
            self (for chaining .execute())

        Note:
            - Embedding happens at execute() time, so this method is synchronous and chainable
            - Any WHERE predicates added before vector_search() filter the candidate set
            - Results are ordered by similarity score, not by any subsequent .order() calls
            - Use .limit() to further restrict results beyond topk
        """
        self._vector_search_params = {
            'query': query,
            'column': column,
            'topk': topk,
            'embedder': embedder,
            'return_scores': return_scores
        }
        return self

    def hybrid_search(
        self,
        query: str,
        vector_column: str = "embedding",
        keyword_column: str = "content",
        topk: int = 50,
        alpha: float = 0.7,
        overquery: int = 3
    ):
        """Hybrid search: fuse vector + keyword retrieval with single query.

        Executes vector search and keyword search sequentially, normalizes scores,
        and fuses with weighted combination.

        Args:
            query: Search query (used for both vector and keyword)
            vector_column: Column with embeddings (default: "embedding")
            keyword_column: Column with text for FTS (default: "content")
            topk: Final number of results to return (default: 50)
            alpha: Weight for vector scores (1-alpha for keyword). Default 0.7 (70% vector)
            overquery: Overquery factor - fetches topk*overquery from each leg (default: 3)

        Returns:
            self (for chaining .rerank() or .execute())
        """
        self._hybrid_search_params = {
            'query': query,
            'vector_column': vector_column,
            'keyword_column': keyword_column,
            'topk': topk,
            'alpha': alpha,
            'overquery': overquery
        }
        return self

    def rerank(self, query: str, content_column: str, topk: int, reranker=None, model: Optional[str] = None):
        """Rerank results using Voyage rerank API for precision refinement.

        Chainable after vector_search(), keyword_search(), or hybrid fusion.
        Reranking happens at execute() time: fetch candidates → extract content → rerank API → reorder.

        Args:
            query: Reranking query (can include instructions for rerank-2.5)
            content_column: Column containing text content to rerank on
            topk: Number of top results to return after reranking
            reranker: Reranker client (if None, uses db.embedder which is VoyageClient)
            model: Reranker model name (default: rerank-2.5 from embedder config)

        Returns:
            self (for chaining .execute())
        """
        if topk > 1000:
            raise ValueError(f"Rerank API supports max 1000 results, got topk={topk}")

        self._rerank_params = {
            'query': query,
            'content_column': content_column,
            'topk': topk,
            'reranker': reranker,
            'model': model
        }
        return self

    def _prefetch_filter_ids(self) -> Optional[List[int]]:
        """Prefetch IDs from current WHERE predicates for filtering vector/keyword searches."""
        if self._pred is None:
            return None

        temp_ir = SelectIR(
            table=self.table,
            columns=["id"],
            where=self._pred,
            order=[],
            limit=None,
        )
        bound = bind_select(temp_ir, self.schema)
        planned, _ = plan_select(bound, self.schema, self.dialect)
        sql, params = generate_select(planned, self.dialect)
        rows = self.db._exec(sql, params)
        return [row['id'] for row in rows]

    def _do_vector_search(self, query: str, column: str, topk: int, embedder, return_scores: bool,
                          filter_ids: Optional[List[int]] = None):
        """Internal: Perform the actual vector search with embedding."""
        if embedder is None:
            embedder = getattr(self.db, 'embedder', None)
            if embedder is None:
                raise ValueError("No embedder available. Pass embedder argument or set db.embedder")

        vector_table = self.table
        vector_column = column
        underlying_field = None

        if self.table in self.schema.views:
            view_cls = self.schema.views[self.table]
            type_map = view_cls.type_map(self.schema)
            if column not in type_map:
                raise ValueError(f"Column '{column}' not found in view '{self.table}'")

            view_field = view_cls.get_fields()[column]
            underlying_field = type_map[column]

            vector_table = getattr(view_field, '_view_src_table')
            vector_column = getattr(view_field, '_view_src_field')
        else:
            table_cls = self.schema.get_table(self.table)
            fields = table_cls.get_fields()
            if column not in fields:
                raise ValueError(f"Column '{column}' not found in table '{self.table}'")
            underlying_field = fields[column]

        vector_store = self.db.get_or_create_vector_store(vector_table, vector_column)

        is_contextualized = getattr(underlying_field, 'contextualized', False)
        if is_contextualized:
            query_embedding = embedder.contextual_query_vector(query=query)
        else:
            query_embedding = embedder.query_vector(query=query)

        query_vec = np.array(query_embedding, dtype=np.float32)

        if filter_ids is None and self._pred is not None:
            filter_ids = self._prefetch_filter_ids()

        ids, scores = vector_store.search(query_vec, topk=topk, filter_ids=filter_ids)

        self._vector_scores = dict(zip(ids.tolist(), scores.tolist()))
        self._return_scores = return_scores

        if len(ids) == 0:
            self._pred = None
            self.in_("id", [])
            self.order_by = []
        else:
            self._pred = None
            self.in_("id", ids.tolist())

            order_cases = " ".join(
                f"WHEN {int(id_)} THEN {rank}"
                for rank, id_ in enumerate(ids.tolist())
            )
            self.order_by = [(f"CASE id {order_cases} END", False)]

        # Prevent accidental re-execution of the vector search on reused builders
        if hasattr(self, "_vector_search_params"):
            delattr(self, "_vector_search_params")

    def _do_keyword_search(self, query: str, column: str, topk: int) -> Tuple[List[int], Dict[int, float]]:
        """Internal: Perform keyword search and return (ids, scores)."""
        base_pred = self._pred
        fts_pred = KeywordFTS(Col(column), query)
        where_pred = fts_pred if base_pred is None else And([base_pred, fts_pred])

        ir = SelectIR(
            table=self.table,
            columns=["id"],
            where=where_pred,
            order=[],
            limit=topk,
        )

        bound = bind_select(ir, self.schema)
        planned, _ = plan_select(bound, self.schema, self.dialect)
        sql, params = generate_select(planned, self.dialect)

        rows = self.db._exec(sql, params)

        ids = [r["id"] for r in rows]

        if rows and "_rank" in rows[0]:
            scores = {r["id"]: -float(r["_rank"]) for r in rows}
        else:
            n = len(ids)
            if n == 0:
                scores = {}
            elif n == 1:
                scores = {ids[0]: 1.0}
            else:
                scores = {id_: 1.0 - (idx / (n - 1)) for idx, id_ in enumerate(ids)}

        return ids, scores

    def count(self) -> int:
        """Execute COUNT(*) query and return the integer count directly."""
        ir = SelectIR(
            table=self.table,
            columns=["COUNT(*) as count"],
            where=self._pred,
            order=[],
            limit=None,
        )
        bound = bind_select(ir, self.schema)
        planned, _ = plan_select(bound, self.schema, self.dialect)
        sql, params = generate_select(planned, self.dialect)
        rows = self.db._exec(sql, params)

        return rows[0]['count'] if rows else 0

    def execute(self):
        """Execute the SELECT query."""
        if hasattr(self, '_hybrid_search_params'):
            self._do_hybrid_search(**self._hybrid_search_params)
        elif hasattr(self, '_vector_search_params'):
            self._do_vector_search(**self._vector_search_params)

        ir = SelectIR(
            table=self.table,
            columns=self.selected,
            where=self._pred,
            order=self.order_by,
            limit=self.limit_n,
        )
        bound = bind_select(ir, self.schema)
        planned, _ = plan_select(bound, self.schema, self.dialect)
        sql, params = generate_select(planned, self.dialect)
        rows = self.db._exec(sql, params)

        processed = []
        for row in rows:
            row = self.db._deserialize_json_fields(self.table, row)
            if hasattr(self, '_vector_scores') and self._vector_scores and 'id' in row:
                row['_score'] = self._vector_scores.get(row['id'], 0.0)
            processed.append(row)

        if hasattr(self, '_rerank_params'):
            processed = self._do_rerank(processed, **self._rerank_params)

        return Result(processed)

    def _do_rerank(self, rows: List[Dict], query: str, content_column: str, topk: int,
                         reranker=None, model: Optional[str] = None) -> List[Dict]:
        """Internal: Rerank rows using Voyage rerank API."""
        if not rows:
            return rows

        if reranker is None:
            reranker = getattr(self.db, 'embedder', None)
            if reranker is None:
                raise ValueError("No reranker available. Set db.embedder or pass reranker argument")

        if not hasattr(reranker, 'rerank'):
            raise ValueError(f"Reranker {type(reranker).__name__} does not have rerank() method")

        documents = []
        for row in rows:
            if content_column not in row:
                raise ValueError(f"Column '{content_column}' not found in row. "
                               f"Available columns: {list(row.keys())}")
            doc = row[content_column]
            if doc is None:
                doc = ""
            documents.append(str(doc))

        rerank_results = reranker.rerank(
            query=query,
            documents=documents,
            top_k=topk,
            model=model
        )

        reranked_rows = []
        for result in rerank_results:
            original_idx = result['index']
            row = rows[original_idx].copy()

            if '_score' in row:
                row['_original_score'] = row['_score']

            row['_score'] = result['relevance_score']

            reranked_rows.append(row)

        return reranked_rows

    def _do_hybrid_search(
        self,
        query: str,
        vector_column: str,
        keyword_column: str,
        topk: int,
        alpha: float,
        overquery: int
    ):
        """Internal: Execute hybrid search (vector + keyword fusion) - sync version."""
        embedder = getattr(self.db, 'embedder', None)
        if embedder is None:
            from veclite.core.errors import DatabaseError
            raise DatabaseError(
                "Cannot perform hybrid_search without an embedder. "
                "Set VOYAGE_API_KEY environment variable."
            )

        filter_ids = self._prefetch_filter_ids()

        vec_topk = topk * overquery
        kw_topk = topk * overquery

        vec_params = {
            'query': query,
            'column': vector_column,
            'embedder': getattr(self.db, 'embedder', None),
            'return_scores': True,
            'filter_ids': filter_ids
        }

        kw_params = {
            'query': query,
            'column': keyword_column
        }

        self._do_vector_search(topk=vec_topk, **vec_params)
        vec_ids = list(self._vector_scores.keys())
        vec_scores = self._vector_scores

        kw_ids, kw_scores = self._do_keyword_search(topk=kw_topk, **kw_params)

        if not vec_ids and not kw_ids:
            self._pred = None
            self.in_("id", [])
            self.order_by = []
            self._fused_scores = {}
            self._vector_scores = {}
            return

        if not vec_ids:
            alpha_effective = 0.0
        elif not kw_ids:
            alpha_effective = 1.0
        else:
            alpha_effective = alpha

        def minmax(scores: Dict[int, float]) -> Dict[int, float]:
            if not scores:
                return {}
            vals = list(scores.values())
            lo, hi = min(vals), max(vals)
            if hi == lo:
                return {k: 1.0 for k in scores}
            rng = hi - lo
            return {k: (v - lo) / rng for k, v in scores.items()}

        vec_shifted = {i: (s + 1.0) / 2.0 for i, s in vec_scores.items()}
        vec_norm = minmax(vec_shifted)

        kw_norm = minmax(kw_scores)

        all_ids = list(dict.fromkeys(vec_ids + kw_ids))
        fused = {
            id_: alpha_effective * vec_norm.get(id_, 0.0) + (1 - alpha_effective) * kw_norm.get(id_, 0.0)
            for id_ in all_ids
        }

        final_ids = sorted(all_ids, key=lambda i: fused[i], reverse=True)[:topk]

        if not final_ids:
            self._pred = None
            self.in_("id", [])
            self.order_by = []
            self._fused_scores = {}
            self._vector_scores = {}
        else:
            self._pred = None
            self.in_("id", final_ids)
            order_cases = " ".join(
                f"WHEN {int(id_)} THEN {rank}"
                for rank, id_ in enumerate(final_ids)
            )
            self.order_by = [(f"CASE id {order_cases} END", False)]
            self._fused_scores = fused
            self._vector_scores = fused

        if hasattr(self, "_vector_search_params"):
            delattr(self, "_vector_search_params")
