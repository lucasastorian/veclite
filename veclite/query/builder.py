"""Entry point for all query operations on a table or view."""
from typing import Dict, List, Union, Any, Optional
from veclite.query.compiler.select import SelectBuilder
from veclite.query.compiler.insert import InsertBuilder
from veclite.query.compiler.update import UpdateBuilder
from veclite.query.compiler.delete import DeleteBuilder
from veclite.query.compiler.upsert import UpsertBuilder


class TableQueryBuilder:
    """Entry point for all query operations on a table or view."""

    def __init__(self, client, schema, table: str):
        self.client = client
        self.schema = schema
        self.table_name = table
        self.is_view = table in schema.views
        self.table = schema.view(table) if self.is_view else schema.table(table)

    def select(self, fields: str = '*') -> SelectBuilder:
        """Start a SELECT query."""
        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if fields != '*':
            builder.select(fields)
        return builder

    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> InsertBuilder:
        """Start an INSERT query."""
        if self.is_view:
            raise ValueError(f"Cannot INSERT into view '{self.table_name}'.")
        return InsertBuilder(self.client, self.schema, self.table_name, data=data)

    def update(self, data: Dict[str, Any]) -> UpdateBuilder:
        """Start an UPDATE query."""
        if self.is_view:
            raise ValueError(f"Cannot UPDATE view '{self.table_name}'.")
        return UpdateBuilder(self.client, self.schema, self.table_name, data)

    def delete(self) -> DeleteBuilder:
        """Start a DELETE query."""
        if self.is_view:
            raise ValueError(f"Cannot DELETE from view '{self.table_name}'.")
        return DeleteBuilder(self.client, self.schema, self.table_name)

    def upsert(
        self,
        values: Union[Dict[str, Any], List[Dict[str, Any]]],
        on_conflict: Union[str, List[str]],
        ignore_duplicates: bool = False,
        returning: str = "representation",
        count: Optional[str] = None,
        default_to_null: bool = False,
    ) -> UpsertBuilder:
        """Start an UPSERT query (INSERT ... ON CONFLICT ...)."""
        if self.is_view:
            raise ValueError(f"Cannot UPSERT into view '{self.table_name}'.")
        return UpsertBuilder(
            self.client, self.schema, self.table_name,
            values=values,
            on_conflict=on_conflict,
            ignore_duplicates=ignore_duplicates,
            returning=returning,
            count=count,
            default_to_null=default_to_null,
        )

    def keyword_search(self, query: str, column: Optional[str] = None, returning: Optional[str] = None) -> SelectBuilder:
        """Full-text search using FTS5 with BM25 ranking."""
        if self.is_view:
            type_map = self.table.type_map(self.schema)
            fts_columns = [alias for alias, fd in type_map.items() if getattr(fd, "fts", False)]
            all_fields = set(type_map.keys())
        else:
            fields = self.table.get_fields()
            fts_columns = [name for name, field in fields.items() if getattr(field, "fts", False)]
            all_fields = set(fields.keys())

        if column:
            if column not in fts_columns:
                if column in all_fields:
                    raise ValueError(
                        f"Column '{column}' on {'view' if self.is_view else 'table'} '{self.table_name}' is not FTS-enabled. "
                        f"FTS columns: {fts_columns}"
                    )
                else:
                    raise ValueError(f"Column '{column}' does not exist on {'view' if self.is_view else 'table'} '{self.table_name}'.")
            search_column = column
        else:
            if not fts_columns:
                raise ValueError(f"{'View' if self.is_view else 'Table'} '{self.table_name}' has no FTS-enabled columns.")
            elif len(fts_columns) > 1:
                raise ValueError(
                    f"{'View' if self.is_view else 'Table'} '{self.table_name}' has multiple FTS-enabled columns: {fts_columns}. "
                    f"Please specify which column to search using the 'column' parameter."
                )
            search_column = fts_columns[0]

        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if returning:
            builder.select(returning)
        builder.keyword_search(query, search_column)
        return builder

    def regex_search(self, pattern: str, column: str, returning: Optional[str] = None) -> SelectBuilder:
        """Pattern search using REGEXP."""
        if self.is_view:
            type_map = self.table.type_map(self.schema)
            all_fields = set(type_map.keys())
        else:
            all_fields = set(self.table.get_fields().keys())

        if column not in all_fields:
            raise ValueError(f"Column '{column}' does not exist on {'view' if self.is_view else 'table'} '{self.table_name}'.")

        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if returning:
            builder.select(returning)
        builder.regex(column, pattern)
        return builder

    def fts(self, query: str, column: Optional[str] = None, returning: Optional[str] = None) -> SelectBuilder:
        """Exact phrase search using FTS5.

        Returns only results containing the exact phrase.

        Args:
            query: Exact phrase to search for
            column: FTS-enabled column to search (optional if only one FTS column exists)
            returning: Fields to return (optional)
        """
        if self.is_view:
            type_map = self.table.type_map(self.schema)
            fts_columns = [alias for alias, fd in type_map.items() if getattr(fd, "fts", False)]
            all_fields = set(type_map.keys())
        else:
            fields = self.table.get_fields()
            fts_columns = [name for name, field in fields.items() if getattr(field, "fts", False)]
            all_fields = set(fields.keys())

        if column:
            if column not in fts_columns:
                if column in all_fields:
                    raise ValueError(
                        f"Column '{column}' on {'view' if self.is_view else 'table'} '{self.table_name}' is not FTS-enabled. "
                        f"FTS columns: {fts_columns}"
                    )
                else:
                    raise ValueError(f"Column '{column}' does not exist on {'view' if self.is_view else 'table'} '{self.table_name}'.")
            search_column = column
        else:
            if not fts_columns:
                raise ValueError(f"{'View' if self.is_view else 'Table'} '{self.table_name}' has no FTS-enabled columns.")
            elif len(fts_columns) > 1:
                raise ValueError(
                    f"{'View' if self.is_view else 'Table'} '{self.table_name}' has multiple FTS-enabled columns: {fts_columns}. "
                    f"Please specify which column to search using the 'column' parameter."
                )
            search_column = fts_columns[0]

        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if returning:
            builder.select(returning)
        builder.fts(query, search_column)
        return builder

    def vector_search(
        self,
        query: str,
        column: Optional[str] = None,
        topk: int = 50,
        embedder=None,
        return_scores: bool = False,
        returning: Optional[str] = None
    ) -> SelectBuilder:
        """Vector similarity search using brute-force cosine similarity.

        Args:
            query: Query text to embed and search
            column: Column name with vector embeddings (optional if only one vector column exists)
            topk: Number of results to return
            embedder: Embedder instance (if None, uses client.embedder)
            return_scores: If True, _score field is included (deprecated - always True)
            returning: Fields to return (optional)

        Returns:
            SelectBuilder for chaining
        """
        # Find vector columns
        if self.is_view:
            type_map = self.table.type_map(self.schema)
            vector_columns = [alias for alias, fd in type_map.items() if getattr(fd, "vector", False)]
            all_fields = set(type_map.keys())
        else:
            fields = self.table.get_fields()
            vector_columns = [name for name, field in fields.items() if getattr(field, "vector", False)]
            all_fields = set(fields.keys())

        if column:
            if column not in vector_columns:
                if column in all_fields:
                    raise ValueError(
                        f"Column '{column}' on {'view' if self.is_view else 'table'} '{self.table_name}' is not vector-enabled. "
                        f"Vector columns: {vector_columns}"
                    )
                else:
                    raise ValueError(f"Column '{column}' does not exist on {'view' if self.is_view else 'table'} '{self.table_name}'.")
            search_column = column
        else:
            if not vector_columns:
                raise ValueError(f"{'View' if self.is_view else 'Table'} '{self.table_name}' has no vector-enabled columns.")
            elif len(vector_columns) > 1:
                raise ValueError(
                    f"{'View' if self.is_view else 'Table'} '{self.table_name}' has multiple vector-enabled columns: {vector_columns}. "
                    f"Please specify which column to search using the 'column' parameter."
                )
            search_column = vector_columns[0]

        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if returning:
            builder.select(returning)
        builder.vector_search(query, search_column, topk, embedder, return_scores)
        return builder

    def hybrid_search(
        self,
        query: str,
        vector_column: Optional[str] = None,
        keyword_column: Optional[str] = None,
        topk: int = 50,
        alpha: float = 0.7,
        overquery: int = 3,
        returning: Optional[str] = None
    ) -> SelectBuilder:
        """Hybrid search: fuse vector + keyword retrieval with single query.

        Executes vector search and keyword search in parallel, normalizes scores,
        and fuses with weighted combination.

        Args:
            query: Search query (used for both vector and keyword)
            vector_column: Column with embeddings (optional if only one vector column exists)
            keyword_column: Column with text for FTS (optional if only one FTS column exists)
            topk: Number of results to return
            alpha: Weight for vector scores (1-alpha = weight for keyword scores)
            overquery: Multiplier for initial retrieval before fusion
            returning: Fields to return (optional)

        Returns:
            SelectBuilder for chaining
        """
        # Find vector columns
        if self.is_view:
            type_map = self.table.type_map(self.schema)
            vector_columns = [alias for alias, fd in type_map.items() if getattr(fd, "vector", False)]
            fts_columns = [alias for alias, fd in type_map.items() if getattr(fd, "fts", False)]
        else:
            fields = self.table.get_fields()
            vector_columns = [name for name, field in fields.items() if getattr(field, "vector", False)]
            fts_columns = [name for name, field in fields.items() if getattr(field, "fts", False)]

        # Resolve vector column
        if vector_column:
            if vector_column not in vector_columns:
                raise ValueError(
                    f"Column '{vector_column}' is not vector-enabled. "
                    f"Vector columns: {vector_columns}"
                )
            search_vector_column = vector_column
        else:
            if not vector_columns:
                raise ValueError(f"{'View' if self.is_view else 'Table'} '{self.table_name}' has no vector-enabled columns.")
            elif len(vector_columns) > 1:
                raise ValueError(
                    f"{'View' if self.is_view else 'Table'} '{self.table_name}' has multiple vector-enabled columns: {vector_columns}. "
                    f"Please specify which column to search using the 'vector_column' parameter."
                )
            search_vector_column = vector_columns[0]

        # Resolve keyword column
        if keyword_column:
            if keyword_column not in fts_columns:
                raise ValueError(
                    f"Column '{keyword_column}' is not FTS-enabled. "
                    f"FTS columns: {fts_columns}"
                )
            search_keyword_column = keyword_column
        else:
            if not fts_columns:
                raise ValueError(f"{'View' if self.is_view else 'Table'} '{self.table_name}' has no FTS-enabled columns.")
            elif len(fts_columns) > 1:
                raise ValueError(
                    f"{'View' if self.is_view else 'Table'} '{self.table_name}' has multiple FTS-enabled columns: {fts_columns}. "
                    f"Please specify which column to search using the 'keyword_column' parameter."
                )
            search_keyword_column = fts_columns[0]

        builder = SelectBuilder(self.client, self.schema, self.table_name)
        if returning:
            builder.select(returning)
        builder.hybrid_search(query, search_vector_column, search_keyword_column, topk, alpha, overquery)
        return builder
