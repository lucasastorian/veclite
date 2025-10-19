# VecLite

**Local‑first SQLite + vectors for agentic RAG — zero infra.**

VecLite combines relational filters, BM25 keyword search, vector similarity, hybrid fusion, and optional reranking — all in one local database that runs entirely on your machine.

## What is VecLite For?

VecLite is **NOT** a production database for high‑traffic servers. It's specifically designed for:

- **RAG Applications** - Build retrieval systems for LLM applications
- **Local Development** - Prototype semantic search features quickly
- **Jupyter Notebooks** - Interactive data analysis with vector search
- **Research & Experimentation** - Test embedding models and search strategies
- **Desktop Applications** - Local-first apps with semantic search
- **Edge Computing** - Lightweight vector search on devices

## Why VecLite?

### Simple & Local
- **No External Services** - Everything runs on SQLite
- **Zero Infrastructure** - No Docker, no cloud, just `pip install`
- **Perfect for Prototyping** - Get semantic search running in minutes
- **Notebook-Friendly** - Both sync and async APIs

### Built for RAG
- **Automatic Embeddings** - Mark fields with `vector=True` and go
- **Hybrid Retrieval** - Combine semantic + keyword for better recall
- **Standalone RAG** - Integrate with frameworks via custom retrievers if desired
- **Smart Batching** - Optimized for embedding generation

### Developer Experience
- **Schema-First Design** - Define tables with Python classes
- **Type-Safe Queries** - Fluent, chainable query builder
- **Sync & Async** - Choose the right API for your use case
- **Auto-Migration** - Schema changes applied automatically

## Key Features

=== "Vector Search"
    ```python
    # Find similar documents by semantic meaning
    results = client.table("documents").vector_search(
        query="Python async programming",
        topk=5
    ).execute()
    ```

=== "Keyword Search"
    ```python
    # Full-text search with BM25 ranking
    results = client.table("articles").keyword_search(
        query="machine learning",
        topk=10
    ).execute()
    ```

=== "Hybrid Search"
    ```python
    # Best of both worlds: semantic + keyword
    results = client.table("posts").hybrid_search(
        query="database optimization",
        alpha=0.7,  # 70% vector, 30% keyword
        topk=10
    ).execute()
    ```

## Use Cases

- **RAG Pipelines** - Retrieval for LangChain, LlamaIndex applications
- **Jupyter Notebooks** - Interactive semantic search during data exploration
- **Prototyping** - Test embedding models and search strategies locally
- **Desktop Apps** - Local-first applications with semantic capabilities
- **Research** - Experiment with hybrid search algorithms
- **CLI Tools** - Add semantic search to command-line applications

## Quick Example

```python
from veclite import Client, Schema
from veclite.schema import Table, Integer, Text, VectorConfig

# Define schema
class Document(Table):
    id = Integer(primary_key=True)
    title = Text()
    content = Text(
        vector=VectorConfig.voyage_lite(),  # Auto-embeddings
        fts=True  # Full-text search
    )

# Create database (nested folder)
schema = Schema()
schema.add_table(Document)
client = Client.create(schema, "my_db")  # creates ./my_db/{sqlite.db, vectors/}

# Insert data
client.table("documents").insert([
    {"title": "Python Guide", "content": "Learn Python programming"},
    {"title": "SQL Tutorial", "content": "Master database queries"},
]).execute()

# Search semantically
results = client.table("documents").vector_search(
    query="programming tutorials",
    topk=5
).execute()

## Storage Layout (Nested)

The path you pass to `Client.create()` is a directory that contains all artifacts:

- `sqlite.db` (+ `-wal`/`-shm` when active)
- `vectors/` (per‑field vector files)

Example: `Client.create(schema, "rag_db")` creates:
```
./rag_db/
  sqlite.db
  sqlite.db-wal  # when WAL active
  vectors/
    documents__content.vec
    documents__content.id
    documents__content.tomb.json
```

## Atomic Batch Embeddings (Consistency)

Ensure all‑or‑nothing inserts with batched embeddings (async):

```python
async with async_client.batch_embeddings():
    await async_client.table("documents").insert(batch1).execute()
    await async_client.table("documents").insert(batch2).execute()
# If any embedding fails → rollback SQLite; no vectors written
```

- Default is atomic. Non‑atomic option: `async with db.batch_embeddings(atomic=False): ...` which batches for efficiency and writes failures to an outbox for later retry.

## Recipe: SEC Filings (Relational + FTS + Vectors)

Keep filings in one DB, pages as FTS‑only, and chunks with vectors. Let an agent both retrieve semantically and read exact page ranges.

```python
from veclite import Client, Schema
from veclite.schema import Table, Integer, Text, Boolean

class Filings(Table):
    id = Integer(primary_key=True)
    ticker = Text(index=True)
    form_type = Text(index=True)
    filing_date = Text(index=True)

class FilingPages(Table):
    id = Integer(primary_key=True)
    filing_id = Integer(index=True)
    page_number = Integer(index=True)
    content = Text(fts=True)  # FTS only

class FilingChunks(Table):
    id = Integer(primary_key=True)
    filing_id = Integer(index=True)
    page = Integer(index=True)
    content = Text(vector=True, fts=True)  # vectors + FTS
    has_table = Boolean(default=False)

schema = Schema()
schema.add_table(Filings)
schema.add_table(FilingPages)
schema.add_table(FilingChunks)

client = Client.create(schema, "sec_db")

# Hybrid retrieval on chunks within a filing
q = "Apple risk factors and competitive challenges"
hits = client.table("filing_chunks").hybrid_search(q, topk=10, alpha=0.7) \
    .eq("filing_id", 12345).execute()

# Read a page window around best hit
best = hits.data[0]
page = best["page"]
filing_id = best["filing_id"]
pages = client.table("filing_pages").select("*") \
    .eq("filing_id", filing_id) \
    .between("page_number", page - 1, page + 1) \
    .order("page_number") \
    .execute()
```
```

## Next Steps

<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } __Get Started in 5 Minutes__

    ---

    Install VecLite and create your first database

    [:octicons-arrow-right-24: Installation](installation.md)

-   :material-book-open-variant:{ .lg .middle } __Learn the Basics__

    ---

    Understand schemas, queries, and search

    [:octicons-arrow-right-24: Getting Started](getting-started/quickstart.md)

-   :material-code-braces:{ .lg .middle } __API Reference__

    ---

    Detailed documentation for all classes and methods

    [:octicons-arrow-right-24: API Docs](api/client.md)

-   :material-lightning-bolt:{ .lg .middle } __Advanced Features__

    ---

    Schema validation, embeddings, performance tuning

    [:octicons-arrow-right-24: Advanced](advanced/schema-validation.md)

</div>
