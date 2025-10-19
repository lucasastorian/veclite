# VecLite

**Local‑first SQLite + vectors for agentic RAG — zero infra.**

![Tests](https://img.shields.io/badge/tests-135%20passed-green)
![Python](https://img.shields.io/badge/python-3.9+-blue)
![License](https://img.shields.io/badge/license-MIT-blue)

VecLite isn't a traditional ORM adapted for search — it's **purpose‑built for retrieval**. It combines relational filters, BM25, vectors, hybrid fusion, and optional reranking in one lightweight library that runs entirely on your machine.

**Why VecLite exists:**
- ⚡ **Local‑first, zero infra** — SQLite + vectors; no services to run
- 🧰 **No plumbing** — embeddings + indices managed automatically
- 🧩 **Relational + views** — enrich chunks without denormalization
- 🔎 **Three search modes** — BM25, vector, hybrid (+ optional rerank modifier)
- ✅ **Atomic embeddings** — batch once; no rows without vectors on failure
- 🔗 **Simple filters** — eq/in_/between/JSON contains/ilike/regex
- 📦 **One folder** — your entire RAG + DB stack (sqlite.db + vectors/)
- 🗂️ **No metadata limits** — normal SQL columns/JSON, any shape
- 🧱 **Vector + non‑vector tables** — mix FTS‑only and regular tables alongside vectors

---

## Comparison

| Capability | VecLite (SQLite + Vectors) | Typical Managed Vector DBs |
| --- | --- | --- |
| Setup | Local‑first, zero infra | Hosted service / cluster |
| Data model | Relational tables + views + JSON | Vector‑centric, document/NoSQL |
| Non‑vector queries | Yes (filters, ranges, regex, FTS) | Limited (metadata filters only) |
| Joins / views | Yes (via SQL views) | No |
| Keyword search | Yes (BM25 via FTS5) | Rare / not native |
| Vector search | Yes (cosine) | Yes |
| Hybrid search | Yes (vector + BM25) | Varies; often custom |
| Rerank | Optional modifier | External / varies |
| CRUD | Full insert/update/upsert/delete | Insert/update; deletes vary |
| Metadata limits | No (normal SQL columns/JSON) | Often constrained (field count/size) |
| Record size limits | No hard per‑row payload limit | Common (~40KB metadata payloads) |
| Non‑vector tables | Yes (regular/FTS‑only tables) | No (vector‑centric indexes) |
| Consistency | Atomic batch embeddings | Varies by service |
| Storage | One folder: sqlite.db + vectors/ | Remote indexes/storage |
| Best for | Local RAG, agents, notebooks | Production scale, multi‑tenant APIs |

VecLite is not a production, multi‑tenant vector service — it’s a compact, relational RAG database you can fully own and query normally.

Note: Many managed vector services impose strict metadata payload limits (e.g., ~40KB per record) and center the data model around vector indexes; VecLite lets you store any shape/size of metadata and create tables without vectors at all.

> **Use VecLite for:** Local‑first RAG, agentic retrieval, Jupyter notebooks, desktop/edge apps
> **Not for:** High‑traffic multi‑tenant servers; drop‑in vector store adapters (integrate via custom retrievers instead)

---

## Quick Start

Build a semantic search system in 5 lines:

```bash
pip install veclite[voyage]  # Includes Voyage AI embeddings
export VOYAGE_API_KEY="your-key"  # Get from https://www.voyageai.com
```

```python
from veclite import Client, Schema
from veclite.schema import Table, Integer, Text

# 1. Define schema - embeddings happen automatically
class Document(Table):
    __tablename__ = "documents"
    id = Integer(primary_key=True)
    content = Text(vector=True, fts=True)  # vector + keyword search

# 2. Create database (nested folder)
schema = Schema()
schema.add_table(Document)
client = Client.create(schema, "rag_db")  # creates ./rag_db/{sqlite.db, vectors/}

# 3. Insert - embeddings generated automatically
client.table("documents").insert([
    {"content": "Python is a programming language"},
    {"content": "Machine learning uses neural networks"},
    {"content": "The Solar System has 8 planets"},
]).execute()

# 4. Search by meaning (finds ML doc, not Python)
results = client.table("documents").vector_search(
    query="AI and deep learning",
    topk=5
).execute()
```

**That's it.** No embedding pipelines, no vector databases, no infrastructure.

---

## Atomic Batch Embeddings (Consistency)

Ensure all‑or‑nothing inserts with batched embeddings:

```python
# Async example
async with async_client.batch_embeddings():
    await async_client.table("documents").insert([...]).execute()
    await async_client.table("documents").insert([...]).execute()
# If any embedding fails → rollback SQLite; no vectors written
```

- Default is atomic: one SQLite transaction; embeddings generated; vectors written; then COMMIT.
- Non‑atomic option: `async with db.batch_embeddings(atomic=False): ...` batches for efficiency and writes failures to an outbox for later retry via `flush_vector_outbox()`.

---

## Search Modes (+ optional rerank modifier)

VecLite provides the complete retrieval stack - perfect for agentic RAG systems that need different search strategies:

### 1. 🔍 **Keyword Search (BM25)**
Traditional full-text search with production-grade BM25 ranking:
```python
results = client.table("docs").keyword_search(
    query="machine learning transformers",
    topk=10
).execute()
```

**Use when:** Exact term matching matters (product codes, names, technical terms)

### 2. 🎯 **Vector Search (Semantic)**
Find by meaning, not just keywords:
```python
results = client.table("docs").vector_search(
    query="AI tutorials for beginners",  # Matches "ML guides for newcomers"
    topk=10
).execute()
```

**Use when:** Semantic similarity matters more than exact terms

### 3. 🚀 **Hybrid Search (Best of Both)**
Combines keyword + vector with Reciprocal Rank Fusion:
```python
results = client.table("docs").hybrid_search(
    query="transformer architecture",
    alpha=0.7,  # 70% semantic, 30% keyword
    topk=10
).execute()
```

**Use when:** You want both precision (keywords) and recall (semantics)
**Perfect for:** General-purpose RAG retrieval

### 🎖️ Rerank Modifier (optional)
Post-retrieval modifier to refine candidates:
```python
from veclite.embeddings import VoyageClient

# Get candidates with hybrid search
candidates = client.table("docs") \
    .hybrid_search("quantum computing", topk=100) \
    .execute()

# Rerank top 100 → best 10
embedder = VoyageClient()
reranked = embedder.rerank(
    query="quantum computing applications",
    documents=[doc["content"] for doc in candidates.data],
    top_k=10
)
```

**Use when:** Quality > speed (2-stage retrieval)

---



## Perfect for Agentic RAG

VecLite's modular design makes it ideal for agentic systems where the AI chooses retrieval strategies:

```python
class RAGAgent:
    def retrieve(self, query: str, strategy: str = "auto"):
        if strategy == "auto":
            # Agent decides based on query type
            if self._is_technical_query(query):
                return self.keyword_search(query)
            else:
                return self.hybrid_search(query)

        elif strategy == "keyword":
            return self.db.table("docs").keyword_search(query, topk=10)

        elif strategy == "semantic":
            return self.db.table("docs").vector_search(query, topk=10)

        elif strategy == "hybrid":
            return self.db.table("docs").hybrid_search(query, alpha=0.7, topk=10)

        elif strategy == "deep":
            # Two-stage: hybrid → rerank
            candidates = self.db.table("docs").hybrid_search(query, topk=100)
            return self.embedder.rerank(query, candidates.data, top_k=10)
```

**Agents can:**
- Choose search strategies dynamically
- Combine multiple retrieval modes
- Filter by metadata before/after search
- Iteratively refine with different strategies

---

## Recipe: SEC Filings (Relational + FTS + Vectors)

Keep filings in one DB, pages as FTS‑only, and chunks with vectors. Let an agent both retrieve semantically and read exact page ranges.

```python
from veclite import Client, Schema
from veclite.schema import Table, Integer, Text, Boolean

class Filings(Table):
    __tablename__ = "filings"
    id = Integer(primary_key=True)
    ticker = Text(index=True)
    form_type = Text(index=True)
    filing_date = Text(index=True)

class FilingPages(Table):
    __tablename__ = "filing_pages"
    id = Integer(primary_key=True)
    filing_id = Integer(index=True)
    page_number = Integer(index=True)
    content = Text(fts=True)  # FTS only

class FilingChunks(Table):
    __tablename__ = "filing_chunks"
    id = Integer(primary_key=True)
    filing_id = Integer(index=True)
    page = Integer(index=True)
    content = Text(vector=True, fts=True)  # vectors + FTS
    has_table = Boolean(default=False)

schema = Schema()
schema.add_table(Filings)
schema.add_table(FilingPages)
schema.add_table(FilingChunks)

client = Client.create(schema, "sec_db")  # ./sec_db/{sqlite.db, vectors/}

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

This three‑table pattern (filings, filing_pages, filing_chunks) gives agents precision (page ranges) and recall (semantic + keyword).

## Automatic Embeddings

No manual embedding pipeline needed - VecLite handles everything:

```python
# Mark field for auto-embeddings
class Paper(Table):
    __tablename__ = "papers"
    id = Integer(primary_key=True)
    title = Text()
    abstract = Text(vector=True, fts=True)  # Auto-embed on insert/update
    year = Integer()
```

**What happens automatically:**
- ✅ Embeddings generated on insert/update
- ✅ Batching for efficiency
- ✅ LMDB caching (avoid re-embedding)
- ✅ Vector storage alongside SQLite

**Supported models:**
- `vector=True` → voyage-3.5-lite (512D, **default**)
- `vector=VectorConfig.voyage_3()` → voyage-3 (1024D)
- `vector=VectorConfig.voyage_large()` → voyage-3.5-large (1536D)
- `contextualized=True` → voyage-context-3 (contextualized retrieval, 512D default)

---

## Advanced Search Examples

### Filtered Search
```python
# Search within filtered subset
results = client.table("papers") \
    .hybrid_search("climate impacts", alpha=0.6, topk=20) \
    .eq("category", "science") \
    .gt("year", 2020) \
    .is_not_null("peer_reviewed") \
    .execute()
```

### Multi-Field Search
```python
class Article(Table):
    __tablename__ = "articles"
    id = Integer(primary_key=True)
    title = Text(vector=True, fts=True)
    body = Text(vector=True, fts=True)

# Search specific field
results = client.table("articles").vector_search(
    query="AI safety",
    column="title",  # Search titles only
    topk=10
).execute()
```

### Contextualized Embeddings (Advanced RAG)
```python
# Better retrieval with document context
class Filing(Table):
    __tablename__ = "filings"
    id = Integer(primary_key=True)
    content = Text(contextualized=True, contextualized_dim=512, fts=True)

# Each chunk embedded with awareness of surrounding chunks
# → Higher quality retrieval for long documents
```

---

## Installation

```bash
# Core (SQLite + local vectors)
pip install veclite

# With Voyage AI embeddings (recommended)
pip install veclite[voyage]

# With embedding cache (LMDB)
pip install veclite[cache]

# Everything
pip install veclite[all]
```

**Requirements:**
- Python 3.9+
- SQLite 3.35+ (included with Python)
- NumPy

---

## Sync vs Async

Choose the right API for your use case:

**Sync** - Notebooks, scripts, simple applications:
```python
from veclite import Client

client = Client.create(schema, "db.db")
results = client.table("docs").hybrid_search("query", topk=10).execute()
```

**Async** - Web apps, concurrent workloads:
```python
from veclite import AsyncClient

client = AsyncClient.create(schema, "db.db")
results = await client.table("docs").hybrid_search("query", topk=10).execute()
```

---

## When to Use VecLite

### ✅ **Perfect For**
- **RAG Systems** - Complete standalone retrieval solution
- **Agentic RAG** - Agents that choose retrieval strategies dynamically
- **Semantic Search** - Find documents by meaning, not just keywords
- **Jupyter Notebooks** - Interactive development and analysis
- **Desktop Applications** - Local-first semantic search
- **Edge/IoT Devices** - On-device retrieval without external APIs

### ❌ **NOT For**
- **Production web servers** - Use Qdrant, Pinecone, Weaviate instead
- **Multi-tenant SaaS** - VecLite is single-tenant by design
- **High concurrency** - SQLite write limitations

---

## Documentation

📚 **[Full Documentation](https://veclite.readthedocs.io)**

- [Installation](https://veclite.readthedocs.io/installation.md)
- [Quickstart Guide](https://veclite.readthedocs.io/getting-started/quickstart.md)
- [Vector Search](https://veclite.readthedocs.io/search/vector.md)
- [Keyword Search](https://veclite.readthedocs.io/search/keyword.md)
- [Hybrid Search](https://veclite.readthedocs.io/search/hybrid.md)
- [Schema Definition](https://veclite.readthedocs.io/getting-started/schema.md)
- [API Reference](https://veclite.readthedocs.io/api/client.md)

---

## Testing

```bash
# Run all 120 tests
pytest tests/

# Run specific test suite
pytest tests/test_vector_search_sync.py
pytest tests/test_hybrid_search_async.py
```

---

## Contributing

Contributions welcome! VecLite is designed to be simple and focused on RAG use cases.

```bash
git clone https://github.com/lucasastorian/veclite.git
cd veclite
pip install -e ".[dev]"
pytest tests/
```

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- Built on SQLite's FTS5 for BM25 keyword search
- Inspired by Supabase's fluent query API
- Optimized for RAG and local-first applications
- Voyage AI for state-of-the-art embeddings

---

**[View Docs](https://veclite.readthedocs.io)** | **[GitHub](https://github.com/lucasastorian/veclite)** | **[Issues](https://github.com/lucasastorian/veclite/issues)**
