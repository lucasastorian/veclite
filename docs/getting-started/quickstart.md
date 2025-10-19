# Quickstart

Get up and running with VecLite in minutes.

## Step 1: Install

```bash
pip install veclite[voyage]
export VOYAGE_API_KEY="your-key"
```

## Step 2: Define Your Schema

Create a schema by defining table classes:

```python
from veclite.schema import Table, Integer, Text

class Document(Table):
    id = Integer(primary_key=True)
    title = Text()
    content = Text(vector=True, fts=True)  # vectors + BM25
```

## Step 3: Create Database

```python
from veclite import Client, Schema

schema = Schema()
schema.add_table(Document)

# Nested folder with sqlite.db and vectors/
client = Client.create(schema, "rag_db")
```

## Step 4: Insert Data

```python
client.table("documents").insert([
    {"title": "Python Guide", "content": "Learn Python programming"},
    {"title": "ML Tutorial", "content": "Neural networks and deep learning"},
    {"title": "Database Tips", "content": "Optimize SQL queries"},
]).execute()
```

## Step 5: Query Data

### Vector Search (semantic)
```python
hits = client.table("documents").vector_search(
    query="AI and deep learning",
    topk=5
).execute()
```

### Keyword Search (BM25)
```python
hits = client.table("documents").keyword_search(
    query="SQL optimization",
    topk=5
).execute()
```

### Hybrid Search (best of both)
```python
hits = client.table("documents").hybrid_search(
    query="neural network tutorial",
    alpha=0.7,
    topk=5
).execute()
```

## Atomic Batch Embeddings (async)
```python
from veclite import AsyncClient
aclient = AsyncClient.create(schema, "rag_db")
async with aclient.batch_embeddings():
    await aclient.table("documents").insert([...]).execute()
```

## What's Next?

- Search basics (overview): ../search/overview.md
- SEC filings recipe: ../recipes/sec-filings.md
