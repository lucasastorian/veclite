# Search Overview

VecLite provides three search modes in one local database — keyword (BM25), vector (semantic), hybrid (fusion) — plus an optional rerank modifier.

## Keyword (BM25)
```python
hits = client.table("docs").keyword_search("transformer architecture", topk=10).execute()
```

## Vector (Cosine)
```python
hits = client.table("docs").vector_search("AI tutorials for beginners", topk=10).execute()
```

## Hybrid (Best of Both)
```python
hits = client.table("docs").hybrid_search(
    query="neural network tutorial",
    alpha=0.7,  # 70% vector, 30% keyword
    topk=10
).execute()
```

## Rerank Modifier (Optional)
```python
from veclite.embeddings import VoyageClient
embedder = VoyageClient()
candidates = client.table("docs").hybrid_search("quantum computing", topk=100).execute()
reranked = embedder.rerank(
    query="quantum computing applications",
    documents=[d["content"] for d in candidates.data],
    top_k=10
)
```
