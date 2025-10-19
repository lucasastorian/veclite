# Inserting Data

Insert single rows or lists of rows.

```python
client.table("docs").insert({"title": "A", "content": "hello"}).execute()
client.table("docs").insert([
  {"title": "B", "content": "world"},
]).execute()
```

Async with atomic batch embeddings:

```python
async with async_client.batch_embeddings():
    await async_client.table("docs").insert(batch).execute()
```

