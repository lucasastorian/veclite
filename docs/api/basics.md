# API Basics

## Query Builder

```python
# Select
rows = client.table("docs").select("id, title").order("id", desc=False).limit(20).execute()

# Filters
client.table("docs").select().eq("category", "AI").gt("year", 2020).execute()

# JSON contains
client.table("docs").contains("tags", "transformers").execute()

# Regex / ILIKE
client.table("docs").regex("title", r"^Intro").execute()
client.table("docs").ilike("title", "python").execute()
```

## Filters Reference

All filters are chainable and combined with AND logic.

- `eq(field, value)` — equality
  ```python
  client.table("docs").select().eq("category", "AI").execute()
  ```

- `neq(field, value)` — not equal

- `gt/gte/lt/lte(field, value)` — numeric/date comparisons (skips if value is None)
  ```python
  client.table("docs").select().gt("year", 2020).execute()
  ```

- `between(field, lower, upper)` — inclusive range; either bound can be None
  ```python
  client.table("docs").select().between("year", 2018, 2022).execute()
  ```

- `in_(field, values)` / `not_in(field, values)` — membership
  ```python
  client.table("docs").select().in_("category", ["AI", "DB"]).execute()
  ```

- `is_null(field)` / `is_not_null(field)`

- `contains(field, value)` — JSON contains
  - If the column is a JSON array: checks element containment
  - If the column is a JSON object: checks key existence
  ```python
  client.table("docs").contains("tags", "transformers").execute()
  client.table("docs").contains("metadata", "author").execute()  # key in JSON object
  ```

- `ilike(field, pattern)` — case‑insensitive LIKE
  - If pattern lacks `%` or `_`, a `%...%` wildcard is added automatically
  ```python
  client.table("docs").ilike("title", "python").execute()
  client.table("docs").ilike("title", "%intro%").execute()
  ```

- `regex(field, pattern)` — case‑insensitive REGEXP
  ```python
  client.table("docs").regex("title", r"^intro").execute()
  ```

Ordering and limiting:
- `order(field, desc=False)`
- `limit(n)`

## Insert / Upsert / Update / Delete

```python
# Insert
client.table("docs").insert({"title": "A", "content": "hello"}).execute()

# Upsert by unique key (insert or update)
client.table("docs").upsert(
    {"id": 1, "title": "A+", "content": "hello world"},
    on_conflict="id"
).execute()

# Update
client.table("docs").update({"title": "Updated"}).eq("id", 1).execute()

# Delete
client.table("docs").delete().eq("id", 1).execute()

# Async atomic batch (optional)
async with async_client.batch_embeddings():
    await async_client.table("docs").insert(batch).execute()
```
