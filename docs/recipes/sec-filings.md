# Recipe: SEC Filings (Relational + FTS + Vectors)

Use three tables in a single DB to support both precise page reads and high‑quality semantic retrieval.

## Tables

- `filings`: metadata (ticker, form type, date)
- `filing_pages`: page text (FTS‑only)
- `filing_chunks`: chunked text (vectors + FTS)

## Schema

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
```

## Hybrid Retrieval + Page Window

```python
q = "Apple risk factors and competitive challenges"
hits = client.table("filing_chunks").hybrid_search(q, topk=10, alpha=0.7) \
    .eq("filing_id", 12345).execute()

best = hits.data[0]
page = best["page"]
filing_id = best["filing_id"]

pages = client.table("filing_pages").select("*") \
    .eq("filing_id", filing_id) \
    .between("page_number", page - 1, page + 1) \
    .order("page_number") \
    .execute()
```

This pattern gives agents the ability to retrieve by meaning and then read exact page ranges for context windows.

## Company-Enriched Views (No Denormalization)

Use a SQL view to project company metadata onto chunks for filtering without duplicating fields or updating every chunk when company data changes.

```python
from veclite.schema.view import View, Field

class CompanyFilingChunks(View):
    __viewname__ = "company_filing_chunks"
    __tables__ = (FilingChunks, Filings, Companies)  # FK path: chunks → filings → companies

    # IMPORTANT: expose underlying chunk primary key as 'id'
    id = Field(table="filing_chunks", field="id")

    # Chunk fields
    filing_id = Field(table="filing_chunks", field="filing_id")
    page = Field(table="filing_chunks", field="page")
    content = Field(table="filing_chunks", field="content")  # vector + FTS-enabled on source

    # Filing fields
    form = Field(table="filings", field="form")
    filing_date = Field(table="filings", field="filing_date")

    # Company fields
    company_id = Field(table="filings", field="company_id")
    company_name = Field(table="companies", field="name")
    company_sector = Field(table="companies", field="sector")
    market_cap = Field(table="companies", field="market_cap")

# Register the view
schema.add_view(CompanyFilingChunks)

# Vector search over the VIEW, filter by company metadata
hits = client.table("company_filing_chunks").vector_search(
    query="supply chain risks",
    topk=10
).gte("market_cap", 1e10).eq("company_sector", "Technology").execute()
```

Notes:
- Views used with `vector_search()` must expose an `id` field that maps to the underlying table's primary key.
- VecLite resolves the view field back to the source vector store automatically — no duplication of vectors required.
- This avoids the NoSQL pitfall where you’d need to re-write every chunk when company metadata changes.
