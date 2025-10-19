"""Test async UPSERT operations."""
import pytest


@pytest.mark.asyncio
async def test_upsert_single_row_insert(async_client):
    """Test upsert inserts when row doesn't exist."""
    result = await async_client.table("users").upsert(
        {"email": "alice@example.com", "name": "Alice", "age": 30},
        on_conflict="email"
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_upsert_single_row_update(async_client):
    """Test upsert updates when row exists."""
    await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = await async_client.table("users").upsert(
        {"email": "alice@example.com", "name": "Alice Updated", "age": 31},
        on_conflict="email"
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice Updated"
    assert result.data[0]["age"] == 31

    rows = await async_client.table("users").select().execute()
    assert len(rows.data) == 1


@pytest.mark.asyncio
async def test_upsert_large_batch(async_client):
    """Test upsert with large batch (> max_vars, triggers executemany)."""
    rows = [
        {"email": f"user{i}@example.com", "name": f"User{i}", "age": 20 + i}
        for i in range(1000)
    ]
    result = await async_client.table("users").upsert(rows, on_conflict="email").execute()

    assert result.count == 1000

    count = await async_client.table("users").select().count()
    assert count == 1000


@pytest.mark.asyncio
async def test_upsert_by_primary_key(async_client):
    """Test upsert on primary key."""
    await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = await async_client.table("users").upsert(
        {"id": 1, "name": "Alice Updated", "email": "alice@example.com", "age": 31},
        on_conflict="id"
    ).execute()

    assert result.data[0]["name"] == "Alice Updated"
