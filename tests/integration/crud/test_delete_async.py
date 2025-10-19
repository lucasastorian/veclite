"""Test async DELETE operations."""
import pytest


@pytest.mark.asyncio
async def test_delete_single_row(async_client):
    """Test deleting a single row."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = await async_client.table("users").delete().eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"

    remaining = await async_client.table("users").select().execute()
    assert len(remaining.data) == 1
    assert remaining.data[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_delete_multiple_rows(async_client):
    """Test deleting multiple rows."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = await async_client.table("users").delete().gte("age", 30).execute()

    assert len(result.data) == 2

    remaining = await async_client.table("users").select().execute()
    assert len(remaining.data) == 1
    assert remaining.data[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_delete_all_rows(async_client):
    """Test deleting all rows (no predicate)."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = await async_client.table("users").delete().execute()

    assert len(result.data) == 2

    remaining = await async_client.table("users").select().execute()
    assert len(remaining.data) == 0
