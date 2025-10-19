"""Test async UPDATE operations."""
import pytest


@pytest.mark.asyncio
async def test_update_single_row(async_client):
    """Test updating a single row."""
    await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = await async_client.table("users").update({"age": 31}).eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["age"] == 31


@pytest.mark.asyncio
async def test_update_multiple_rows(async_client):
    """Test updating multiple rows."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = await async_client.table("users").update({"age": 40}).gte("age", 30).execute()

    assert len(result.data) == 2
    assert all(row["age"] == 40 for row in result.data)


@pytest.mark.asyncio
async def test_update_with_null_value(async_client):
    """Test updating to NULL value."""
    await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = await async_client.table("users").update({"age": None}).eq("name", "Alice").execute()

    assert result.data[0]["age"] is None
