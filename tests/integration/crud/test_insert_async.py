"""Test async INSERT operations."""
import pytest


@pytest.mark.asyncio
async def test_insert_single_row(async_client):
    """Test inserting a single row."""
    result = await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"
    assert result.data[0]["email"] == "alice@example.com"
    assert result.data[0]["age"] == 30
    assert result.data[0]["id"] == 1


@pytest.mark.asyncio
async def test_insert_multiple_rows(async_client):
    """Test inserting multiple rows."""
    rows = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    result = await async_client.table("users").insert(rows).execute()

    assert len(result.data) == 3
    assert result.data[0]["name"] == "Alice"
    assert result.data[1]["name"] == "Bob"
    assert result.data[2]["name"] == "Charlie"


@pytest.mark.asyncio
async def test_insert_with_null_values(async_client):
    """Test inserting rows with NULL values."""
    result = await async_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": None}).execute()

    assert result.data[0]["age"] is None
