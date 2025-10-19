"""Test async SELECT operations and predicates."""
import pytest


@pytest.mark.asyncio
async def test_select_all_rows(async_client):
    """Test selecting all rows."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = await async_client.table("users").select().execute()

    assert len(result.data) == 2


@pytest.mark.asyncio
async def test_select_with_eq_predicate(async_client):
    """Test SELECT with eq() predicate."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = await async_client.table("users").select().eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_select_with_in_predicate(async_client):
    """Test SELECT with in_() predicate."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = await async_client.table("users").select().in_("name", ["Alice", "Charlie"]).execute()

    assert len(result.data) == 2
    assert {row["name"] for row in result.data} == {"Alice", "Charlie"}


@pytest.mark.asyncio
async def test_select_with_chained_predicates(async_client):
    """Test SELECT with multiple chained predicates (AND logic)."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = await async_client.table("users").select().gte("age", 25).lte("age", 30).execute()

    assert len(result.data) == 2
    assert {row["name"] for row in result.data} == {"Alice", "Bob"}


@pytest.mark.asyncio
async def test_count(async_client):
    """Test count() method."""
    await async_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    count = await async_client.table("users").select().count()

    assert count == 2
