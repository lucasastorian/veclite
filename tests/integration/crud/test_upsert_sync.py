"""Test sync UPSERT operations and edge cases."""
import pytest


def test_upsert_single_row_insert(sync_client):
    """Test upsert inserts when row doesn't exist."""
    result = sync_client.table("users").upsert(
        {"email": "alice@example.com", "name": "Alice", "age": 30},
        on_conflict="email"
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


def test_upsert_single_row_update(sync_client):
    """Test upsert updates when row exists."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").upsert(
        {"email": "alice@example.com", "name": "Alice Updated", "age": 31},
        on_conflict="email"
    ).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice Updated"
    assert result.data[0]["age"] == 31

    rows = sync_client.table("users").select().execute()
    assert len(rows.data) == 1


def test_upsert_multiple_rows_small_batch(sync_client):
    """Test upsert with small batch (< max_vars)."""
    rows = [
        {"email": f"user{i}@example.com", "name": f"User{i}", "age": 20 + i}
        for i in range(10)
    ]
    result = sync_client.table("users").upsert(rows, on_conflict="email").execute()

    assert len(result.data) == 10


def test_upsert_multiple_rows_large_batch(sync_client):
    """Test upsert with large batch (> max_vars, triggers executemany)."""
    rows = [
        {"email": f"user{i}@example.com", "name": f"User{i}", "age": 20 + i}
        for i in range(1000)
    ]
    result = sync_client.table("users").upsert(rows, on_conflict="email").execute()

    assert result.count == 1000

    count = sync_client.table("users").select().count()
    assert count == 1000


def test_upsert_mixed_insert_and_update(sync_client):
    """Test upsert with mix of existing and new rows."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").upsert([
        {"email": "alice@example.com", "name": "Alice Updated", "age": 31},
        {"email": "charlie@example.com", "name": "Charlie", "age": 35},
    ], on_conflict="email").execute()

    assert len(result.data) == 2

    count = sync_client.table("users").select().count()
    assert count == 3


def test_upsert_by_primary_key(sync_client):
    """Test upsert on primary key."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").upsert(
        {"id": 1, "name": "Alice Updated", "email": "alice@example.com", "age": 31},
        on_conflict="id"
    ).execute()

    assert result.data[0]["name"] == "Alice Updated"


def test_upsert_ignore_duplicates(sync_client):
    """Test upsert with ignore_duplicates=True."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").upsert(
        {"email": "alice@example.com", "name": "Should Not Update", "age": 99},
        on_conflict="email",
        ignore_duplicates=True
    ).execute()

    rows = sync_client.table("users").select().execute()
    assert rows.data[0]["name"] == "Alice"
    assert rows.data[0]["age"] == 30


def test_upsert_with_partial_data_and_pk(sync_client):
    """Test upsert by PK with partial data (uses SELECT-based INSERT)."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").upsert(
        {"id": 1, "age": 31},
        on_conflict="id"
    ).execute()

    assert result.data[0]["name"] == "Alice"
    assert result.data[0]["age"] == 31


def test_upsert_fails_on_invalid_conflict_column(sync_client):
    """Test that upsert fails on non-unique conflict column."""
    with pytest.raises(ValueError, match="must have UNIQUE constraint"):
        sync_client.table("users").upsert(
            {"name": "Alice", "email": "alice@example.com", "age": 30},
            on_conflict="age"
        ).execute()
