"""Test sync DELETE operations."""


def test_delete_single_row(sync_client):
    """Test deleting a single row."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").delete().eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"

    remaining = sync_client.table("users").select().execute()
    assert len(remaining.data) == 1
    assert remaining.data[0]["name"] == "Bob"


def test_delete_multiple_rows(sync_client):
    """Test deleting multiple rows."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").delete().gte("age", 30).execute()

    assert len(result.data) == 2

    remaining = sync_client.table("users").select().execute()
    assert len(remaining.data) == 1
    assert remaining.data[0]["name"] == "Bob"


def test_delete_with_no_matches(sync_client):
    """Test that delete with no matches returns empty result."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").delete().eq("name", "Nonexistent").execute()

    assert len(result.data) == 0

    remaining = sync_client.table("users").select().execute()
    assert len(remaining.data) == 1


def test_delete_all_rows(sync_client):
    """Test deleting all rows (no predicate)."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").delete().execute()

    assert len(result.data) == 2

    remaining = sync_client.table("users").select().execute()
    assert len(remaining.data) == 0
