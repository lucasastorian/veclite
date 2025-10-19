"""Test sync UPDATE operations."""


def test_update_single_row(sync_client):
    """Test updating a single row."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").update({"age": 31}).eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["age"] == 31


def test_update_multiple_rows(sync_client):
    """Test updating multiple rows."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").update({"age": 40}).gte("age", 30).execute()

    assert len(result.data) == 2
    assert all(row["age"] == 40 for row in result.data)


def test_update_with_null_value(sync_client):
    """Test updating to NULL value."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").update({"age": None}).eq("name", "Alice").execute()

    assert result.data[0]["age"] is None


def test_update_without_predicate_updates_all(sync_client):
    """Test that update without predicate updates all rows."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").update({"age": 99}).execute()

    assert len(result.data) == 2
    assert all(row["age"] == 99 for row in result.data)


def test_update_no_matches_returns_empty(sync_client):
    """Test that update with no matches returns empty result."""
    sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    result = sync_client.table("users").update({"age": 40}).eq("name", "Nonexistent").execute()

    assert len(result.data) == 0


def test_update_json_field(sync_product_client):
    """Test updating JSON field."""
    sync_product_client.table("products").insert({
        "name": "Laptop",
        "price": 999.99,
        "metadata": {"tags": ["electronics"]}
    }).execute()

    new_metadata = {"tags": ["electronics", "sale"], "discount": 0.1}
    result = sync_product_client.table("products").update({"metadata": new_metadata}).eq("name", "Laptop").execute()

    assert result.data[0]["metadata"] == new_metadata
