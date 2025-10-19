"""Test sync INSERT operations."""


def test_insert_single_row(sync_client):
    """Test inserting a single row."""
    result = sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": 30}).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"
    assert result.data[0]["email"] == "alice@example.com"
    assert result.data[0]["age"] == 30
    assert result.data[0]["id"] == 1


def test_insert_multiple_rows(sync_client):
    """Test inserting multiple rows."""
    rows = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    result = sync_client.table("users").insert(rows).execute()

    assert len(result.data) == 3
    assert result.data[0]["name"] == "Alice"
    assert result.data[1]["name"] == "Bob"
    assert result.data[2]["name"] == "Charlie"


def test_insert_with_null_values(sync_client):
    """Test inserting rows with NULL values."""
    result = sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com", "age": None}).execute()

    assert result.data[0]["age"] is None


def test_insert_with_missing_optional_field(sync_client):
    """Test that inserting with missing optional fields works (email is unique but nullable)."""
    result = sync_client.table("users").insert({"name": "Alice", "email": "alice@example.com"}).execute()

    assert result.data[0]["name"] == "Alice"
    assert result.data[0]["age"] is None


def test_insert_json_field(sync_product_client):
    """Test inserting JSON data."""
    metadata = {"tags": ["electronics", "sale"], "rating": 4.5}
    result = sync_product_client.table("products").insert({
        "name": "Laptop",
        "price": 999.99,
        "metadata": metadata
    }).execute()

    assert result.data[0]["metadata"] == metadata
