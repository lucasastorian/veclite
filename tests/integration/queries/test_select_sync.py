"""Test sync SELECT operations and predicates."""


def test_select_all_rows(sync_client):
    """Test selecting all rows."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().execute()

    assert len(result.data) == 2


def test_select_with_eq_predicate(sync_client):
    """Test SELECT with eq() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().eq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


def test_select_with_neq_predicate(sync_client):
    """Test SELECT with neq() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().neq("name", "Alice").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Bob"


def test_select_with_in_predicate(sync_client):
    """Test SELECT with in_() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").select().in_("name", ["Alice", "Charlie"]).execute()

    assert len(result.data) == 2
    assert {row["name"] for row in result.data} == {"Alice", "Charlie"}


def test_select_with_gt_predicate(sync_client):
    """Test SELECT with gt() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").select().gt("age", 30).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Charlie"


def test_select_with_gte_predicate(sync_client):
    """Test SELECT with gte() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().gte("age", 30).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


def test_select_with_lt_predicate(sync_client):
    """Test SELECT with lt() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().lt("age", 30).execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Bob"


def test_select_with_lte_predicate(sync_client):
    """Test SELECT with lte() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").select().lte("age", 30).execute()

    assert len(result.data) == 2
    assert {row["name"] for row in result.data} == {"Alice", "Bob"}


def test_select_with_is_null_predicate(sync_client):
    """Test SELECT with is_null() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": None},
    ]).execute()

    result = sync_client.table("users").select().is_null("age").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Bob"


def test_select_with_is_not_null_predicate(sync_client):
    """Test SELECT with is_not_null() predicate."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": None},
    ]).execute()

    result = sync_client.table("users").select().is_not_null("age").execute()

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Alice"


def test_select_with_chained_predicates(sync_client):
    """Test SELECT with multiple chained predicates (AND logic)."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").select().gte("age", 25).lte("age", 30).execute()

    assert len(result.data) == 2
    assert {row["name"] for row in result.data} == {"Alice", "Bob"}


def test_select_with_limit(sync_client):
    """Test SELECT with limit()."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]).execute()

    result = sync_client.table("users").select().limit(2).execute()

    assert len(result.data) == 2


def test_select_with_order_by(sync_client):
    """Test SELECT with order()."""
    sync_client.table("users").insert([
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    result = sync_client.table("users").select().order("age").execute()

    assert result.data[0]["name"] == "Bob"
    assert result.data[1]["name"] == "Alice"
    assert result.data[2]["name"] == "Charlie"


def test_count(sync_client):
    """Test count() method."""
    sync_client.table("users").insert([
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]).execute()

    count = sync_client.table("users").select().count()

    assert count == 2
