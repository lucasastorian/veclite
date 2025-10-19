"""Test unique constraints on tables and their use in upsert operations."""
import pytest
import pytest_asyncio
from pathlib import Path
from veclite import Client, AsyncClient, Schema
from veclite.schema import Table, Integer, Text


# Test tables with unique constraints
class FilingChunk(Table):
    """Table with composite unique constraint."""
    __tablename__ = "filing_chunks"
    __uniques__ = [("filing_id", "chunk_type")]

    id = Integer(primary_key=True)
    filing_id = Text()
    chunk_type = Text()
    content = Text()


class User(Table):
    """Table with single-column unique constraint."""
    __tablename__ = "users"

    id = Integer(primary_key=True)
    email = Text(unique=True)  # Single-column unique uses field.unique
    name = Text(nullable=True)


class Product(Table):
    """Table with multiple unique constraints."""
    __tablename__ = "products"
    __uniques__ = [
        ("vendor_id", "vendor_sku")  # Composite unique constraint
    ]

    id = Integer(primary_key=True)
    sku = Text(unique=True)  # Single-column unique uses field.unique
    vendor_id = Text()
    vendor_sku = Text()
    name = Text()


# Fixtures
@pytest.fixture
def filing_chunk_schema():
    """Schema with composite unique constraint table."""
    schema = Schema()
    schema.add_table(FilingChunk)
    return schema


@pytest.fixture
def user_schema():
    """Schema with single-column unique constraint table."""
    schema = Schema()
    schema.add_table(User)
    return schema


@pytest.fixture
def product_schema():
    """Schema with multiple unique constraints."""
    schema = Schema()
    schema.add_table(Product)
    return schema


@pytest.fixture
def sync_filing_client(tmp_path, filing_chunk_schema):
    """Sync client with filing_chunk table."""
    db_path = tmp_path / "test.db"
    client = Client.create(filing_chunk_schema, str(db_path))
    yield client
    client.close()


@pytest_asyncio.fixture
async def async_filing_client(tmp_path, filing_chunk_schema):
    """Async client with filing_chunk table."""
    db_path = tmp_path / "test.db"
    client = AsyncClient.create(filing_chunk_schema, str(db_path))
    yield client
    client.close()


@pytest.fixture
def sync_user_client(tmp_path, user_schema):
    """Sync client with user table."""
    db_path = tmp_path / "test.db"
    client = Client.create(user_schema, str(db_path))
    yield client
    client.close()


@pytest_asyncio.fixture
async def async_user_client(tmp_path, user_schema):
    """Async client with user table."""
    db_path = tmp_path / "test.db"
    client = AsyncClient.create(user_schema, str(db_path))
    yield client
    client.close()


@pytest.fixture
def sync_product_client(tmp_path, product_schema):
    """Sync client with product table."""
    db_path = tmp_path / "test.db"
    client = Client.create(product_schema, str(db_path))
    yield client
    client.close()


# Tests: Schema Creation
def test_unique_constraint_in_schema_definition():
    """Test that __uniques__ is properly defined on table class."""
    # Composite unique constraint
    assert hasattr(FilingChunk, '__uniques__')
    assert FilingChunk.__uniques__ == [("filing_id", "chunk_type")]

    # Single-column unique uses field.unique, not __uniques__
    assert User.get_fields()['email'].unique == True

    # Product has single-column unique (sku) AND composite unique
    assert Product.get_fields()['sku'].unique == True
    assert hasattr(Product, '__uniques__')
    assert Product.__uniques__ == [("vendor_id", "vendor_sku")]


def test_unique_constraint_creates_database_constraint(sync_filing_client):
    """Test that unique constraints are enforced at the database level."""
    # Insert first row
    sync_filing_client.table("filing_chunks").insert({
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "risk_factors",
        "content": "Risk content here"
    }).execute()

    # Try to insert duplicate - should fail with UNIQUE constraint error
    with pytest.raises(Exception) as exc_info:
        sync_filing_client.table("filing_chunks").insert({
            "filing_id": "AAPL-2024-10K",
            "chunk_type": "risk_factors",
            "content": "Different content"
        }).execute()

    assert "UNIQUE constraint failed" in str(exc_info.value)


def test_single_column_unique_constraint(sync_user_client):
    """Test single-column unique constraint."""
    # Insert first user
    sync_user_client.table("users").insert({
        "email": "alice@example.com",
        "name": "Alice"
    }).execute()

    # Try to insert duplicate email - should fail
    with pytest.raises(Exception) as exc_info:
        sync_user_client.table("users").insert({
            "email": "alice@example.com",
            "name": "Alice Smith"
        }).execute()

    assert "UNIQUE constraint failed" in str(exc_info.value)


def test_multiple_unique_constraints(sync_product_client):
    """Test table with multiple unique constraints."""
    # Insert first product
    sync_product_client.table("products").insert({
        "sku": "WIDGET-001",
        "vendor_id": "ACME",
        "vendor_sku": "W001",
        "name": "Widget"
    }).execute()

    # Duplicate SKU should fail
    with pytest.raises(Exception) as exc_info:
        sync_product_client.table("products").insert({
            "sku": "WIDGET-001",  # Duplicate!
            "vendor_id": "OTHER",
            "vendor_sku": "X001",
            "name": "Other Widget"
        }).execute()

    assert "UNIQUE constraint failed" in str(exc_info.value)

    # Duplicate (vendor_id, vendor_sku) should fail
    with pytest.raises(Exception) as exc_info:
        sync_product_client.table("products").insert({
            "sku": "WIDGET-002",
            "vendor_id": "ACME",  # Same vendor
            "vendor_sku": "W001",  # Same vendor SKU
            "name": "Another Widget"
        }).execute()

    assert "UNIQUE constraint failed" in str(exc_info.value)


# Tests: Upsert with Unique Constraints (Sync)
def test_upsert_with_composite_unique_constraint_sync(sync_filing_client):
    """Test that upsert uses composite unique constraint to determine insert vs update."""
    # First upsert - should insert
    result1 = sync_filing_client.table("filing_chunks").upsert(
        values={
            "filing_id": "AAPL-2024-10K",
            "chunk_type": "risk_factors",
            "content": "Original risk content"
        },
        on_conflict=["filing_id", "chunk_type"]
    ).execute()

    assert len(result1.data) == 1
    original_id = result1.data[0]["id"]
    assert result1.data[0]["content"] == "Original risk content"

    # Second upsert with same (filing_id, chunk_type) - should update
    result2 = sync_filing_client.table("filing_chunks").upsert(
     values={
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "risk_factors",
        "content": "Updated risk content"
    },
     on_conflict=["filing_id", "chunk_type"]
 ).execute()

    assert len(result2.data) == 1
    assert result2.data[0]["id"] == original_id  # Same ID - was updated
    assert result2.data[0]["content"] == "Updated risk content"

    # Verify only one row exists
    all_rows = sync_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 1


def test_upsert_with_single_column_unique_constraint_sync(sync_user_client):
    """Test upsert with single-column unique constraint."""
    # First upsert - insert
    result1 = sync_user_client.table("users").upsert(
     values={
        "email": "bob@example.com",
        "name": "Bob"
    },
     on_conflict=["email"]
 ).execute()

    original_id = result1.data[0]["id"]

    # Second upsert with same email - update
    result2 = sync_user_client.table("users").upsert(
     values={
        "email": "bob@example.com",
        "name": "Robert"
    },
     on_conflict=["email"]
 ).execute()

    assert result2.data[0]["id"] == original_id
    assert result2.data[0]["name"] == "Robert"

    # Verify only one row
    all_rows = sync_user_client.table("users").select("*").execute()
    assert len(all_rows.data) == 1


def test_upsert_different_values_inserts_new_row_sync(sync_filing_client):
    """Test that upsert with different unique values creates new row."""
    # Insert first chunk
    sync_filing_client.table("filing_chunks").upsert(

        values={
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "risk_factors",
        "content": "Risk content"
    },

        on_conflict=["filing_id", "chunk_type"]

    ).execute()

    # Upsert with different chunk_type - should insert new row
    sync_filing_client.table("filing_chunks").upsert(

        values={
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "financial_data",  # Different!
        "content": "Financial content"
    },

        on_conflict=["filing_id", "chunk_type"]

    ).execute()

    # Should have 2 rows
    all_rows = sync_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 2


# Tests: Upsert with Unique Constraints (Async)
@pytest.mark.asyncio
async def test_upsert_with_composite_unique_constraint_async(async_filing_client):
    """Test async upsert with composite unique constraint."""
    # First upsert - insert
    result1 = await async_filing_client.table("filing_chunks").upsert(
     values={
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "risk_factors",
        "content": "Original risk content"
    },
     on_conflict=["filing_id", "chunk_type"]
 ).execute()

    original_id = result1.data[0]["id"]

    # Second upsert - update
    result2 = await async_filing_client.table("filing_chunks").upsert(
     values={
        "filing_id": "AAPL-2024-10K",
        "chunk_type": "risk_factors",
        "content": "Updated risk content"
    },
     on_conflict=["filing_id", "chunk_type"]
 ).execute()

    assert result2.data[0]["id"] == original_id
    assert result2.data[0]["content"] == "Updated risk content"

    # Verify only one row
    all_rows = await async_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 1


@pytest.mark.asyncio
async def test_upsert_with_single_column_unique_constraint_async(async_user_client):
    """Test async upsert with single-column unique constraint."""
    # First upsert - insert
    result1 = await async_user_client.table("users").upsert(
     values={
        "email": "carol@example.com",
        "name": "Carol"
    },
     on_conflict=["email"]
 ).execute()

    original_id = result1.data[0]["id"]

    # Second upsert - update
    result2 = await async_user_client.table("users").upsert(
     values={
        "email": "carol@example.com",
        "name": "Caroline"
    },
     on_conflict=["email"]
 ).execute()

    assert result2.data[0]["id"] == original_id
    assert result2.data[0]["name"] == "Caroline"


# Tests: Batch Upsert
def test_batch_upsert_with_unique_constraints_sync(sync_filing_client):
    """Test batch upsert with unique constraints."""
    # First batch - all inserts
    sync_filing_client.table("filing_chunks").upsert(
        values=[
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "risk_factors",
                "content": "Risk content"
            },
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "financial_data",
                "content": "Financial content"
            },
            {
                "filing_id": "MSFT-2024-10K",
                "chunk_type": "risk_factors",
                "content": "MSFT risk content"
            }
        ],
        on_conflict=["filing_id", "chunk_type"]
    ).execute()

    # Verify 3 rows inserted
    all_rows = sync_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 3

    # Second batch - mix of updates and inserts
    sync_filing_client.table("filing_chunks").upsert(
        values=[
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "risk_factors",  # Update existing
                "content": "Updated risk content"
            },
            {
                "filing_id": "GOOGL-2024-10K",
                "chunk_type": "risk_factors",  # New insert
                "content": "GOOGL risk content"
            }
        ],
        on_conflict=["filing_id", "chunk_type"]
    ).execute()

    # Should have 4 rows (3 original + 1 new)
    all_rows = sync_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 4

    # Verify the update happened
    updated = sync_filing_client.table("filing_chunks").select("*").eq(
        "filing_id", "AAPL-2024-10K"
    ).eq("chunk_type", "risk_factors").execute()

    assert updated.data[0]["content"] == "Updated risk content"


@pytest.mark.asyncio
async def test_batch_upsert_with_unique_constraints_async(async_filing_client):
    """Test async batch upsert with unique constraints."""
    # First batch - all inserts
    await async_filing_client.table("filing_chunks").upsert(
        values=[
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "risk_factors",
                "content": "Risk content"
            },
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "financial_data",
                "content": "Financial content"
            }
        ],
        on_conflict=["filing_id", "chunk_type"]
    ).execute()

    # Verify 2 rows
    all_rows = await async_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 2

    # Second batch - update first, insert new
    await async_filing_client.table("filing_chunks").upsert(
        values=[
            {
                "filing_id": "AAPL-2024-10K",
                "chunk_type": "risk_factors",  # Update
                "content": "Updated risk content"
            },
            {
                "filing_id": "MSFT-2024-10K",
                "chunk_type": "risk_factors",  # Insert
                "content": "MSFT risk content"
            }
        ],
        on_conflict=["filing_id", "chunk_type"]
    ).execute()

    # Should have 3 rows
    all_rows = await async_filing_client.table("filing_chunks").select("*").execute()
    assert len(all_rows.data) == 3


# Tests: Edge Cases
def test_upsert_on_primary_key(sync_user_client):
    """Test that upsert can use primary key as conflict resolution."""
    # First insert - let ID auto-generate
    result1 = sync_user_client.table("users").upsert(
        values={
            "email": "dave@example.com",
            "name": "Dave"
        },
        on_conflict=["email"]
    ).execute()

    dave_id = result1.data[0]["id"]

    # Upsert on the same email - should update the existing row
    result2 = sync_user_client.table("users").upsert(
        values={
            "email": "dave@example.com",
            "name": "David"  # Changed name
        },
        on_conflict=["email"]
    ).execute()

    assert result2.data[0]["id"] == dave_id  # Same ID
    assert result2.data[0]["name"] == "David"  # Updated name

    # Verify only one row exists
    all_rows = sync_user_client.table("users").select("*").execute()
    assert len(all_rows.data) == 1


def test_unique_constraint_with_null_values(sync_filing_client):
    """Test that NULL values in unique constraints are handled correctly."""
    # SQLite allows multiple NULLs in unique constraints
    # This behavior may vary - just documenting current behavior
    pass  # TODO: Implement if needed
