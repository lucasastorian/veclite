"""Test sync batch_embeddings context manager."""
import tempfile
import pytest
import os
from pathlib import Path
from veclite import Client, Schema
from veclite.schema import Table, Integer, Text
from tests.fixtures.mock_embedder import MockEmbedder


@pytest.fixture
def sync_client_regular():
    """Create sync client with regular vector field."""
    # Set fake API key for testing
    os.environ["VOYAGE_API_KEY"] = "sk-test-key-12345"

    class Document(Table):
        __tablename__ = "documents"
        id = Integer(primary_key=True)
        content = Text(vector=True, fts=True)

    schema = Schema()
    schema.add_table(Document)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        client = Client.create(schema, str(db_path))
        client.embedder = MockEmbedder(dimension=512)

        # Add mock embeddings
        for i in range(10):
            client.embedder.add_mock(f"doc{i}", [float(i)] + [0.0] * 511)
        client.embedder.add_mock("query", [1.0] + [0.0] * 511)

        yield client
        client.close()


@pytest.fixture
def sync_client_contextual():
    """Create sync client with contextualized vector field."""
    # Set fake API key for testing
    os.environ["VOYAGE_API_KEY"] = "sk-test-key-12345"

    class Document(Table):
        __tablename__ = "documents"
        id = Integer(primary_key=True)
        content = Text(vector=True, contextualized=True, fts=True)

    schema = Schema()
    schema.add_table(Document)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        client = Client.create(schema, str(db_path))
        client.embedder = MockEmbedder(dimension=512)

        # Add mock embeddings
        for i in range(10):
            client.embedder.add_mock(f"doc{i}", [float(i)] + [0.0] * 511)
        client.embedder.add_mock("query", [1.0] + [0.0] * 511)

        yield client
        client.close()


def test_batch_embeddings_sync_regular_happy_path(sync_client_regular):
    """Test batch_embeddings with regular vectors - happy path."""
    # Use batch_embeddings context manager
    with sync_client_regular.batch_embeddings():
        # Insert multiple documents
        sync_client_regular.table("documents").insert([
            {"content": "doc0"},
            {"content": "doc1"},
            {"content": "doc2"},
        ]).execute()

        # Upsert more documents
        sync_client_regular.table("documents").upsert(
            values={"content": "doc3"},
            on_conflict=["id"]
        ).execute()

        sync_client_regular.table("documents").upsert(
            values={"content": "doc4"},
            on_conflict=["id"]
        ).execute()

    # After context exits, verify all rows exist
    result = sync_client_regular.table("documents").select("*").execute()
    assert len(result.data) == 5

    # Verify vectors were written - try vector search
    search_result = sync_client_regular.table("documents").vector_search(
        query="query",
        topk=5
    ).execute()
    assert len(search_result.data) == 5


def test_batch_embeddings_sync_regular_error_rollback(sync_client_regular):
    """Test batch_embeddings with regular vectors - rollback on error."""
    # Insert some initial data
    sync_client_regular.table("documents").insert([
        {"content": "doc0"},
    ]).execute()

    # Verify initial state
    initial_result = sync_client_regular.table("documents").select("*").execute()
    assert len(initial_result.data) == 1

    # Try to insert within context manager but raise error
    try:
        with sync_client_regular.batch_embeddings():
            sync_client_regular.table("documents").insert([
                {"content": "doc1"},
                {"content": "doc2"},
            ]).execute()

            # Raise error to trigger rollback
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # After rollback, should still have only 1 row
    final_result = sync_client_regular.table("documents").select("*").execute()
    assert len(final_result.data) == 1
    assert final_result.data[0]["content"] == "doc0"


def test_batch_embeddings_sync_contextual_happy_path(sync_client_contextual):
    """Test batch_embeddings with contextualized vectors - happy path."""
    # Use batch_embeddings context manager
    with sync_client_contextual.batch_embeddings():
        # Insert multiple documents
        sync_client_contextual.table("documents").insert([
            {"content": "doc0"},
            {"content": "doc1"},
            {"content": "doc2"},
        ]).execute()

        # Upsert more documents
        sync_client_contextual.table("documents").upsert(
            values={"content": "doc3"},
            on_conflict=["id"]
        ).execute()

    # After context exits, verify all rows exist
    result = sync_client_contextual.table("documents").select("*").execute()
    assert len(result.data) == 4

    # Verify vectors were written - try vector search
    search_result = sync_client_contextual.table("documents").vector_search(
        query="query",
        topk=4
    ).execute()
    assert len(search_result.data) == 4


def test_batch_embeddings_sync_contextual_error_rollback(sync_client_contextual):
    """Test batch_embeddings with contextualized vectors - rollback on error."""
    # Insert some initial data
    sync_client_contextual.table("documents").insert([
        {"content": "doc0"},
    ]).execute()

    # Verify initial state
    initial_result = sync_client_contextual.table("documents").select("*").execute()
    assert len(initial_result.data) == 1

    # Try to insert within context manager but raise error
    try:
        with sync_client_contextual.batch_embeddings():
            sync_client_contextual.table("documents").insert([
                {"content": "doc1"},
                {"content": "doc2"},
            ]).execute()

            # Raise KeyError to trigger rollback
            raise KeyError("Simulated key error")
    except KeyError:
        pass

    # After rollback, should still have only 1 row
    final_result = sync_client_contextual.table("documents").select("*").execute()
    assert len(final_result.data) == 1
    assert final_result.data[0]["content"] == "doc0"


def test_batch_embeddings_sync_nested_error(sync_client_regular):
    """Test that nested errors in batch_embeddings trigger rollback."""
    # Insert initial data
    sync_client_regular.table("documents").insert([
        {"content": "doc0"},
    ]).execute()

    try:
        with sync_client_regular.batch_embeddings():
            # First insert should succeed
            sync_client_regular.table("documents").insert([
                {"content": "doc1"},
            ]).execute()

            # Nested operation that fails
            try:
                sync_client_regular.table("documents").insert([
                    {"content": "doc2"},
                ]).execute()
                raise RuntimeError("Nested error")
            except RuntimeError:
                # Re-raise to propagate to outer context
                raise
    except RuntimeError:
        pass

    # Should rollback everything
    result = sync_client_regular.table("documents").select("*").execute()
    assert len(result.data) == 1
    assert result.data[0]["content"] == "doc0"
