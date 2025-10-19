"""Shared pytest fixtures."""
import pytest
import pytest_asyncio
from pathlib import Path
from veclite import Client, AsyncClient, Schema
from tests.fixtures.schemas import User, Product, Post, Article, Document
from tests.fixtures.mock_embedder import MockEmbedder, MockAsyncEmbedder


@pytest.fixture
def user_schema():
    """Simple schema with users table."""
    schema = Schema()
    schema.add_table(User)
    return schema


@pytest.fixture
def product_schema():
    """Schema with products table (has JSON field)."""
    schema = Schema()
    schema.add_table(Product)
    return schema


@pytest.fixture
def multi_table_schema():
    """Schema with multiple related tables."""
    schema = Schema()
    schema.add_table(User)
    schema.add_table(Post)
    return schema


@pytest.fixture
def sync_client(tmp_path, user_schema, mock_embedder):
    """Sync client with users table - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = Client.create(user_schema, str(db_path))
    client.embedder = mock_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest_asyncio.fixture
async def async_client(tmp_path, user_schema, mock_async_embedder):
    """Async client with users table - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = AsyncClient.create(user_schema, str(db_path))
    client.embedder = mock_async_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest.fixture
def sync_product_client(tmp_path, product_schema, mock_embedder):
    """Sync client with products table - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = Client.create(product_schema, str(db_path))
    client.embedder = mock_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest.fixture
def sync_multi_client(tmp_path, multi_table_schema, mock_embedder):
    """Sync client with users + posts tables - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = Client.create(multi_table_schema, str(db_path))
    client.embedder = mock_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest.fixture
def article_schema():
    """Schema with articles table (has FTS field)."""
    schema = Schema()
    schema.add_table(Article)
    return schema


@pytest.fixture
def sync_article_client(tmp_path, article_schema, mock_embedder):
    """Sync client with articles table - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = Client.create(article_schema, str(db_path))
    client.embedder = mock_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest_asyncio.fixture
async def async_article_client(tmp_path, article_schema, mock_async_embedder):
    """Async client with articles table - auto-cleanup."""
    db_path = tmp_path / "test.db"
    client = AsyncClient.create(article_schema, str(db_path))
    client.embedder = mock_async_embedder  # Always inject mock embedder for tests
    yield client
    client.close()


@pytest.fixture
def document_schema():
    """Schema with documents table (has both vector and FTS fields)."""
    schema = Schema()
    schema.add_table(Document)
    return schema


@pytest.fixture
def mock_embedder():
    """Mock embedder for testing vector search."""
    return MockEmbedder(dimension=64)


@pytest.fixture
def mock_async_embedder():
    """Mock async embedder for testing vector search."""
    return MockAsyncEmbedder(dimension=64)


@pytest.fixture
def sync_document_client(tmp_path, document_schema, mock_embedder, monkeypatch):
    """Sync client with documents table and mock embedder - auto-cleanup."""
    # Set fake API key so embedder initialization doesn't fail
    monkeypatch.setenv("VOYAGE_API_KEY", "test-key-for-testing")

    db_path = tmp_path / "test.db"
    client = Client.create(document_schema, str(db_path))
    client.embedder = mock_embedder  # Replace with mock embedder
    yield client
    client.close()


@pytest_asyncio.fixture
async def async_document_client(tmp_path, document_schema, mock_async_embedder, monkeypatch):
    """Async client with documents table and mock embedder - auto-cleanup."""
    # Set fake API key so embedder initialization doesn't fail
    monkeypatch.setenv("VOYAGE_API_KEY", "test-key-for-testing")

    db_path = tmp_path / "test.db"
    client = AsyncClient.create(document_schema, str(db_path))
    client.embedder = mock_async_embedder  # Replace with mock embedder
    yield client
    client.close()
