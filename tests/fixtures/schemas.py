"""Reusable test schemas."""
from veclite.schema.table import Table
from veclite.schema.fields import Integer, Text, Float, JSONField, Boolean
from veclite.schema.vector_config import VectorConfig


class User(Table):
    __tablename__ = "users"

    id = Integer(primary_key=True)
    name = Text()
    email = Text(unique=True)
    age = Integer(nullable=True)


class Product(Table):
    __tablename__ = "products"

    id = Integer(primary_key=True)
    name = Text()
    price = Float()
    in_stock = Boolean(default=True)
    metadata = JSONField(nullable=True)


class Post(Table):
    __tablename__ = "posts"

    id = Integer(primary_key=True)
    title = Text()
    content = Text()
    author_id = Integer(foreign_key="users.id")
    published = Boolean(default=False)


class Article(Table):
    """Table with FTS-enabled fields for keyword search testing."""
    __tablename__ = "articles"

    id = Integer(primary_key=True)
    title = Text()
    content = Text(fts=True)  # Enable full-text search
    category = Text(nullable=True)
    views = Integer(default=0)


class Document(Table):
    """Table with both vector and FTS fields for semantic search testing."""
    __tablename__ = "documents"

    id = Integer(primary_key=True)
    title = Text()
    content = Text(vector=VectorConfig.mock(64), fts=True)  # Enable both vector and FTS with 64-dim embeddings
    category = Text(nullable=True)
