"""Test view creation, persistence, and operations."""
import pytest
from pathlib import Path
from veclite import Client, Schema, View
from veclite.schema.table import Table
from veclite.schema.fields import Integer, Text
from veclite.schema.view import ViewField


def test_create_view(tmp_path):
    """Test creating a database with a view."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        content = Text()
        author_id = Integer(foreign_key="authors.id")

    # Define view
    class AuthorPosts(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema = Schema()
    schema.add_table(Author)
    schema.add_table(Post)
    schema.add_view(AuthorPosts)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Check view exists in database
    cursor = client.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name='author_posts'"
    )
    assert cursor.fetchone() is not None

    client.close()


def test_view_persistence(tmp_path):
    """Test that views are persisted and validated on reconnect."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        author_id = Integer(foreign_key="authors.id")

    # Define view
    class AuthorPosts(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema = Schema()
    schema.add_table(Author)
    schema.add_table(Post)
    schema.add_view(AuthorPosts)

    db_path = tmp_path / "test.db"

    # Create database with view
    client1 = Client.create(schema, str(db_path))
    client1.close()

    # Reconnect with same schema (should work)
    client2 = Client.connect(schema, str(db_path))
    assert client2 is not None
    client2.close()


def test_view_mismatch_detected(tmp_path):
    """Test that view schema mismatches are detected."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        author_id = Integer(foreign_key="authors.id")

    # First view
    class AuthorPosts1(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema1 = Schema()
    schema1.add_table(Author)
    schema1.add_table(Post)
    schema1.add_view(AuthorPosts1)

    db_path = tmp_path / "test.db"
    client = Client.create(schema1, str(db_path))
    client.close()

    # Modified view (added field)
    class AuthorPosts2(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")
        author_id = ViewField(table="posts", field="author_id")  # NEW FIELD

    schema2 = Schema()
    schema2.add_table(Author)
    schema2.add_table(Post)
    schema2.add_view(AuthorPosts2)

    # Should fail with schema mismatch
    with pytest.raises(ValueError, match="Schema mismatch detected"):
        Client.connect(schema2, str(db_path))


def test_drop_view(tmp_path):
    """Test dropping a view."""
    # Define tables
    class User(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()

    class Comment(Table):
        __tablename__ = "comments"
        id = Integer(primary_key=True)
        text = Text()
        user_id = Integer(foreign_key="users.id")

    # Define view
    class UserComments(View):
        __viewname__ = "user_comments"
        __tables__ = ("users", "comments")

        id = ViewField(table="comments", field="id")
        comment_text = ViewField(table="comments", field="text")
        user_name = ViewField(table="users", field="name")

    schema = Schema()
    schema.add_table(User)
    schema.add_table(Comment)
    schema.add_view(UserComments)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Verify view exists
    cursor = client.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name='user_comments'"
    )
    assert cursor.fetchone() is not None

    # Drop the view
    client.drop_view("user_comments")

    # Verify view no longer exists in database
    cursor = client.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name='user_comments'"
    )
    assert cursor.fetchone() is None

    # Verify view removed from schema
    assert "user_comments" not in client.schema.views

    client.close()

    # Reconnect and verify view is still gone
    schema_no_view = Schema()
    schema_no_view.add_table(User)
    schema_no_view.add_table(Comment)

    client2 = Client.connect(schema_no_view, str(db_path))
    cursor = client2.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name='user_comments'"
    )
    assert cursor.fetchone() is None
    client2.close()


def test_drop_view_fails_if_not_found(tmp_path):
    """Test that dropping a non-existent view raises error."""
    class User(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()

    schema = Schema()
    schema.add_table(User)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    with pytest.raises(ValueError, match="View 'nonexistent' not found"):
        client.drop_view("nonexistent")

    client.close()


def test_view_query(tmp_path):
    """Test querying data from a view."""
    # Define tables
    class Product(Table):
        __tablename__ = "products"
        id = Integer(primary_key=True)
        name = Text()
        price = Integer()

    class Category(Table):
        __tablename__ = "categories"
        id = Integer(primary_key=True)
        name = Text()

    # Schema needs Category registered first
    schema = Schema()
    schema.add_table(Product)
    schema.add_table(Category)

    class ProductCategory(Table):
        __tablename__ = "product_categories"
        id = Integer(primary_key=True)
        product_id = Integer(foreign_key="products.id")
        category_id = Integer(foreign_key="categories.id")

    # Define view
    class ProductView(View):
        __viewname__ = "product_view"
        __tables__ = ("products", "product_categories", "categories")

        id = ViewField(table="products", field="id")
        product_name = ViewField(table="products", field="name")
        product_price = ViewField(table="products", field="price")
        category_name = ViewField(table="categories", field="name")

    schema.add_table(ProductCategory)
    schema.add_view(ProductView)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Insert test data
    client.table("categories").insert({"name": "Electronics"}).execute()
    client.table("products").insert({"name": "Laptop", "price": 1000}).execute()
    client.table("product_categories").insert({"product_id": 1, "category_id": 1}).execute()

    # Query view
    results = client.table("product_view").select().execute()

    assert len(results.data) == 1
    row = results.data[0]
    assert row["product_name"] == "Laptop"
    assert row["product_price"] == 1000
    assert row["category_name"] == "Electronics"

    client.close()


def test_view_insert_fails(tmp_path):
    """Test that insert operations fail on views."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        author_id = Integer(foreign_key="authors.id")

    # Define view
    class AuthorPosts(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema = Schema()
    schema.add_table(Author)
    schema.add_table(Post)
    schema.add_view(AuthorPosts)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Insert should fail on view
    with pytest.raises(Exception):  # SQLite will raise an error
        client.table("author_posts").insert({"post_title": "Test", "author_name": "Alice"}).execute()

    client.close()


def test_view_update_fails(tmp_path):
    """Test that update operations fail on views."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        author_id = Integer(foreign_key="authors.id")

    # Define view
    class AuthorPosts(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema = Schema()
    schema.add_table(Author)
    schema.add_table(Post)
    schema.add_view(AuthorPosts)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Add some data via tables
    client.table("authors").insert({"name": "Alice"}).execute()
    client.table("posts").insert({"title": "Post 1", "author_id": 1}).execute()

    # Update should fail on view
    with pytest.raises(Exception):  # SQLite will raise an error
        client.table("author_posts").update({"post_title": "Updated"}).eq("id", 1).execute()

    client.close()


def test_view_delete_fails(tmp_path):
    """Test that delete operations fail on views."""
    # Define tables
    class Author(Table):
        __tablename__ = "authors"
        id = Integer(primary_key=True)
        name = Text()

    class Post(Table):
        __tablename__ = "posts"
        id = Integer(primary_key=True)
        title = Text()
        author_id = Integer(foreign_key="authors.id")

    # Define view
    class AuthorPosts(View):
        __viewname__ = "author_posts"
        __tables__ = ("authors", "posts")

        id = ViewField(table="posts", field="id")
        post_title = ViewField(table="posts", field="title")
        author_name = ViewField(table="authors", field="name")

    schema = Schema()
    schema.add_table(Author)
    schema.add_table(Post)
    schema.add_view(AuthorPosts)

    db_path = tmp_path / "test.db"
    client = Client.create(schema, str(db_path))

    # Add some data via tables
    client.table("authors").insert({"name": "Alice"}).execute()
    client.table("posts").insert({"title": "Post 1", "author_id": 1}).execute()

    # Delete should fail on view
    with pytest.raises(Exception):  # SQLite will raise an error
        client.table("author_posts").delete().eq("id", 1).execute()

    client.close()
