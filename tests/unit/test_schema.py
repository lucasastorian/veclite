"""Test schema creation, validation, and migration."""
import pytest
from pathlib import Path
from veclite import Client, Schema
from veclite.schema.table import Table
from veclite.schema.fields import Integer, Text


def test_create_database(tmp_path, user_schema):
    """Test creating a new database."""
    db_path = tmp_path / "test.db"
    client = Client.create(user_schema, str(db_path))

    assert db_path.exists()

    cursor = client.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    assert cursor.fetchone() is not None

    client.close()


def test_connect_to_existing_database(tmp_path, user_schema):
    """Test connecting to an existing database with matching schema."""
    db_path = tmp_path / "test.db"

    client1 = Client.create(user_schema, str(db_path))
    client1.close()

    client2 = Client.connect(user_schema, str(db_path))
    assert client2 is not None
    client2.close()


def test_connect_fails_if_db_missing(tmp_path, user_schema):
    """Test that connect() fails if database doesn't exist."""
    db_path = tmp_path / "missing.db"

    with pytest.raises(FileNotFoundError, match="Database does not exist"):
        Client.connect(user_schema, str(db_path))


def test_create_fails_if_db_exists(tmp_path, user_schema):
    """Test that create() fails if database already exists."""
    db_path = tmp_path / "test.db"

    client1 = Client.create(user_schema, str(db_path))
    client1.close()

    with pytest.raises(FileExistsError, match="Database already exists"):
        Client.create(user_schema, str(db_path))


def test_schema_mismatch_detected(tmp_path):
    """Test that schema mismatches are detected on connect()."""
    from tests.fixtures.schemas import User

    class User1(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()

    schema1 = Schema()
    schema1.add_table(User1)

    db_path = tmp_path / "test.db"
    client = Client.create(schema1, str(db_path))
    client.close()

    class User2(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()
        email = Text(nullable=True)

    schema2 = Schema()
    schema2.add_table(User2)

    with pytest.raises(ValueError, match="Schema mismatch detected"):
        Client.connect(schema2, str(db_path))


def test_auto_migrate_adds_nullable_column(tmp_path):
    """Test that auto_migrate=True adds new nullable columns."""
    class User1(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()

    schema1 = Schema()
    schema1.add_table(User1)

    db_path = tmp_path / "test.db"
    client = Client.create(schema1, str(db_path))
    client.table("users").insert({"name": "Alice"}).execute()
    client.close()

    class User2(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()
        email = Text(nullable=True)

    schema2 = Schema()
    schema2.add_table(User2)

    client = Client.connect(schema2, str(db_path), auto_migrate=True)

    result = client.table("users").insert({"name": "Bob", "email": "bob@example.com"}).execute()
    assert result.data[0]["email"] == "bob@example.com"

    rows = client.table("users").select().execute()
    assert len(rows.data) == 2
    assert rows.data[0]["email"] is None
    assert rows.data[1]["email"] == "bob@example.com"

    client.close()


def test_auto_migrate_fails_on_not_null_column(tmp_path):
    """Test that auto_migrate fails on NOT NULL columns without defaults."""
    class User1(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()

    schema1 = Schema()
    schema1.add_table(User1)

    db_path = tmp_path / "test.db"
    client = Client.create(schema1, str(db_path))
    client.close()

    class User2(Table):
        __tablename__ = "users"
        id = Integer(primary_key=True)
        name = Text()
        email = Text(nullable=False)

    schema2 = Schema()
    schema2.add_table(User2)

    with pytest.raises(ValueError, match="Cannot auto-migrate.*NOT NULL"):
        Client.connect(schema2, str(db_path), auto_migrate=True)
