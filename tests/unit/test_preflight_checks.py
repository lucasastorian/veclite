"""Test preflight checks prevent partial database creation."""
import pytest
import os
import tempfile
from pathlib import Path
from veclite import Client, AsyncClient, Schema
from veclite.schema import Table, Integer, Text


def test_sync_client_preflight_check_missing_api_key(tmp_path):
    """Test that sync Client.create() fails before creating files if API key missing."""
    # Remove API key
    old_key = os.environ.pop('VOYAGE_API_KEY', None)
    
    try:
        class Document(Table):
            __tablename__ = "documents"
            id = Integer(primary_key=True)
            content = Text(vector=True)  # Has vector field
        
        schema = Schema(tables=[Document])
        db_path = tmp_path / "test.db"
        
        # Should fail before creating any files
        with pytest.raises(AssertionError, match="VOYAGE_API_KEY"):
            Client.create(schema, str(db_path))
        
        # Verify no database directory was created
        assert not db_path.exists(), "Database directory should not exist after preflight failure"
    
    finally:
        # Restore API key
        if old_key:
            os.environ['VOYAGE_API_KEY'] = old_key


def test_async_client_preflight_check_missing_api_key(tmp_path):
    """Test that async AsyncClient.create() fails before creating files if API key missing."""
    # Remove API key
    old_key = os.environ.pop('VOYAGE_API_KEY', None)
    
    try:
        class Document(Table):
            __tablename__ = "documents"
            id = Integer(primary_key=True)
            content = Text(vector=True)  # Has vector field
        
        schema = Schema(tables=[Document])
        db_path = tmp_path / "test.db"
        
        # Should fail before creating any files
        with pytest.raises(AssertionError, match="VOYAGE_API_KEY"):
            AsyncClient.create(schema, str(db_path))
        
        # Verify no database directory was created
        assert not db_path.exists(), "Database directory should not exist after preflight failure"
    
    finally:
        # Restore API key
        if old_key:
            os.environ['VOYAGE_API_KEY'] = old_key


def test_preflight_check_allows_schemas_without_vector_fields(tmp_path):
    """Test that preflight check passes for schemas without vector fields."""
    # Remove API key
    old_key = os.environ.pop('VOYAGE_API_KEY', None)
    
    try:
        class User(Table):
            __tablename__ = "users"
            id = Integer(primary_key=True)
            name = Text()  # No vector field
        
        schema = Schema(tables=[User])
        db_path = tmp_path / "test.db"
        
        # Should succeed since no vector fields
        client = Client.create(schema, str(db_path))
        client.close()
        
        # Verify database was created
        assert (db_path / "sqlite.db").exists()
    
    finally:
        # Restore API key
        if old_key:
            os.environ['VOYAGE_API_KEY'] = old_key
