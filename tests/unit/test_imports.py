"""Test that all modules can be imported without errors."""
import importlib
from pathlib import Path


def test_all_modules_import():
    """Verify all Python modules in veclite can be imported successfully.

    This catches:
    - Missing type hint imports (e.g., Tuple, Optional)
    - Circular import issues
    - Syntax errors
    - Missing dependencies
    """
    veclite_path = Path(__file__).parent.parent / "veclite"
    py_files = list(veclite_path.rglob("*.py"))

    errors = []

    for py_file in py_files:
        # Skip __pycache__
        if "__pycache__" in str(py_file):
            continue

        # Convert path to module name
        module_parts = py_file.relative_to(veclite_path.parent).with_suffix("").parts
        module_name = ".".join(module_parts)

        try:
            importlib.import_module(module_name)
        except Exception as e:
            errors.append((module_name, str(e)))

    # Assert no errors
    if errors:
        error_msg = f"Found {len(errors)} import errors:\n\n"
        for module, error in errors[:5]:  # Show first 5
            error_msg += f"{module}:\n  {error}\n\n"
        if len(errors) > 5:
            error_msg += f"... and {len(errors) - 5} more errors"
        raise AssertionError(error_msg)


def test_top_level_imports():
    """Test that all top-level exports are importable."""
    from veclite import Client, AsyncClient, Schema, View
    from veclite.schema import Table, Integer, Text, Boolean, VectorConfig
    from veclite.embeddings import VoyageClient, AsyncVoyageClient

    # Verify they're the right types
    assert Client.__name__ == "Client"
    assert AsyncClient.__name__ == "AsyncClient"
    assert Schema.__name__ == "Schema"
    assert View.__name__ == "View"
    assert Table.__name__ == "Table"
    assert VoyageClient.__name__ == "VoyageClient"
    assert AsyncVoyageClient.__name__ == "AsyncVoyageClient"
