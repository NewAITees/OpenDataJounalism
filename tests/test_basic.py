"""Basic tests for the opendatajounalism package."""

import pytest


def test_basic() -> None:
    """Basic test to verify pytest is working."""
    assert True


def test_import() -> None:
    """Test that the package can be imported."""
    import sys
    from pathlib import Path

    # Add src to Python path
    src_path = Path(__file__).parent.parent / "src"
    sys.path.insert(0, str(src_path))

    import opendatajounalism

    assert hasattr(opendatajounalism, "__version__")
    assert opendatajounalism.__version__ == "0.1.0"
