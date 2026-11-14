"""Shared fixtures for the unit tests.

These tests need only duckdb, pandas and pytest: no Airflow, Postgres, S3 or
Docker. Each test builds a tiny in-memory DuckDB warehouse with a handful of
hand-written rows, so the expected answer can be checked by eye.
"""

import sys
from pathlib import Path

import duckdb
import pytest

# Make `from scripts... import ...` work, the same way Airflow imports it
# from the dags/ folder.
DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from factories import create_tables  # noqa: E402


@pytest.fixture
def warehouse():
    """An empty in-memory warehouse with the pipeline's own table layout."""
    con = duckdb.connect(":memory:")
    create_tables(con)
    yield con
    con.close()
