"""Helpers that build test rows with sensible defaults.

Each test only spells out the fields it cares about, for example
purchase("C536379", qty=-1) for a cancellation.
"""

from scripts.incremental_load import USER_PURCHASE_DDL

DS = "2026-11-01"

MOVIE_REVIEW_DDL = """
CREATE TABLE movie_review (
    cid INTEGER,
    positive_review BOOLEAN,
    insert_date DATE
)
"""


def create_tables(con):
    """Create the pipeline's warehouse tables on a DuckDB connection."""
    con.execute(USER_PURCHASE_DDL)
    con.execute(MOVIE_REVIEW_DDL)


def purchase(
    invoice="536365",
    qty=1,
    price=2.5,
    customer=17850,
    ds=DS,
    stock="85123A",
    date="2010-12-01 08:26:00",
):
    """One user_purchase row, in table column order."""
    return (
        invoice,
        stock,
        "TEST ITEM",
        qty,
        date,
        price,
        customer,
        "United Kingdom",
        ds,
    )


def review(cid, positive=True, ds=DS):
    """One movie_review row: (cid, positive_review, insert_date)."""
    return (cid, positive, ds)


def insert_purchases(con, rows):
    con.executemany(
        "INSERT INTO user_purchase VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )


def insert_reviews(con, rows):
    con.executemany("INSERT INTO movie_review VALUES (?, ?, ?)", rows)
