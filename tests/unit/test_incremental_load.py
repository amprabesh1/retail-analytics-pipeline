"""Unit tests for dags/scripts/incremental_load.py.

The extract talks to Postgres and S3 through Airflow hooks. Here those are
replaced by fakes:
- FakePostgresHook runs the pipeline's real SQL (watermark table, extract
  query, upsert) on an in-memory DuckDB, translating psycopg2's %s / %(name)s
  placeholders to DuckDB's ? / $name.
- FakeS3Hook keeps uploaded files in a dict.
"""

import io
import re
from datetime import datetime

import duckdb
import pandas as pd
import pytest
from factories import DS, MOVIE_REVIEW_DDL
from scripts.incremental_load import (
    INITIAL_WATERMARK,
    build_behaviour_metrics,
    extract_user_purchase_incremental,
    load_warehouse,
    publish_metrics,
)

BUCKET = "user-analytics"


def to_duckdb(sql):
    return re.sub(r"%\((\w+)\)s", r"$\1", sql).replace("%s", "?")


class FakeCursor:
    def __init__(self, con):
        self.con = con

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if params is None:
            self.con.execute(to_duckdb(sql))
        else:
            self.con.execute(to_duckdb(sql), list(params))

    def fetchone(self):
        return self.con.fetchone()


class FakeConnection:
    def __init__(self, con):
        self.con = con

    def cursor(self):
        return FakeCursor(self.con)

    def commit(self):
        pass

    def close(self):
        pass


class FakePostgresHook:
    """Source database with retail.user_purchase, backed by DuckDB."""

    def __init__(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA retail")
        self.con.execute(
            """
            CREATE TABLE retail.user_purchase (
                invoice_number VARCHAR, stock_code VARCHAR, detail VARCHAR,
                quantity INTEGER, invoice_date TIMESTAMP,
                unit_price DECIMAL(8,3), customer_id INTEGER,
                country VARCHAR
            )
            """
        )

    def add_purchase(self, invoice, invoice_date, customer=17850):
        self.con.execute(
            "INSERT INTO retail.user_purchase VALUES "
            "(?, 'S1', 'ITEM', 1, ?, 2.5, ?, 'United Kingdom')",
            [invoice, invoice_date, customer],
        )

    def get_conn(self):
        return FakeConnection(self.con)

    def get_pandas_df(self, sql, parameters):
        return self.con.execute(to_duckdb(sql), parameters).df()

    def watermarks(self):
        return self.con.execute(
            "SELECT strftime(ds, '%Y-%m-%d'), low_watermark, "
            "high_watermark, row_count FROM etl.watermark ORDER BY ds"
        ).fetchall()


class FakeS3Hook:
    def __init__(self):
        self.files = {}

    def load_string(self, string_data, key, bucket_name, replace):
        self.files[(bucket_name, key)] = string_data


class FakeIntervalEnd:
    """Stands in for Airflow's pendulum data_interval_end."""

    def __init__(self, value):
        self.value = value

    def in_timezone(self, tz):
        return self

    def naive(self):
        return self.value


def run_extract(pg, s3, ds, interval_end=datetime(2026, 1, 1)):
    key = f"raw/user_purchase/ds={ds}/user_purchase.csv"
    result = extract_user_purchase_incremental(
        ds=ds,
        data_interval_end=FakeIntervalEnd(interval_end),
        s3_bucket=BUCKET,
        s3_key=key,
        pg_hook=pg,
        s3_hook=s3,
    )
    return result, pd.read_csv(io.StringIO(s3.files[(BUCKET, key)]))


@pytest.fixture
def source():
    pg = FakePostgresHook()
    pg.add_purchase("536365", "2010-12-01 08:26")
    pg.add_purchase("536366", "2010-12-01 09:00")
    pg.add_purchase("536367", "2010-12-02 10:00", customer=None)
    return pg


# --- Extract: the watermark --------------------------------------------------


def test_first_run_extracts_every_row(source):
    result, csv = run_extract(source, FakeS3Hook(), "2026-11-01")

    assert result["rows"] == 3
    assert list(csv["invoice_number"]) == [536365, 536366, 536367]
    assert source.watermarks() == [
        ("2026-11-01", INITIAL_WATERMARK, datetime(2010, 12, 2, 10, 0), 3)
    ]


def test_next_run_extracts_only_rows_newer_than_the_watermark(source):
    s3 = FakeS3Hook()
    run_extract(source, s3, "2026-11-01")
    source.add_purchase("536368", "2010-12-03 11:00")

    result, csv = run_extract(source, s3, "2026-11-02")

    assert result["rows"] == 1
    assert list(csv["invoice_number"]) == [536368]


def test_rerunning_a_date_extracts_the_same_rows_again(source):
    s3 = FakeS3Hook()
    run_extract(source, s3, "2026-11-01")
    source.add_purchase("536368", "2010-12-03 11:00")
    first, _ = run_extract(source, s3, "2026-11-02")

    again, csv = run_extract(source, s3, "2026-11-02")

    assert again["rows"] == first["rows"] == 1
    assert list(csv["invoice_number"]) == [536368]
    assert [w[0] for w in source.watermarks()] == ["2026-11-01", "2026-11-02"]


def test_run_with_nothing_new_writes_empty_file_and_keeps_watermark(source):
    s3 = FakeS3Hook()
    run_extract(source, s3, "2026-11-01")

    result, csv = run_extract(source, s3, "2026-11-02")

    assert result["rows"] == 0
    assert len(csv) == 0
    assert "invoice_number" in csv.columns
    _, low, high, rows = source.watermarks()[1]
    assert low == high == datetime(2010, 12, 2, 10, 0)


def test_rows_after_the_run_interval_wait_for_a_later_run(source):
    s3 = FakeS3Hook()
    source.add_purchase("536999", "2011-06-01 12:00")

    early, _ = run_extract(
        source, s3, "2026-11-01", interval_end=datetime(2011, 1, 1)
    )
    late, csv = run_extract(
        source, s3, "2026-11-02", interval_end=datetime(2012, 1, 1)
    )

    assert early["rows"] == 3
    assert late["rows"] == 1
    assert list(csv["invoice_number"]) == [536999]


def test_customer_id_is_written_as_a_whole_number(source):
    s3 = FakeS3Hook()
    run_extract(source, s3, "2026-11-01")
    key = "raw/user_purchase/ds=2026-11-01/user_purchase.csv"
    raw = s3.files[(BUCKET, key)]

    assert ",17850," in raw
    assert "17850.0" not in raw


# --- Load: the warehouse -----------------------------------------------------

CSV_HEADER = (
    "invoice_number,stock_code,detail,quantity,invoice_date,"
    "unit_price,customer_id,country\n"
)


def write_run_files(root, ds, purchase_lines, review_cids):
    """Create the local files the download tasks would leave for a run."""
    purchases = root / f"raw/user_purchase/ds={ds}"
    purchases.mkdir(parents=True)
    (purchases / "user_purchase.csv").write_text(
        CSV_HEADER + "".join(line + "\n" for line in purchase_lines)
    )

    reviews = root / f"clean/movie_review/insert_date={ds}"
    reviews.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE r AS SELECT cid::VARCHAR AS cid, "
        "true AS positive_review FROM unnest(?) t(cid)",
        [review_cids],
    )
    con.execute(f"COPY r TO '{reviews / 'part-0.parquet'}' (FORMAT parquet)")
    con.close()


def purchase_counts(db_path):
    con = duckdb.connect(db_path)
    rows = con.execute(
        "SELECT strftime(ds, '%Y-%m-%d'), count(*) FROM user_purchase "
        "GROUP BY ds ORDER BY ds"
    ).fetchall()
    con.close()
    return rows


LINE = "536365,S1,ITEM,{qty},2010-12-01 08:26:00,2.5,{cust},United Kingdom"


@pytest.fixture
def run_files(tmp_path):
    root = tmp_path / "s3folder"
    write_run_files(
        root, "2026-11-01", [LINE.format(qty=1, cust=17850)] * 2, [17850]
    )
    write_run_files(root, "2026-11-02", [LINE.format(qty=4, cust="")], [1])
    return root, str(tmp_path / "warehouse.duckdb")


def test_loading_the_same_date_twice_does_not_duplicate_rows(run_files):
    root, db = run_files

    load_warehouse(DS, local_root=str(root), db_path=db)
    load_warehouse(DS, local_root=str(root), db_path=db)

    assert purchase_counts(db) == [("2026-11-01", 2)]


def test_loading_a_new_date_keeps_earlier_dates(run_files):
    root, db = run_files

    load_warehouse("2026-11-01", local_root=str(root), db_path=db)
    load_warehouse("2026-11-02", local_root=str(root), db_path=db)

    assert purchase_counts(db) == [("2026-11-01", 2), ("2026-11-02", 1)]


def test_review_snapshot_is_replaced_not_appended(run_files):
    root, db = run_files

    load_warehouse("2026-11-01", local_root=str(root), db_path=db)
    load_warehouse("2026-11-02", local_root=str(root), db_path=db)

    con = duckdb.connect(db)
    assert con.execute("SELECT cid FROM movie_review").fetchall() == [(1,)]
    con.close()


def test_publish_writes_the_metrics_the_checks_saw(tmp_path):
    db = str(tmp_path / "warehouse.duckdb")
    out = tmp_path / "behaviour_metrics.csv"
    con = duckdb.connect(db)
    from scripts.incremental_load import USER_PURCHASE_DDL

    con.execute(USER_PURCHASE_DDL)
    con.execute(MOVIE_REVIEW_DDL)
    con.execute(
        "INSERT INTO user_purchase VALUES "
        "('536365','S1','ITEM',2,'2010-12-01',10,7,'UK','2026-11-01')"
    )
    con.execute("INSERT INTO movie_review VALUES (7, true, '2026-11-01')")
    con.close()

    build_behaviour_metrics(DS, db_path=db)
    publish_metrics(DS, db_path=db, metrics_csv=str(out))

    assert out.read_text().splitlines() == [
        "customer_id,amount_spent,num_positive_reviews,num_reviews",
        "7,20.000,1,1",
    ]
