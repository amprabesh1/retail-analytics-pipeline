"""Incremental loading helpers for user_analytics_dag.

1. extract_user_purchase_incremental: high-watermark extract from Postgres.
   Each run pulls only the rows whose invoice_date is newer than what earlier
   runs already loaded and writes them to
   s3://<bucket>/raw/user_purchase/ds=<ds>/. It returns the window it
   extracted (low, high, rows), which Airflow passes on through XCom.

2. load_warehouse: persistent DuckDB warehouse. Each run replaces only its
   own ds partition (delete + insert) and, in the same transaction, records
   the batch in the warehouse's etl.watermark table.

   The watermark (the bookmark of what has been loaded) lives in the same
   DuckDB file as the data and is committed atomically with it. They cannot
   drift apart: resetting Postgres no longer forgets what the warehouse holds
   (which used to reload every row on top of the old ones), deleting the
   warehouse also deletes its bookmark, and a failed load advances neither.

3. build_behaviour_metrics / publish_metrics: the metric is built as a
   warehouse table first, audited by data_quality.check_metrics_quality,
   and only then published to the CSV the dashboard reads
   (write -> audit -> publish).
"""

from contextlib import closing
from datetime import datetime

SOURCE_TABLE = "retail.user_purchase"

DEFAULT_DB_PATH = "/opt/airflow/temp/warehouse.duckdb"

# Used as the low watermark the very first time the pipeline runs.
INITIAL_WATERMARK = datetime(1900, 1, 1)

# Lives in the DuckDB warehouse, next to the rows it describes.
WATERMARK_DDL = """
CREATE SCHEMA IF NOT EXISTS etl;
CREATE TABLE IF NOT EXISTS etl.watermark (
    source_table   VARCHAR   NOT NULL,
    ds             DATE      NOT NULL,
    low_watermark  TIMESTAMP NOT NULL,
    high_watermark TIMESTAMP NOT NULL,
    row_count      INTEGER   NOT NULL,
    loaded_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source_table, ds)
);
"""

# The low watermark is the highest invoice_date loaded by any *earlier* ds.
# Looking only at earlier runs (ds < current ds) is what makes a re-run of the
# same ds pick the same window again instead of finding nothing new.
LOW_WATERMARK_SQL = """
SELECT max(high_watermark)
FROM etl.watermark
WHERE source_table = ? AND ds < ?::DATE
"""

EXTRACT_SQL = """
SELECT invoice_number, stock_code, detail, quantity, invoice_date,
       unit_price, customer_id, country
FROM retail.user_purchase
WHERE invoice_date > %(low)s
  AND invoice_date < %(upper)s
ORDER BY invoice_date
"""


def read_low_watermark(ds, db_path=DEFAULT_DB_PATH):
    """Highest invoice_date the warehouse holds from runs before ds."""
    import duckdb

    with closing(duckdb.connect(db_path)) as con:
        con.execute(WATERMARK_DDL)
        low = con.execute(LOW_WATERMARK_SQL, [SOURCE_TABLE, ds]).fetchone()
    return low[0] or INITIAL_WATERMARK


def extract_user_purchase_incremental(
    ds,
    data_interval_end,
    s3_bucket,
    s3_key,
    db_path=DEFAULT_DB_PATH,
    postgres_conn_id="postgres_default",
    aws_conn_id="aws_default",
    pg_hook=None,
    s3_hook=None,
):
    """Extract new user_purchase rows for this run.

    Airflow fills in `ds` and `data_interval_end` from the task context.
    `pg_hook` and `s3_hook` are only passed by unit tests (fakes); in Airflow
    they are created from the connection ids.

    The watermark is not advanced here: load_warehouse records it together
    with the rows, from the window this function returns.
    """
    if pg_hook is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    if s3_hook is None:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    low = read_low_watermark(ds, db_path)

    # Upper bound = end of this run's data interval, so a run never picks up
    # rows "from the future" relative to its own logical date.
    upper = data_interval_end.in_timezone("UTC").naive()

    df = pg_hook.get_pandas_df(
        EXTRACT_SQL, parameters={"low": low, "upper": upper}
    )
    # Nullable ints keep customer_id as 17850, not 17850.0, in the CSV.
    for col in ("quantity", "customer_id"):
        df[col] = df[col].astype("Int64")

    high = df["invoice_date"].max().to_pydatetime() if len(df) else low

    # A header-only file is still written when there are no new rows, so
    # downstream tasks always find this run's partition.
    s3_hook.load_string(
        df.to_csv(index=False),
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )

    print(
        f"ds={ds}: extracted {len(df)} rows with invoice_date in "
        f"({low}, {high}] (upper bound {upper})"
    )
    return {"ds": ds, "low": str(low), "high": str(high), "rows": len(df)}


USER_PURCHASE_COLUMNS = """{
    'invoice_number': 'VARCHAR',
    'stock_code': 'VARCHAR',
    'detail': 'VARCHAR',
    'quantity': 'INTEGER',
    'invoice_date': 'TIMESTAMP',
    'unit_price': 'DECIMAL(8,3)',
    'customer_id': 'INTEGER',
    'country': 'VARCHAR'
}"""

# Aggregate each side to one row per customer *before* joining.
# The original DAG joined purchases to reviews row by row and then summed,
# so every purchase was counted once per review and every review once per
# purchase (join fan-out). check_metrics_quality now guards against this.
BEHAVIOUR_METRICS_SQL = """
WITH purchases AS (
  SELECT customer_id, sum(quantity * unit_price) AS amount_spent
  FROM user_purchase
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id
),
reviews AS (
  SELECT cid,
         sum(CASE WHEN positive_review THEN 1 ELSE 0 END)
           AS num_positive_reviews,
         count(*) AS num_reviews
  FROM movie_review
  GROUP BY cid
)
SELECT p.customer_id, p.amount_spent, r.num_positive_reviews, r.num_reviews
FROM purchases p
JOIN reviews r ON p.customer_id = r.cid
"""

USER_PURCHASE_DDL = """
CREATE TABLE IF NOT EXISTS user_purchase (
    invoice_number VARCHAR,
    stock_code VARCHAR,
    detail VARCHAR,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price DECIMAL(8,3),
    customer_id INTEGER,
    country VARCHAR,
    ds DATE
)
"""


EXTRACT_TASK_ID = "user_purchase_to_s3"


def load_warehouse(
    ds,
    ti=None,
    extract=None,
    local_root="/opt/airflow/temp/s3folder",
    db_path=DEFAULT_DB_PATH,
):
    """Upsert this run's purchases, its watermark and the review snapshot.

    `extract` is what extract_user_purchase_incremental returned. In Airflow
    it is pulled from XCom through `ti`; unit tests pass it directly.
    """
    import duckdb

    if extract is None:
        extract = ti.xcom_pull(task_ids=EXTRACT_TASK_ID)
    if not extract or extract.get("ds") != ds:
        raise ValueError(
            f"ds={ds}: no extract result for this run (got {extract!r})"
        )

    purchase_csv = f"{local_root}/raw/user_purchase/ds={ds}/user_purchase.csv"
    review_glob = f"{local_root}/clean/movie_review/insert_date={ds}/*.parquet"

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(USER_PURCHASE_DDL)
        con.execute(WATERMARK_DDL)
        # Idempotent append: wipe this ds, then insert it again.
        con.execute("DELETE FROM user_purchase WHERE ds = ?::DATE", [ds])
        con.execute(
            f"""
            INSERT INTO user_purchase
            SELECT *, ?::DATE AS ds
            FROM read_csv('{purchase_csv}', header = true,
                          hive_partitioning = false,
                          columns = {USER_PURCHASE_COLUMNS})
            """,
            [ds],
        )
        # Record the batch in the same transaction as its rows: both are
        # committed, or neither is. (An upsert, because DuckDB rejects
        # deleting and re-inserting a primary key in one transaction.)
        con.execute(
            """
            INSERT INTO etl.watermark
                (source_table, ds, low_watermark, high_watermark, row_count)
            VALUES (?, ?::DATE, ?::TIMESTAMP, ?::TIMESTAMP, ?)
            ON CONFLICT (source_table, ds) DO UPDATE SET
                low_watermark  = EXCLUDED.low_watermark,
                high_watermark = EXCLUDED.high_watermark,
                row_count      = EXCLUDED.row_count,
                loaded_at      = now()
            """,
            [
                SOURCE_TABLE,
                ds,
                extract["low"],
                extract["high"],
                extract["rows"],
            ],
        )
        # Movie reviews have no timestamp, so each run is a full snapshot;
        # keep the latest snapshot only.
        con.execute(
            f"""
            CREATE OR REPLACE TABLE movie_review AS
            SELECT TRY_CAST(cid AS INTEGER) AS cid,
                   positive_review,
                   ?::DATE AS insert_date
            FROM read_parquet('{review_glob}', hive_partitioning = false)
            """,
            [ds],
        )
        con.execute("COMMIT")
        total = con.execute("SELECT count(*) FROM user_purchase").fetchone()
        print(f"ds={ds}: warehouse user_purchase now has {total[0]} rows")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def build_behaviour_metrics(ds, db_path=DEFAULT_DB_PATH):
    """Write step: rebuild the behaviour_metrics table (not yet published)."""
    import duckdb

    with closing(duckdb.connect(db_path)) as con:
        con.execute(
            "CREATE OR REPLACE TABLE behaviour_metrics AS "
            + BEHAVIOUR_METRICS_SQL
        )
        n = con.execute("SELECT count(*) FROM behaviour_metrics").fetchone()
    print(f"ds={ds}: built behaviour_metrics with {n[0]} customers")


def publish_metrics(
    ds,
    db_path=DEFAULT_DB_PATH,
    metrics_csv="/opt/airflow/data/behaviour_metrics.csv",
):
    """Publish step: runs only after check_metrics_quality has passed."""
    import duckdb

    with closing(duckdb.connect(db_path)) as con:
        con.execute(
            "COPY (SELECT * FROM behaviour_metrics ORDER BY customer_id) "
            f"TO '{metrics_csv}' (HEADER)"
        )
    print(f"ds={ds}: published behaviour_metrics to {metrics_csv}")
