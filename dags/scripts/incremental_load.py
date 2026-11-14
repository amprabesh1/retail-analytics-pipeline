"""Incremental loading helpers for user_analytics_dag.

Two pieces:

1. extract_user_purchase_incremental: high-watermark extract from Postgres.
   Each run pulls only the rows whose invoice_date is newer than what earlier
   runs already loaded, writes them to
   s3://<bucket>/raw/user_purchase/ds=<ds>/, and records the window it
   loaded in the etl.watermark control table.

2. load_warehouse: persistent DuckDB warehouse. Each run replaces only its
   own ds partition (delete + insert) instead of rebuilding from one
   overwritten file.

3. build_behaviour_metrics / publish_metrics: the metric is built as a
   warehouse table first, audited by data_quality.check_metrics_quality,
   and only then published to the CSV the dashboard reads
   (write -> audit -> publish).
"""

from contextlib import closing
from datetime import datetime

SOURCE_TABLE = "retail.user_purchase"

# Used as the low watermark the very first time the pipeline runs.
INITIAL_WATERMARK = datetime(1900, 1, 1)

WATERMARK_DDL = """
CREATE SCHEMA IF NOT EXISTS etl;
CREATE TABLE IF NOT EXISTS etl.watermark (
    source_table   text      NOT NULL,
    ds             date      NOT NULL,
    low_watermark  timestamp NOT NULL,
    high_watermark timestamp NOT NULL,
    row_count      integer   NOT NULL,
    loaded_at      timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (source_table, ds)
);
"""

# The low watermark is the highest invoice_date loaded by any *earlier* ds.
# Looking only at earlier runs (ds < current ds) is what makes a re-run of the
# same ds pick the same window again instead of finding nothing new.
LOW_WATERMARK_SQL = """
SELECT max(high_watermark)
FROM etl.watermark
WHERE source_table = %s AND ds < %s::date
"""

EXTRACT_SQL = """
SELECT invoice_number, stock_code, detail, quantity, invoice_date,
       unit_price, customer_id, country
FROM retail.user_purchase
WHERE invoice_date > %(low)s
  AND invoice_date < %(upper)s
ORDER BY invoice_date
"""

UPSERT_WATERMARK_SQL = """
INSERT INTO etl.watermark
    (source_table, ds, low_watermark, high_watermark, row_count)
VALUES (%s, %s::date, %s, %s, %s)
ON CONFLICT (source_table, ds) DO UPDATE SET
    low_watermark  = EXCLUDED.low_watermark,
    high_watermark = EXCLUDED.high_watermark,
    row_count      = EXCLUDED.row_count,
    loaded_at      = now()
"""


def extract_user_purchase_incremental(
    ds,
    data_interval_end,
    s3_bucket,
    s3_key,
    postgres_conn_id="postgres_default",
    aws_conn_id="aws_default",
    pg_hook=None,
    s3_hook=None,
):
    """Extract new user_purchase rows for this run and record the watermark.

    Airflow fills in `ds` and `data_interval_end` from the task context.
    `pg_hook` and `s3_hook` are only passed by unit tests (fakes); in Airflow
    they are created from the connection ids.
    """
    if pg_hook is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    if s3_hook is None:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    pg = pg_hook

    with closing(pg.get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(WATERMARK_DDL)
            cur.execute(LOW_WATERMARK_SQL, (SOURCE_TABLE, ds))
            low = cur.fetchone()[0] or INITIAL_WATERMARK
        conn.commit()

    # Upper bound = end of this run's data interval, so a run never picks up
    # rows "from the future" relative to its own logical date.
    upper = data_interval_end.in_timezone("UTC").naive()

    df = pg.get_pandas_df(
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

    # Only advance the watermark after the data has landed in S3.
    with closing(pg.get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                UPSERT_WATERMARK_SQL, (SOURCE_TABLE, ds, low, high, len(df))
            )
        conn.commit()

    print(
        f"ds={ds}: loaded {len(df)} rows with invoice_date in "
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

DEFAULT_DB_PATH = "/opt/airflow/temp/warehouse.duckdb"

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


def load_warehouse(
    ds,
    local_root="/opt/airflow/temp/s3folder",
    db_path=DEFAULT_DB_PATH,
):
    """Upsert this run's purchases and the latest review snapshot."""
    import duckdb

    purchase_csv = f"{local_root}/raw/user_purchase/ds={ds}/user_purchase.csv"
    review_glob = f"{local_root}/clean/movie_review/insert_date={ds}/*.parquet"

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(USER_PURCHASE_DDL)
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
