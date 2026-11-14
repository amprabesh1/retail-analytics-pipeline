"""Data quality checks for user_analytics_dag (write -> audit -> publish).

Each check is one SQL query that returns a single integer: the number of
offending rows. 0 means the check passed.

- severity "error": a failure stops the DAG run, so bad data never reaches
  the dashboard.
- severity "warn": a failure is logged but the run continues.

Every result is stored in the dq_results table in the DuckDB warehouse, so
there is a history of data health per run date (ds) and stage.

The rules below come from profiling the OnlineRetail data:
- Invoice numbers are digits, "C" + digits (cancellations, always negative
  quantity), or "A" + digits (bad-debt adjustments, the only negative prices).
- Negative quantities outside cancellations are stock write-offs: unit price
  0 and no customer.
- About 25% of rows have no customer_id (guest checkouts), so a null
  customer_id is normal; only an unusually high rate is suspicious.
"""

from contextlib import closing
from dataclasses import dataclass

ERROR = "error"
WARN = "warn"

# Warn when more than this share of a run's rows has no customer_id.
# The historical rate is about 25%.
MAX_NULL_CUSTOMER_PCT = 30


@dataclass(frozen=True)
class Check:
    name: str
    severity: str
    description: str
    sql: str


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: str
    description: str
    failing_rows: int

    @property
    def passed(self):
        return self.failing_rows == 0


class DataQualityError(Exception):
    """Raised when at least one error-severity check fails."""


# Checks on the rows this run loaded (ds = $ds), plus whole-table sanity.
WAREHOUSE_CHECKS = [
    Check(
        "purchase_keys_not_null",
        ERROR,
        "invoice_number, stock_code and invoice_date are never null",
        """
        SELECT count(*) FROM user_purchase
        WHERE ds = $ds::DATE
          AND (invoice_number IS NULL OR stock_code IS NULL
               OR invoice_date IS NULL)
        """,
    ),
    Check(
        "invoice_number_format",
        ERROR,
        "invoice_number is digits, optionally prefixed by C or A",
        """
        SELECT count(*) FROM user_purchase
        WHERE ds = $ds::DATE
          AND NOT regexp_matches(invoice_number, '^[CA]?[0-9]+$')
        """,
    ),
    Check(
        "cancellations_have_negative_quantity",
        ERROR,
        "C-invoices (cancellations) always have quantity < 0",
        """
        SELECT count(*) FROM user_purchase
        WHERE ds = $ds::DATE
          AND invoice_number LIKE 'C%' AND quantity >= 0
        """,
    ),
    Check(
        "negative_quantity_is_cancellation_or_writeoff",
        ERROR,
        "quantity < 0 only on cancellations or zero-price, "
        "no-customer stock write-offs",
        """
        SELECT count(*) FROM user_purchase
        WHERE ds = $ds::DATE
          AND quantity < 0
          AND invoice_number NOT LIKE 'C%'
          AND NOT (unit_price = 0 AND customer_id IS NULL)
        """,
    ),
    Check(
        "negative_price_only_on_adjustments",
        ERROR,
        "unit_price < 0 only on A-invoices (bad-debt adjustments)",
        """
        SELECT count(*) FROM user_purchase
        WHERE ds = $ds::DATE
          AND unit_price < 0 AND invoice_number NOT LIKE 'A%'
        """,
    ),
    Check(
        "row_count_matches_extract",
        ERROR,
        "rows in the warehouse for this ds equal the rows extracted "
        "from Postgres (etl.watermark.row_count)",
        """
        SELECT abs(count(*) - $expected_rows) FROM user_purchase
        WHERE ds = $ds::DATE
        """,
    ),
    Check(
        "warehouse_not_empty",
        ERROR,
        "user_purchase has at least one row",
        """
        SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM user_purchase
        """,
    ),
    Check(
        "new_rows_loaded",
        WARN,
        "this run loaded at least one new purchase "
        "(0 can be normal for an incremental run)",
        """
        SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM user_purchase
        WHERE ds = $ds::DATE
        """,
    ),
    Check(
        "null_customer_rate",
        WARN,
        f"at most {MAX_NULL_CUSTOMER_PCT}% of this run's rows have no "
        "customer_id",
        f"""
        SELECT CASE
                 WHEN count(*) > 0
                  AND 100.0 * count(*) FILTER (WHERE customer_id IS NULL)
                      / count(*) > {MAX_NULL_CUSTOMER_PCT}
                 THEN count(*) FILTER (WHERE customer_id IS NULL)
                 ELSE 0
               END
        FROM user_purchase
        WHERE ds = $ds::DATE
        """,
    ),
    Check(
        "movie_review_not_empty",
        ERROR,
        "movie_review has at least one row",
        """
        SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM movie_review
        """,
    ),
    Check(
        "movie_review_keys_not_null",
        ERROR,
        "cid and positive_review are never null",
        """
        SELECT count(*) FROM movie_review
        WHERE cid IS NULL OR positive_review IS NULL
        """,
    ),
]

# Checks on the behaviour_metrics table before it is published.
METRIC_CHECKS = [
    Check(
        "metrics_not_empty",
        ERROR,
        "behaviour_metrics has at least one row",
        """
        SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END
        FROM behaviour_metrics
        """,
    ),
    Check(
        "metrics_one_row_per_customer",
        ERROR,
        "customer_id is unique in behaviour_metrics",
        """
        SELECT count(*) - count(DISTINCT customer_id) FROM behaviour_metrics
        """,
    ),
    Check(
        "amount_spent_matches_purchases",
        ERROR,
        "each customer's amount_spent equals the sum of their purchases "
        "(catches join fan-out)",
        """
        WITH truth AS (
            SELECT customer_id, sum(quantity * unit_price) AS amount_spent
            FROM user_purchase
            GROUP BY customer_id
        )
        SELECT count(*)
        FROM behaviour_metrics m
        JOIN truth t USING (customer_id)
        WHERE abs(m.amount_spent - t.amount_spent) > 0.01
        """,
    ),
    Check(
        "review_counts_match_reviews",
        ERROR,
        "num_reviews and num_positive_reviews equal the customer's "
        "reviews in movie_review (catches join fan-out)",
        """
        WITH truth AS (
            SELECT cid,
                   count(*) AS num_reviews,
                   count(*) FILTER (WHERE positive_review)
                       AS num_positive_reviews
            FROM movie_review
            GROUP BY cid
        )
        SELECT count(*)
        FROM behaviour_metrics m
        JOIN truth t ON m.customer_id = t.cid
        WHERE m.num_reviews <> t.num_reviews
           OR m.num_positive_reviews <> t.num_positive_reviews
        """,
    ),
    Check(
        "positive_reviews_within_total",
        ERROR,
        "num_positive_reviews is never more than num_reviews",
        """
        SELECT count(*) FROM behaviour_metrics
        WHERE num_positive_reviews > num_reviews
        """,
    ),
]


def run_checks(con, checks, params):
    """Run each check on a DuckDB connection and return its results.

    Only the parameters a check's SQL actually uses ($name) are passed in,
    because DuckDB rejects unused named parameters.
    """
    results = []
    for check in checks:
        used = {k: v for k, v in params.items() if f"${k}" in check.sql}
        failing_rows = con.execute(check.sql, used).fetchone()[0]
        results.append(
            CheckResult(
                name=check.name,
                severity=check.severity,
                description=check.description,
                failing_rows=int(failing_rows or 0),
            )
        )
    return results


def record_results(con, ds, stage, results):
    """Store results in dq_results, replacing any earlier ones for ds/stage."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_results (
            ds DATE,
            stage VARCHAR,
            check_name VARCHAR,
            severity VARCHAR,
            passed BOOLEAN,
            failing_rows BIGINT,
            description VARCHAR,
            checked_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    con.execute("BEGIN TRANSACTION")
    con.execute(
        "DELETE FROM dq_results WHERE ds = ?::DATE AND stage = ?", [ds, stage]
    )
    for r in results:
        con.execute(
            """
            INSERT INTO dq_results
                (ds, stage, check_name, severity, passed, failing_rows,
                 description)
            VALUES (?::DATE, ?, ?, ?, ?, ?, ?)
            """,
            [
                ds,
                stage,
                r.name,
                r.severity,
                r.passed,
                r.failing_rows,
                r.description,
            ],
        )
    con.execute("COMMIT")


def format_report(stage, results):
    lines = [f"Data quality report: {stage}"]
    for r in results:
        if r.passed:
            status = "PASS"
        elif r.severity == WARN:
            status = "WARN"
        else:
            status = "FAIL"
        lines.append(
            f"  {status:<4}  {r.name:<46} failing_rows={r.failing_rows}"
        )
    return "\n".join(lines)


def enforce(stage, results):
    """Raise DataQualityError if any error-severity check failed."""
    failed = [r for r in results if r.severity == ERROR and not r.passed]
    if failed:
        details = "; ".join(
            f"{r.name} ({r.failing_rows} rows): {r.description}"
            for r in failed
        )
        raise DataQualityError(
            f"{len(failed)} {stage} check(s) failed: {details}"
        )


def _audit(ds, stage, checks, params, db_path):
    import duckdb

    with closing(duckdb.connect(db_path)) as con:
        results = run_checks(con, checks, params)
        record_results(con, ds, stage, results)
    print(format_report(stage, results))
    enforce(stage, results)
    return [r.name for r in results if not r.passed]


def check_warehouse_quality(
    ds,
    db_path="/opt/airflow/temp/warehouse.duckdb",
    postgres_conn_id="postgres_default",
):
    """Airflow task: audit this run's loaded rows before metrics are built."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    row = PostgresHook(postgres_conn_id=postgres_conn_id).get_first(
        "SELECT row_count FROM etl.watermark "
        "WHERE source_table = 'retail.user_purchase' AND ds = %s",
        parameters=(ds,),
    )
    if row is None:
        raise DataQualityError(f"No etl.watermark row for ds={ds}")
    params = {"ds": ds, "expected_rows": row[0]}
    return _audit(ds, "warehouse", WAREHOUSE_CHECKS, params, db_path)


def check_metrics_quality(ds, db_path="/opt/airflow/temp/warehouse.duckdb"):
    """Airflow task: audit behaviour_metrics before it is published."""
    return _audit(ds, "metrics", METRIC_CHECKS, {}, db_path)
