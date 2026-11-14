"""Unit tests for dags/scripts/data_quality.py and the metric SQL."""

import duckdb
import pytest
from factories import (
    DS,
    create_tables,
    insert_purchases,
    insert_reviews,
    purchase,
    review,
)
from scripts.data_quality import (
    METRIC_CHECKS,
    WAREHOUSE_CHECKS,
    WARN,
    DataQualityError,
    check_metrics_quality,
    enforce,
    record_results,
    run_checks,
)
from scripts.incremental_load import BEHAVIOUR_METRICS_SQL

# The metric SQL from the original project (before feature 2), kept here so
# the tests can prove the fan-out bug existed and that the checks catch it.
ORIGINAL_FANOUT_SQL = """
SELECT
  up.customer_id,
  sum(up.quantity * up.unit_price) AS amount_spent,
  sum(CASE WHEN mr.positive_review THEN 1 ELSE 0 END)
    AS num_positive_reviews,
  count(mr.cid) AS num_reviews
FROM user_purchase up
JOIN movie_review mr ON up.customer_id = mr.cid
GROUP BY up.customer_id
"""


def by_name(results):
    return {r.name: r for r in results}


def failing(results):
    """Names of the checks that did not pass."""
    return {name for name, r in by_name(results).items() if not r.passed}


def warehouse_checks(con, ds=DS, expected_rows=None):
    """Run the warehouse checks; by default expect what is actually loaded."""
    if expected_rows is None:
        expected_rows = con.execute(
            "SELECT count(*) FROM user_purchase WHERE ds = ?::DATE", [ds]
        ).fetchone()[0]
    return run_checks(
        con, WAREHOUSE_CHECKS, {"ds": ds, "expected_rows": expected_rows}
    )


def build_metrics(con, sql):
    con.execute("CREATE OR REPLACE TABLE behaviour_metrics AS " + sql)


@pytest.fixture
def clean_warehouse(warehouse):
    """Realistic, valid data, including every legitimate oddity found
    while profiling the real dataset."""
    insert_purchases(
        warehouse,
        [
            purchase("536365", qty=6, price=2.55, customer=17850),
            purchase("536366", qty=2, price=1.85, customer=17850),
            purchase("536367", qty=1, price=4.95, customer=14527),
            purchase("536368", qty=3, price=0.85, customer=14527),
            purchase("536369", qty=12, price=1.25, customer=13047),
            purchase("536370", qty=4, price=3.75, customer=13047),
            # A return: C-invoice with negative quantity.
            purchase("C536379", qty=-1, price=27.5, customer=14527),
            # A stock write-off: negative quantity, price 0, no customer.
            purchase("536380", qty=-10, price=0, customer=None),
            # A bad-debt adjustment: A-invoice with a negative price.
            purchase("A563186", qty=1, price=-11062.06, customer=None),
        ],
    )
    insert_reviews(
        warehouse,
        [review(17850, True), review(14527, False), review(13047, True)],
    )
    return warehouse


# --- Warehouse checks --------------------------------------------------------


def test_clean_data_passes_every_warehouse_check(clean_warehouse):
    assert failing(warehouse_checks(clean_warehouse)) == set()


@pytest.mark.parametrize(
    "bad_row, expected_check",
    [
        (
            purchase("536400", price=-5.0),
            "negative_price_only_on_adjustments",
        ),
        (
            purchase("C536401", qty=3),
            "cancellations_have_negative_quantity",
        ),
        (purchase("X536402"), "invoice_number_format"),
        (purchase("536403", date=None), "purchase_keys_not_null"),
        (
            purchase("536404", qty=-2, price=3.0, customer=17850),
            "negative_quantity_is_cancellation_or_writeoff",
        ),
    ],
    ids=[
        "negative price on a normal invoice",
        "cancellation with positive quantity",
        "unknown invoice prefix",
        "missing invoice date",
        "negative quantity that is not a return or write-off",
    ],
)
def test_each_kind_of_bad_row_trips_exactly_its_check(
    clean_warehouse, bad_row, expected_check
):
    insert_purchases(clean_warehouse, [bad_row])

    results = warehouse_checks(clean_warehouse)

    assert failing(results) == {expected_check}
    assert by_name(results)[expected_check].failing_rows == 1


def test_row_count_mismatch_with_extract_is_caught(clean_warehouse):
    loaded = 9
    results = warehouse_checks(clean_warehouse, expected_rows=loaded + 2)

    check = by_name(results)["row_count_matches_extract"]
    assert not check.passed
    assert check.failing_rows == 2


def test_only_this_runs_rows_are_checked(clean_warehouse):
    insert_purchases(
        clean_warehouse, [purchase("536400", price=-5.0, ds="2026-10-31")]
    )

    assert failing(warehouse_checks(clean_warehouse, ds=DS)) == set()
    assert "negative_price_only_on_adjustments" in failing(
        warehouse_checks(clean_warehouse, ds="2026-10-31")
    )


@pytest.mark.parametrize(
    "customers, should_warn",
    [
        ([17850, 17850, 17850, None], False),
        ([17850, None], True),
    ],
    ids=["25% guests is normal", "50% guests warns"],
)
def test_null_customer_rate_warns_only_above_threshold(
    warehouse, customers, should_warn
):
    insert_purchases(
        warehouse,
        [purchase(f"5364{i}", customer=c) for i, c in enumerate(customers)],
    )

    check = by_name(warehouse_checks(warehouse))["null_customer_rate"]

    assert check.severity == WARN
    assert check.passed is not should_warn


def test_warnings_do_not_stop_the_run(clean_warehouse):
    results = warehouse_checks(clean_warehouse, ds="2026-11-05")

    assert failing(results) == {"new_rows_loaded"}
    enforce("warehouse", results)


def test_an_error_check_stops_the_run(clean_warehouse):
    insert_purchases(clean_warehouse, [purchase("536400", price=-5.0)])

    with pytest.raises(DataQualityError, match="negative_price"):
        enforce("warehouse", warehouse_checks(clean_warehouse))


def test_rerunning_checks_replaces_earlier_results(clean_warehouse):
    for _ in range(2):
        record_results(
            clean_warehouse,
            DS,
            "warehouse",
            warehouse_checks(clean_warehouse),
        )

    stored = clean_warehouse.execute(
        "SELECT count(*) FROM dq_results WHERE ds = ?::DATE", [DS]
    ).fetchone()[0]
    assert stored == len(WAREHOUSE_CHECKS)


# --- The fan-out bug ---------------------------------------------------------


@pytest.fixture
def fanout_warehouse(warehouse):
    """Customer 1: two purchases (10 + 20 = 30) and three reviews, two of
    them positive. Customer 2: one purchase (5) and one positive review."""
    insert_purchases(
        warehouse,
        [
            purchase("536001", qty=1, price=10, customer=1),
            purchase("536002", qty=1, price=20, customer=1),
            purchase("536003", qty=1, price=5, customer=2),
        ],
    )
    insert_reviews(
        warehouse,
        [
            review(1, True),
            review(1, True),
            review(1, False),
            review(2, True),
        ],
    )
    return warehouse


def metric_row(con, customer_id):
    return con.execute(
        "SELECT amount_spent, num_reviews, num_positive_reviews "
        "FROM behaviour_metrics WHERE customer_id = ?",
        [customer_id],
    ).fetchone()


def test_original_metric_sql_multiplies_purchases_by_reviews(
    fanout_warehouse,
):
    build_metrics(fanout_warehouse, ORIGINAL_FANOUT_SQL)

    spent, reviews, positive = metric_row(fanout_warehouse, 1)
    assert spent == 90
    assert reviews == 6
    assert positive == 4


def test_metric_checks_catch_the_fanout_bug(fanout_warehouse):
    build_metrics(fanout_warehouse, ORIGINAL_FANOUT_SQL)

    results = by_name(run_checks(fanout_warehouse, METRIC_CHECKS, {}))

    assert results["amount_spent_matches_purchases"].failing_rows == 1
    assert results["review_counts_match_reviews"].failing_rows == 1


def test_fixed_metric_sql_gives_true_numbers_and_passes(fanout_warehouse):
    build_metrics(fanout_warehouse, BEHAVIOUR_METRICS_SQL)

    assert metric_row(fanout_warehouse, 1) == (30, 3, 2)
    assert metric_row(fanout_warehouse, 2) == (5, 1, 1)
    assert failing(run_checks(fanout_warehouse, METRIC_CHECKS, {})) == set()


def test_metrics_task_refuses_to_pass_fanout_metrics(tmp_path):
    db_path = str(tmp_path / "warehouse.duckdb")
    con = duckdb.connect(db_path)
    create_tables(con)
    insert_purchases(
        con,
        [
            purchase("536001", qty=1, price=10, customer=1),
            purchase("536002", qty=1, price=20, customer=1),
        ],
    )
    insert_reviews(con, [review(1), review(1)])
    build_metrics(con, ORIGINAL_FANOUT_SQL)
    con.close()

    with pytest.raises(DataQualityError, match="amount_spent_matches"):
        check_metrics_quality(DS, db_path=db_path)

    con = duckdb.connect(db_path)
    failed = con.execute(
        "SELECT check_name FROM dq_results WHERE NOT passed ORDER BY 1"
    ).fetchall()
    con.close()
    assert failed == [
        ("amount_spent_matches_purchases",),
        ("review_counts_match_reviews",),
    ]
