from airflow.models import DagBag


def test_no_import_errors(monkeypatch):

    # Set variables
    monkeypatch.setenv("AIRFLOW_VAR_BUCKET", "test-bucket")
    monkeypatch.setenv("AIRFLOW_VAR_EMR_ID", "test-emr-id")

    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag_bag.size() == 1


def test_only_one_run_at_a_time():
    # Two concurrent runs would both read the watermark before either
    # loaded, so both would load the same rows (a double load that the
    # warehouse_total_matches_watermarks check cannot see, because each
    # run also records its own batch).
    dag = DagBag().get_dag("user_analytics_dag")
    assert dag.max_active_runs == 1
