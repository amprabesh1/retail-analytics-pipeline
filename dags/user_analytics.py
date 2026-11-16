import os
import shutil
from datetime import datetime, timedelta

import boto3
from scripts.data_quality import (
    check_metrics_quality,
    check_warehouse_quality,
)
from scripts.incremental_load import (
    build_behaviour_metrics,
    extract_user_purchase_incremental,
    load_warehouse,
    publish_metrics,
)

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)


def get_s3_folder(
    s3_bucket, s3_folder, local_folder="/opt/airflow/temp/s3folder/"
):
    # TODO: Move AWS credentials to env variables
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )
    bucket = s3.Bucket(s3_bucket)
    local_path = os.path.join(local_folder, s3_folder)
    # Delete the local folder if it exists
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")


with DAG(
    "user_analytics_dag",
    description="A DAG to Pull user data and movie review data \
        to analyze their behaviour",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    user_analytics_bucket = "user-analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=user_analytics_bucket
    )
    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="/opt/airflow/data/movie_review.csv",
        dest_key="raw/movie_review/ds={{ ds }}/movie_review.csv",
        dest_bucket=user_analytics_bucket,
        replace=True,
    )

    # Incremental: only rows newer than the warehouse's watermark, one S3
    # partition per run date. The extracted window goes to load_warehouse
    # through XCom, which records it together with the rows.
    user_purchase_to_s3 = PythonOperator(
        task_id="user_purchase_to_s3",
        python_callable=extract_user_purchase_incremental,
        op_kwargs={
            "s3_bucket": user_analytics_bucket,
            "s3_key": "raw/user_purchase/ds={{ ds }}/user_purchase.csv",
        },
    )

    movie_classifier = BashOperator(
        task_id="movie_classifier",
        bash_command=(
            "python /opt/airflow/dags/scripts/spark/"
            "random_text_classification.py"
            " --input s3a://user-analytics/raw/movie_review/ds={{ ds }}/"
            "movie_review.csv"
            " --output s3a://user-analytics/clean/movie_review"
            " --run-id {{ ds }}"
        ),
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "clean/movie_review/insert_date={{ ds }}/",
        },
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "raw/user_purchase/ds={{ ds }}/",
        },
    )

    # Write -> audit -> publish: the dashboard only sees data that passed
    # the quality checks.
    load_to_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
    )

    check_warehouse = PythonOperator(
        task_id="check_warehouse_quality",
        python_callable=check_warehouse_quality,
    )

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=build_behaviour_metrics,
    )

    check_metrics = PythonOperator(
        task_id="check_metrics_quality",
        python_callable=check_metrics_quality,
    )

    publish = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics,
    )

    markdown_path = "/opt/airflow/dags/scripts/dashboard/"
    q_cmd = (
        f"cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd"
    )
    gen_dashboard = BashOperator(
        task_id="generate_dashboard", bash_command=q_cmd
    )

    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    user_purchase_to_s3 >> get_user_purchase_to_warehouse

    movie_review_to_s3 >> movie_classifier >> get_movie_review_to_warehouse

    (
        [get_user_purchase_to_warehouse, get_movie_review_to_warehouse]
        >> load_to_warehouse
        >> check_warehouse
        >> get_user_behaviour_metric
        >> check_metrics
        >> publish
        >> gen_dashboard
    )
