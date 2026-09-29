"""
Day 9 - Datasets / Assets (data-driven scheduling) - PRODUCER
=============================================================

So far, DAGs ran either manually or on a time schedule (@daily, cron). But
often you want a DAG to run *when data is ready*, not at a fixed time. For
example: "run the reporting DAG as soon as the sales table is refreshed."

Airflow 3 does this with Assets (previously called Datasets). The idea:

  - A PRODUCER task declares that it "updates" an Asset via `outlets=[asset]`.
    When that task finishes successfully, Airflow marks the Asset as updated.

  - A CONSUMER DAG is scheduled ON that Asset (schedule=[asset]) instead of a
    time. Airflow triggers it automatically whenever the Asset is updated.

This is "data-driven scheduling": the consumer runs based on data readiness,
not the clock. This file is the PRODUCER; airflow_day9_consumer.py consumes it.
"""

from airflow.sdk import dag, task, Asset
from datetime import datetime

# Define the Asset. Think of it as a logical handle to a dataset (a table,
# an S3 path, etc.). The consumer DAG will listen to this same Asset.
sales_asset = Asset("s3://demo/sales_data")


@dag(
    dag_id="airflow_day9_producer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",       # producer runs on a normal schedule
    catchup=False,
    tags=["day9", "assets", "producer"],
)
def airflow_day9_producer():

    # outlets=[sales_asset] tells Airflow this task updates the Asset.
    # When this task succeeds, Airflow marks sales_asset as updated, which in
    # turn triggers any DAG scheduled on that Asset.
    @task(outlets=[sales_asset])
    def refresh_sales_data() -> None:
        print("[producer] refreshed sales data -> marking asset updated")


    refresh_sales_data()


airflow_day9_producer = airflow_day9_producer()
