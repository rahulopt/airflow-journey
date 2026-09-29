"""
Day 9 - Datasets / Assets (data-driven scheduling) - CONSUMER
=============================================================

This is the CONSUMER DAG. Instead of a time-based schedule, it is scheduled ON
the Asset (schedule=[sales_asset]). Airflow triggers this DAG automatically
whenever the producer task updates that Asset - no cron, no manual trigger.

Flow:
    producer.refresh_sales_data (outlets=[sales_asset])  --updates-->  sales_asset
    sales_asset  --triggers-->  this consumer DAG
"""

from airflow.sdk import dag, task, Asset
from datetime import datetime

# IMPORTANT: this must be the SAME Asset identifier used in the producer.
sales_asset = Asset("s3://demo/sales_data")


@dag(
    dag_id="airflow_day9_consumer",
    start_date=datetime(2026, 1, 1),
    schedule=[sales_asset],   # <-- data-driven: run when the Asset is updated
    catchup=False,
    tags=["day9", "assets", "consumer"],
)
def airflow_day9_consumer():

    @task
    def build_report() -> None:
        print("[consumer] sales asset was updated -> building report now")

    build_report()


airflow_day9_consumer = airflow_day9_consumer()
