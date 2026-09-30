"""
Day 14 - Idempotency + UPSERT (safe to re-run)
==============================================
Why: a production pipeline must be IDEMPOTENT - running it multiple times
produces the SAME result, without creating duplicates or corrupt data.

Remember Day 11? Each run INSERTed a new row, so re-running created duplicate
rows (two rows of 390). That is NON-idempotent.

Fix = UPSERT ("insert or update"): if a row for a key already exists, UPDATE
it; otherwise INSERT it. In Postgres this is:

    INSERT INTO t (key, val) VALUES (%s, %s)
    ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val;

So no matter how many times the DAG runs, there is exactly ONE row per key,
always holding the latest value. This DAG upserts a daily regional total.

A Connection `my_postgres` already exists (from Day 5).

"""

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

CONN_ID='my_postgres'
@dag(
    dag_id="airflow_day14",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["day14","idempotency"]
)
def airflow_day14():
    @task 
    def create_table() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS day14_regional_totals (
            region TEXT PRIMARY KEY,
            total  INT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """
        hook.run(create_table_sql)  

    @task 
    def upsert_totals() -> None:
        # pretend these are today's computed totals:
        data = [("US", 300), ("UK", 550), ("IN",    175)]
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        for region, total in data:
            upsert_sql = """
            INSERT INTO day14_regional_totals (region, total) VALUES (%s, %s)
            ON CONFLICT (region) DO UPDATE SET total = EXCLUDED.total, updated_at = now();
            """
            hook.run(upsert_sql, parameters=(region, total))


    @task 
    def show_counts() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = hook.get_records("SELECT region, total FROM day14_regional_totals ORDER BY region;")
        for row in rows:
            print(f"Region: {row[0]}, Total: {row[1]}")
        print(f"Total rows: {len(rows)}")




    create_table() >> upsert_totals() >> show_counts()


airflow_day14 = airflow_day14()