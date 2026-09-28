"""
Day 5 — Postgres (Connections + SQL operators)
==============================================

This DAG talks to a real PostgreSQL database. It shows the two pieces every
"real" pipeline needs when working with an external system:

  1. A CONNECTION — Airflow stores DB credentials under a `conn_id` (managed in
     the UI under Admin -> Connections, or via env var / CLI). Tasks reference
     the connection by id and never hard-code credentials.

  2. Provider OPERATORS / HOOKS:
       - SQLExecuteQueryOperator  -> run SQL statements (classic operator; there
                                     is no @task.sql, so we instantiate it).
       - PostgresHook             -> open a connection in Python to fetch rows.

Pipeline: create table -> insert rows -> read rows back.

Prerequisite: create a connection with id `my_postgres` pointing at the DB.
For this local stack the metadata Postgres works fine, e.g.:

  docker compose exec airflow-scheduler airflow connections add my_postgres \
    --conn-type postgres --conn-host postgres --conn-login airflow \
    --conn-password airflow --conn-schema airflow --conn-port 5432
"""

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

CONN_ID = "my_postgres"


@dag(
    dag_id="airflow_day5",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day5", "postgres", "sql"],
)
def airflow_day5():

    # --- Classic provider operator: create a table if it doesn't exist ---
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS day5_demo (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """,
    )

    # --- Classic provider operator: insert a couple of rows ---
    insert_rows = SQLExecuteQueryOperator(
        task_id="insert_rows",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO day5_demo (name) VALUES ('alice'), ('bob');
        """,
    )

    # --- Modern @task using a Hook to read the data back in Python ---
    # A Hook is the programmatic way to talk to an external system; here it
    # opens the same connection and runs a query, returning rows to Python.
    @task
    def read_rows() -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = hook.get_records("SELECT id, name FROM day5_demo ORDER BY id;")
        print(f"[read_rows] fetched {len(rows)} rows:")
        for r in rows:
            print(f"  id={r[0]}, name={r[1]}")
        return len(rows)

    create_table >> insert_rows >> read_rows()


airflow_day5 = airflow_day5()
