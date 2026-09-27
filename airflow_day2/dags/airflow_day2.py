"""
Day 2 — XCom (passing data between tasks)
==========================================

In Day 1 the tasks did not communicate with each other; they only printed.
In a real pipeline, each task consumes the OUTPUT of the previous one:

        extract  ->  transform  ->  load

This mechanism of moving a task's output to the next task is called XCom
(short for "Cross-Communication").

With the TaskFlow API (@task) XCom is straightforward:
  - `return` a value from a task            -> Airflow stores it in XCom.
  - Accept it as an argument in another task -> Airflow fetches it from XCom.

Under the hood, Airflow persists these values in the `xcom` table of the
metadata database (Postgres).
"""

from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="airflow_day2",
    start_date=datetime(2026, 1, 1),
    schedule=None,      # manual trigger only
    catchup=False,      # do not backfill past dates
    tags=["day2", "xcom"],
)
def airflow_day2():

    # ---- Task 1: EXTRACT ----
    # Produces a value and returns it. Returning the value pushes it to XCom.
    @task
    def extract() -> int:
        value = 10
        print(f"[extract] produced value: {value}")
        return value

    # ---- Task 2: TRANSFORM ----
    # The `number` argument receives extract()'s output: Airflow pulls it
    # from XCom and injects it here automatically.
    @task
    def transform(number: int) -> int:
        result = number * 2
        print(f"[transform] doubled {number} -> {result}")
        return result

    # ---- Task 3: LOAD ----
    # Consumes transform()'s output and persists/prints the final result.
    @task
    def load(number: int) -> None:
        print(f"[load] final result stored: {number}")

    # ---- Pipeline wiring ----
    # Passing one task's output directly into the next does two things:
    #   1) Passes data through XCom.
    #   2) Establishes the dependency (extract -> transform -> load).
    # So there is no need to declare dependencies with `>>` explicitly.
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)


airflow_day2 = airflow_day2()
