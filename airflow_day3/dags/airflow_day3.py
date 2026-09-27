"""
Day 3 — Operators (the building blocks of a task)
=================================================

An *operator* is a template for a single unit of work. Every task in a DAG is
an instance of some operator. There are two ways to write them:

  1. TaskFlow decorators  (modern, preferred for custom logic)
        @task            -> PythonOperator   (run a Python function)
        @task.bash       -> BashOperator     (run a shell command)
        @task.branch     -> BranchPythonOperator (choose a path)

  2. Classic operators    (instantiate the operator class directly; used for
                           ready-made provider operators such as SQL, S3, HTTP
                           that have no @task.* equivalent)
        EmptyOperator(...)         -> a no-op placeholder
        BashOperator(...)          -> the classic form of @task.bash

Modern data engineers prefer the TaskFlow decorators for custom Python/Bash
logic, and use classic provider operators only where no decorator exists.
This DAG shows both side by side so the equivalence is clear.
"""

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


@dag(
    dag_id="airflow_day3",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day3", "operators"],
)
def airflow_day3():

    # --- Classic operator: a no-op start marker ---
    # EmptyOperator does nothing; it is handy as a clear start/end anchor
    # or a join point. There is no @task.* equivalent, so we instantiate it.
    start = EmptyOperator(task_id="start")

    # --- Modern: @task.python (same as plain @task) ---
    # Runs a Python function. This is a PythonOperator under the hood.
    @task.python
    def extract() -> int:
        value = 42
        print(f"[extract] produced: {value}")
        return value

    # --- Modern: @task.bash ---
    # Returns a shell command string, which Airflow runs via a BashOperator.
    @task.bash
    def show_date() -> str:
        return "echo Today is: $(date)"

    # --- Classic: BashOperator (the non-decorator form of @task.bash) ---
    # Exactly the same capability as @task.bash, just written the classic way.
    greet = BashOperator(
        task_id="greet",
        bash_command="echo 'Hello from a classic BashOperator'",
    )

    # --- Modern: @task.branch ---
    # Returns the task_id of the branch to follow; other branches are skipped.
    @task.branch
    def choose_path(number: int) -> str:
        # Uses extract()'s value (passed via XCom) to decide the route.
        return "big_number" if number > 10 else "small_number"

    @task
    def big_number() -> None:
        print("[branch] took the BIG number path")

    @task
    def small_number() -> None:
        print("[branch] took the SMALL number path")

    # --- Classic operator: a no-op end marker ---
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ---- Wiring ----
    number = extract()                 # XCom output feeds the branch
    branch = choose_path(number)

    # start -> extract -> [show_date, greet] run in parallel after extract
    start >> number
    number >> [show_date(), greet]

    # extract -> branch -> one of the two paths -> end
    branch >> [big_number(), small_number()] >> end


airflow_day3 = airflow_day3()
