"""
Day 8 - TaskGroups (organizing large DAGs)
===========================================

As DAGs grow, the graph gets crowded and hard to read. A *TaskGroup* lets you
bundle related tasks into a single collapsible unit in the Airflow UI. It is
purely organizational - it does not change how tasks run, it just makes big
pipelines easier to read and manage.

A common pattern is to group tasks by pipeline stage:

        [ extract group ]  >>  [ transform group ]  >>  [ load group ]

With the TaskFlow API you create a group using the @task_group decorator.
Tasks defined inside the decorated function become members of that group, and
in the UI they appear nested under the group's name (e.g. extract.pull_orders).
"""

from airflow.sdk import dag, task, task_group
from datetime import datetime


@dag(
    dag_id="airflow_day8",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day8", "taskgroups"],
)
def airflow_day8():

    # ---- GROUP 1: extract ----
    # Two independent extract tasks bundled under the "extract" group.
    @task_group(group_id="extract")
    def extract_group():
        @task
        def pull_orders() -> int:
            print("[extract.pull_orders] pulled orders")
            return 100

        @task
        def pull_customers() -> int:
            print("[extract.pull_customers] pulled customers")
            return 50

        # returning both makes the values available to the next stage
        return {"orders": pull_orders(), "customers": pull_customers()}

    # ---- GROUP 2: transform ----
    # Takes the extracted counts and produces a combined result.
    @task_group(group_id="transform")
    def transform_group(orders: int, customers: int):
        @task
        def clean(orders: int, customers: int) -> int:
            total = orders + customers
            print(f"[transform.clean] combined total = {total}")
            return total

        return clean(orders, customers)

    # ---- GROUP 3: load ----
    @task_group(group_id="load")
    def load_group(total: int):
        @task
        def write_result(total: int) -> None:
            print(f"[load.write_result] loaded final total = {total}")

        write_result(total)

    # ---- Wiring the groups together ----
    extracted = extract_group()
    combined = transform_group(extracted["orders"], extracted["customers"])
    load_group(combined)


airflow_day8 = airflow_day8()
