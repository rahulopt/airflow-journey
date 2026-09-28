"""
Day 4 — Scheduling + Retries
============================

Until now every DAG used `schedule=None` (manual trigger only). Real pipelines
run automatically on a schedule and must survive transient failures. Day 4
covers both.

SCHEDULING — when a DAG runs automatically:
  - `schedule="@daily"`            preset (also @hourly, @weekly, @monthly)
  - `schedule="0 9 * * *"`         cron: every day at 09:00
  - `catchup=True`                 backfill every missed interval since
                                   start_date; `False` runs only from now on.

RETRIES — automatically re-run a failed task:
  - `retries=N`                    how many extra attempts
  - `retry_delay=timedelta(...)`   wait between attempts
  Set them once in `default_args` to apply to every task in the DAG.
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta


# default_args apply to ALL tasks in the DAG unless a task overrides them.
default_args = {
    "retries": 2,                          # try 2 extra times on failure
    "retry_delay": timedelta(seconds=15),  # wait 15s between attempts
}


@dag(
    dag_id="airflow_day4",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",     # run once per day automatically
    catchup=False,         # do NOT backfill past intervals; start from now
    default_args=default_args,
    tags=["day4", "scheduling", "retries"],
)
def airflow_day4():

    # A normal task that always succeeds.
    @task
    def reliable_task() -> str:
        print("[reliable] this task always succeeds")
        return "ok"

    # A task that FAILS on its first attempt, then SUCCEEDS on a retry.
    # We read the current try number from the runtime Context: attempt 1 fails,
    # attempts 2+ pass — so you can watch `try_number` increase in the DB/UI
    # and see the retry mechanism actually working.
    @task(retries=2, retry_delay=timedelta(seconds=15))
    def flaky_task(**context) -> str:
        attempt = context["ti"].try_number
        print(f"[flaky] attempt number: {attempt}")
        if attempt == 1:
            raise ValueError("Simulated failure on first attempt")
        print("[flaky] succeeded on retry")
        return "recovered"

    reliable_task() >> flaky_task()


airflow_day4 = airflow_day4()
