"""
Day 6 — Dynamic Task Mapping (.expand)
======================================

So far every task was fixed: one function -> one task instance. But in real
pipelines the amount of work is only known at RUNTIME — e.g. "process every
file that landed in S3 today", where the number of files varies each run.

Dynamic Task Mapping solves this. `.expand()` runs the SAME task once per item
in a list, creating one mapped task instance per element — automatically and
in parallel. A list of 3 items -> 3 instances; 100 items -> 100 instances.

This mirrors the classic map/reduce pattern:
    get_files (produce list)  ->  process.expand (map over each)  ->  summarize (reduce)
"""

from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="airflow_day6",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day6", "dynamic-mapping"],
)
def airflow_day6():

    # --- Produce a list at runtime (its length is not known in advance) ---
    @task
    def get_files() -> list[str]:
        files = ["users.csv", "orders.csv", "products.csv"]
        print(f"[get_files] found {len(files)} files to process")
        return files

    # --- MAP: this single task is expanded over each item in the list ---
    # `.expand(file=...)` below creates one instance of `process` per file.
    @task
    def process(file: str) -> int:
        row_count = len(file) * 10          # pretend we processed the file
        print(f"[process] {file} -> {row_count} rows")
        return row_count

    # --- REDUCE: collect all mapped results into one task ---
    # `counts` receives the list of every mapped `process` return value.
    @task
    def summarize(counts: list[int]) -> None:
        print(f"[summarize] processed {len(counts)} files, "
              f"total rows = {sum(counts)}")

    # Wiring: expand process over the file list, then reduce the results.
    file_list = get_files()
    counts = process.expand(file=file_list)   # <-- dynamic mapping happens here
    summarize(counts)


airflow_day6 = airflow_day6()
