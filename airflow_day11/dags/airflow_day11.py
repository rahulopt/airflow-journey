"""
Day 11 - Mini Project (capstone)
================================
Goal: build ONE end-to-end pipeline that combines everything learned:
  Sensor (Day 7) -> Ingest TaskGroup (Day 8) -> Dynamic mapping (Day 6)
  -> Load to Postgres (Day 5) -> Finalize with trigger rule (Day 10),
  all with retries (Day 4).

Fill in the code yourself under each TODO. Do NOT delete the TODOs until done.
"""

from airflow.sdk import dag, task, task_group
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os

CONN_ID = "my_postgres"
FLAG_FILE = "/tmp/day11_orders.flag"


@dag(
    dag_id="airflow_day11",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["day11","project"]
)
def airflow_day11():
    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_input() -> PokeReturnValue:
        """
        Sensor task that checks for the existence of a flag file.
        If the file exists, the sensor will return True, indicating that the pipeline can proceed.
        If the file does not exist, the sensor will return False, and it will keep checking until the timeout is reached.
        """
        return PokeReturnValue(is_done=os.path.exists(FLAG_FILE)) 
    @task_group(group_id="ingest")
    def ingest_group():
        @task
        def get_files() -> list[str]:
            return ["orders_us.csv", "orders_uk.csv", "orders_in.csv"]
        return get_files()
    
    @task
    def process(file: str) -> int:
        """
        Simulates processing a file and returns the number of records processed.
        For demonstration purposes, it calculates the number of records as the length of the file name multiplied by 10.
        """
        return len(file) * 10

    @task
    def load_total(counts: list[int]) -> None:
        total = sum(counts)
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run("CREATE TABLE IF NOT EXISTS day11_summary "
                 "(id SERIAL PRIMARY KEY, total INT, created_at TIMESTAMPTZ DEFAULT now());")
        hook.run(f"INSERT INTO day11_summary (total) VALUES ({total});")

    @task(trigger_rule="all_done")
    def finalize() -> None:
        print("pipeline complete")  

    
    flag = wait_for_input()
    files = ingest_group()
    flag >> files
    counts = process.expand(file=files)
    load_total(counts) >> finalize()


airflow_day11 = airflow_day11()
