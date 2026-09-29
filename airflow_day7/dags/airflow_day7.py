"""
Day 7 - Sensors (waiting for a condition before proceeding)
===========================================================

Until now, tasks ran immediately. But real pipelines often must WAIT for
something first: a file to land in S3, a partition to appear, a specific time,
or an external system to be ready. A *sensor* is a special task that keeps
checking ("poking") a condition and only succeeds once the condition is met.

Two ways to write sensors:

  1. @task.sensor  (modern TaskFlow) - wrap your own Python check.
       Return PokeReturnValue(is_done=True) when the condition is satisfied.

  2. Classic provider sensors (e.g. FileSensor) - ready-made sensors for
     common systems (files, S3 objects, SQL rows, etc.).

Two important sensor modes:
  - mode="poke"        : holds a worker slot the whole time it waits (simple,
                         fine for short waits).
  - mode="reschedule"  : frees the worker slot between checks (efficient for
                         long waits - use this in production for long polls).

Always set `timeout` and `poke_interval` so a sensor cannot wait forever.
"""

from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

# The FileSensor watches this path (inside the container). We create the file
# from an upstream task so the whole demo runs end to end.
WATCHED_FILE = "/tmp/day7_ready.flag"


@dag(
    dag_id="airflow_day7",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day7", "sensors"],
)
def airflow_day7():

    # --- A normal task that "creates" the file the sensor is waiting for ---
    # In a real pipeline this file would be dropped by an external system.
    @task
    def create_flag_file() -> str:
        with open(WATCHED_FILE, "w") as f:
            f.write("ready")
        print(f"[create_flag_file] created {WATCHED_FILE}")
        return WATCHED_FILE

    # --- Modern: @task.sensor (a custom Python condition) ---
    # This sensor pokes every 5s, checking our own condition. It succeeds by
    # returning PokeReturnValue(is_done=True). Here we simply check that the
    # flag file exists.
    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_flag() -> PokeReturnValue:
        import os
        exists = os.path.exists(WATCHED_FILE)
        print(f"[wait_for_flag] poking... file exists = {exists}")
        return PokeReturnValue(is_done=exists)

    # --- Classic: FileSensor (ready-made provider sensor) ---
    # Same idea, but using the built-in FileSensor instead of custom code.
    wait_with_filesensor = FileSensor(
        task_id="wait_with_filesensor",
        filepath=WATCHED_FILE,
        fs_conn_id="fs_default",     # default local filesystem connection
        poke_interval=5,
        timeout=60,
        mode="reschedule",
    )

    # --- Downstream task that runs only after the sensors succeed ---
    @task
    def process_after_wait() -> None:
        print("[process_after_wait] file is present - now processing data")

    # Wiring: create the file, then both sensors confirm it, then process.
    flag = create_flag_file()
    sensor1 = wait_for_flag()
    flag >> [sensor1, wait_with_filesensor] >> process_after_wait()


airflow_day7 = airflow_day7()
