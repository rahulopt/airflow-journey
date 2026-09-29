"""
Day 10 - Trigger Rules + Callbacks (pipeline resilience)
========================================================

By default, a task runs only when ALL its upstream tasks succeed
(trigger_rule="all_success"). Real pipelines need more control:

  - Run a CLEANUP task even if something upstream failed.
  - Run a JOIN/notification task no matter what happened upstream.
  - React to success/failure with CALLBACKS (e.g. send an alert).

TRIGGER RULES (set per task with trigger_rule=...):
  - all_success   (default) : run if all upstream succeeded
  - all_failed              : run if all upstream failed
  - one_failed              : run as soon as any upstream fails
  - one_success             : run as soon as any upstream succeeds
  - none_failed             : run if no upstream failed (success or skipped ok)
  - always                  : run regardless of upstream state

CALLBACKS (functions Airflow calls on task outcome):
  - on_success_callback / on_failure_callback
"""

from airflow.sdk import dag, task
from datetime import datetime


# --- Callback functions: Airflow calls these on task outcome ---
def notify_success(context):
    print(f"[callback] SUCCESS: task {context['task_instance'].task_id} finished ok")

def notify_failure(context):
    print(f"[callback] FAILURE: task {context['task_instance'].task_id} failed!")


@dag(
    dag_id="airflow_day10",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day10", "trigger-rules", "callbacks"],
)
def airflow_day10():

    # A task that always succeeds, with a success callback attached.
    @task(on_success_callback=notify_success)
    def step_ok() -> str:
        print("[step_ok] completed successfully")
        return "ok"

    # A task that always FAILS, with a failure callback attached.
    # We use this to demonstrate downstream trigger rules.
    @task(on_failure_callback=notify_failure, retries=0)
    def step_fails() -> str:
        raise ValueError("Simulated failure to test trigger rules")

    # CLEANUP: runs if ANY upstream failed (one_failed).
    # Great for rollback / cleanup logic that must run on failure.
    @task(trigger_rule="one_failed")
    def cleanup() -> None:
        print("[cleanup] an upstream task failed -> running cleanup")

    # FINALIZE: runs ALWAYS, regardless of upstream success/failure.
    # Useful for notifications or 'pipeline finished' markers.
    @task(trigger_rule="all_done")
    def finalize() -> None:
        print("[finalize] pipeline finished (runs no matter what)")

    ok = step_ok()
    fail = step_fails()
    # both step outcomes feed cleanup and finalize
    [ok, fail] >> cleanup()
    [ok, fail] >> finalize()


airflow_day10 = airflow_day10()
