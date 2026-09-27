from airflow.sdk import dag, task
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
)
def airflow_day1():

    @task
    def task1():
        print("Task 1 executed")
    @task
    def task2():
        print("Task 2 executed")

    # Create tasks
    t1 = task1()
    t2 = task2()

    # task1 will run before task2
    t1 >> t2


airflow_day1 = airflow_day1()