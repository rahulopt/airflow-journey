from airflow.sdk import dag, task
from datetime import datetime


@dag(dag_id="airflow_day1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
)
def airflow_day1():

    @task(task_id="task1")
    def task1():
        print("Task 1 executed")
    @task(task_id="task2")
    def task2():
        print("Task 2 executed")
    @task(task_id="task3")
    def task3():
        print("Task 3 executed")

    # Create tasks
    t1 = task1()
    t2 = task2()
    t3= task3()

    # task1 will run before task2
    t1 >> t2
    t2 >> t3


airflow_day1 = airflow_day1()