"""
Day 13 - Data Quality checks (validate before you load)
=======================================================
Why: never load bad data into your warehouse. Before loading, VALIDATE the
data. If it fails the checks, stop the pipeline (raise an error) so the bad
data never reaches downstream systems. "Garbage in, garbage out."

A sample CSV is at /tmp/day13_sales.csv with intentional problems:
    id,region,amount
    1,US,100
    2,UK,250
    3,IN,          <- NULL amount (bad)
    4,US,-50       <- negative amount (bad)
    5,UK,300

Pipeline idea:
    read_data  ->  validate (raise if bad)  ->  load (only if valid)  ->  report
"""
from airflow.sdk import dag, task
from datetime import datetime
import csv

DATA_FILE = "/tmp/day13_sales.csv"


@dag(
    dag_id='airflow_day13',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['day13', 'data-quality']  
)

def airflow_day13():
    @task
    def read_data() -> list[dict]:
        """
        Task to read data from a CSV file and return a list of dictionaries.
        Each dictionary represents a row in the CSV file.
        """
        rows = []
        with open(DATA_FILE, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                rows.append(row)
        return rows
    @task
    def validate(rows: list[dict]) -> list[dict]:
        """
        Task to validate the data quality of the rows.
        Checks for:
            - Empty/null 'amount' values
            - Negative 'amount' values
        If any errors are found, raises a ValueError to stop the pipeline.
        Returns the valid rows if all checks pass.
        """
        errors = []
        valid_rows = []
        for row in rows:
            amount = row.get('amount')
            if amount is None or amount.strip() == '':
                errors.append(f"Row {row['id']} has empty/null amount.")
            elif float(amount) < 0:
                errors.append(f"Row {row['id']} has negative amount: {amount}.")
            else:
                valid_rows.append(row)
        
        if errors:
            for error in errors:
                print(error)
            raise ValueError("Data validation failed. See errors above.")
        
        return valid_rows


    @task
    def load(rows: list[dict]) -> None:
        """
        Task to simulate loading valid rows into a database.
        For demonstration purposes, it prints the number of valid rows loaded.
        """
        print(f"Loaded {len(rows)} valid rows into the database.")


    rows = read_data()
    valid = validate(rows)
    load(valid)



airflow_day13 = airflow_day13()
