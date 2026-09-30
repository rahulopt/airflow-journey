"""
Day 12 - Variables & Connections (production config, no hardcoding)
===================================================================
Why: in production you must NOT hardcode file paths, list of files, batch
sizes, environment names, or credentials inside the DAG. Config changes should
not require code changes.

  - Variables    = key/value config (paths, batch size, env, feature flags).
                   Managed in UI (Admin -> Variables) or CLI. Read in code with
                   Variable.get(...). Can store JSON for structured config.
  - Connections  = credentials/endpoints (DB, API, S3). NEVER in code; managed
                   in UI (Admin -> Connections) or CLI; referenced by conn_id.

A Variable named `day12_config` is already set (JSON):
    {"source_files": ["sales_jan.csv", "sales_feb.csv"], "min_rows": 5, "env": "dev"}
And a Connection `my_postgres` already exists (from Day 5).

"""

from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

CONN_ID = "my_postgres"
@dag(
    dag_id="airflow_day12",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    tags=["day12","config"]
)
def airflow_day12():
    @task
    def load_config() -> dict:
        """
        Load the configuration from the Airflow Variable named 'day12_config'.
        The Variable is expected to contain a JSON object with keys:
        - source_files: list of file names
        - min_rows: minimum number of rows expected in each file
        - env: environment name (e.g., dev, prod)
        """
        cfg = Variable.get("day12_config", deserialize_json=True)
        print(f"Environment: {cfg['env']}, Source Files: {cfg['source_files']}, Min Rows: {cfg['min_rows']}")
        return cfg

    @task
    def show_files(cfg: dict) -> int:
        """
        Task to display the number of source files and the minimum rows threshold.
        Returns the number of source files.
        """
        num_files = len(cfg["source_files"])
        print(f"Number of source files: {num_files}, Minimum rows threshold: {cfg['min_rows']}")
        return num_files
    @task
    def check_db(cfg: dict) -> None:
        """
        Task to check the database connection using the PostgresHook.
        It retrieves the current database name and prints it along with the environment from the config.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        db_name = hook.get_first("SELECT current_database();")[0]
        print(f"Environment: {cfg['env']}, Connected to database: {db_name}")


    cfg= load_config()
    show_files(cfg)
    check_db(cfg)


    

airflow_day12 = airflow_day12()
