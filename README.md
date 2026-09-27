# airflow-journey

A hands-on journey learning [Apache Airflow](https://airflow.apache.org/), organized day by day. Each `airflow_dayN` directory is a self-contained local Airflow environment running via Docker Compose.

Currently on the latest stable **Airflow 3.3.2**.

## Repository layout

```
airflow-journey/
└── airflow_day1/
    ├── docker-compose.yaml   # Airflow 3.3.2 stack (CeleryExecutor + Redis + Postgres)
    ├── .env.example          # Template for required environment variables
    ├── dags/
    │   └── airflow_day1.py    # A minimal 2-task TaskFlow DAG
    ├── config/               # Airflow config (airflow.cfg is generated, not committed)
    ├── plugins/              # Custom plugins (empty for now)
    └── logs/                 # Task/scheduler logs (git-ignored)
```

## Day 1

A minimal DAG built with the Airflow 3.x **Task SDK** (`airflow.sdk`):

```python
from airflow.sdk import dag, task
from datetime import datetime


@dag(start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def airflow_day1():

    @task
    def task1():
        print("Task 1 executed")

    @task
    def task2():
        print("Task 2 executed")

    t1 = task1()
    t2 = task2()
    t1 >> t2  # task1 runs before task2


airflow_day1 = airflow_day1()
```

- `schedule=None` — trigger manually only
- `catchup=False` — no backfilling of past runs
- Dependency: `task1 >> task2`

## Prerequisites

- Docker + Docker Compose
- At least 4 GB of memory allocated to Docker

## Getting started

From inside `airflow_day1/`:

1. Create your `.env` from the template and fill in the secrets:

   ```bash
   cp .env.example .env
   ```

   Generate the two required secrets and paste them into `.env`:

   ```bash
   # FERNET_KEY
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

   # AIRFLOW__API_AUTH__JWT_SECRET
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

2. Initialize the metadata database and create the admin user:

   ```bash
   docker compose up airflow-init
   ```

3. Start the full stack:

   ```bash
   docker compose up -d
   ```

4. Open the web UI at [http://localhost:8080](http://localhost:8080) and log in with the credentials from your `.env` (default `airflow` / `airflow`).

5. Trigger the DAG (or unpause it in the UI):

   ```bash
   docker compose exec airflow-scheduler airflow dags trigger airflow_day1
   ```

## Useful commands

```bash
# List DAGs
docker compose exec airflow-scheduler airflow dags list

# Check for DAG import errors
docker compose exec airflow-scheduler airflow dags list-import-errors

# View running services
docker compose ps

# Tear down (‑v also deletes the Postgres volume / metadata DB)
docker compose down -v
```

## Stack

| Component  | Version            |
|------------|--------------------|
| Airflow    | 3.3.2              |
| Executor   | CeleryExecutor     |
| Broker     | Redis 7.2-bookworm |
| Metadata DB| PostgreSQL 16      |

## Notes

- `.env` and the generated `config/airflow.cfg` contain secrets and are intentionally git-ignored. Never commit them.
- Airflow 3.x uses the new Task SDK (`airflow.sdk`) and a separate `airflow-apiserver` service (the old `webserver` from 2.x is gone). The 2.x and 3.x metadata DB schemas are incompatible, so switching major versions requires a fresh volume (`docker compose down -v`).
