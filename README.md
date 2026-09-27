# airflow-journey

A hands-on journey learning [Apache Airflow](https://airflow.apache.org/), organized day by day. Each `airflow_dayN` directory is a self-contained local Airflow environment running via Docker Compose.

Currently on the latest stable **Airflow 3.3.2**.

## Repository layout

```
airflow-journey/
├── airflow_day1/
│   ├── docker-compose.yaml   # Airflow 3.3.2 stack (CeleryExecutor + Redis + Postgres)
│   ├── .env.example          # Template for required environment variables
│   ├── dags/
│   │   └── airflow_day1.py   # A minimal TaskFlow DAG
│   ├── config/               # Airflow config (airflow.cfg is generated, not committed)
│   ├── plugins/              # Custom plugins (empty for now)
│   └── logs/                 # Task/scheduler logs (git-ignored)
└── airflow_day2/
    └── dags/
        └── airflow_day2.py   # XCom demo: extract -> transform -> load
└── airflow_day3/
    └── dags/
        └── airflow_day3.py   # Operators: TaskFlow decorators + classic + branching
```

> Each `airflow_dayN` directory is meant to be a self-contained environment.
> Days 2 and 3 currently ship only the DAG; run them inside a running Airflow
> stack (e.g. reuse Day 1's `docker-compose.yaml`, or add a dedicated one).

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

## Day 2 — XCom (passing data between tasks)

In Day 1 the tasks did not share data. Day 2 introduces **XCom**
(Cross-Communication), the mechanism Airflow uses to move a task's output to
the next task, forming a classic **extract → transform → load** pipeline:

```python
from airflow.sdk import dag, task
from datetime import datetime


@dag(dag_id="airflow_day2", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False, tags=["day2", "xcom"])
def airflow_day2():

    @task
    def extract() -> int:
        return 10                     # returning a value pushes it to XCom

    @task
    def transform(number: int) -> int:
        return number * 2             # receives extract()'s value from XCom

    @task
    def load(number: int) -> None:
        print(f"final result stored: {number}")

    # Passing outputs directly both moves data via XCom AND wires the
    # dependency (extract -> transform -> load), so no `>>` is needed.
    load(transform(extract()))


airflow_day2 = airflow_day2()
```

Key ideas:

- With the TaskFlow API, `return` a value to **push** it to XCom, and accept it
  as a function argument to **pull** it in the next task.
- Passing an output into the next call also **creates the dependency**
  automatically — no explicit `>>` required.
- Values are persisted in the `xcom` table of the metadata database. For this
  DAG: `extract` stores `10`, `transform` stores `20`, and `load` prints `20`.

## Day 3 — Operators (the building blocks of a task)

An *operator* is a template for a single unit of work; every task is an
instance of one. There are two ways to write them, and Day 3 shows both in a
single DAG:

**TaskFlow decorators (modern, preferred for custom logic):**

```python
@task.python                 # == @task; runs a Python function (PythonOperator)
def extract() -> int:
    return 42

@task.bash                   # runs a shell command (BashOperator)
def show_date() -> str:
    return "echo Today is: $(date)"

@task.branch                 # picks a path; other branches are skipped
def choose_path(number: int) -> str:
    return "big_number" if number > 10 else "small_number"
```

**Classic operators (instantiate the class directly — used for provider
operators such as SQL/S3/HTTP that have no `@task.*` form):**

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")               # no-op anchor / join point
greet = BashOperator(task_id="greet", bash_command="echo hi")
```

Key ideas:

- `@task.python` and `@task` are the same thing; both build a `PythonOperator`.
  Prefer the decorators for custom Python/Bash logic.
- Use classic operators where no decorator exists (`EmptyOperator`, and all the
  provider operators like SQL, S3, HTTP).
- `@task.branch` returns the `task_id` to follow; the unchosen branches are
  marked **skipped**. In this DAG `extract` returns `42`, so `big_number` runs
  and `small_number` is skipped.

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
