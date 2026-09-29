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
└── airflow_day4/
    └── dags/
        └── airflow_day4.py   # Scheduling (@daily) + retries (flaky task recovers)
└── airflow_day5/
    └── dags/
        └── airflow_day5.py   # Postgres: connection + SQL operators + hook
└── airflow_day6/
    └── dags/
        └── airflow_day6.py   # Dynamic task mapping: get_files -> process.expand -> summarize
└── airflow_day7/
    └── dags/
        └── airflow_day7.py   # Sensors: @task.sensor + FileSensor waiting for a file
```

> Each `airflow_dayN` directory is meant to be a self-contained environment.
> Days 2–7 currently ship only the DAG; run them inside a running Airflow
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

## Day 4 — Scheduling + Retries

Earlier DAGs used `schedule=None` (manual only). Day 4 makes a DAG run
automatically and recover from transient failures:

```python
from datetime import datetime, timedelta

default_args = {"retries": 2, "retry_delay": timedelta(seconds=15)}

@dag(dag_id="airflow_day4", start_date=datetime(2026, 1, 1),
     schedule="@daily", catchup=False, default_args=default_args)
def airflow_day4():

    @task(retries=2, retry_delay=timedelta(seconds=15))
    def flaky_task(**context) -> str:
        attempt = context["ti"].try_number
        if attempt == 1:
            raise ValueError("Simulated failure on first attempt")  # fails once
        return "recovered"                                          # passes on retry
```

Key ideas:

- **Scheduling:** `schedule="@daily"` (also `@hourly`/`@weekly`) or a cron
  string like `"0 9 * * *"`. `catchup=False` runs from now on; `True` backfills
  every missed interval since `start_date`.
- **Retries:** `retries` + `retry_delay`, set per-task or once in
  `default_args` for the whole DAG.
- Verified live: `flaky_task` failed on attempt 1 and **succeeded on attempt 2**
  (`try_number = 2`), proving the retry mechanism.

## Day 5 — Postgres (Connections + SQL)

Day 5 talks to a real PostgreSQL database — the two pieces every external
integration needs: a **Connection** (`conn_id`, credentials stored by Airflow)
and provider **operators/hooks**.

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "my_postgres"

create_table = SQLExecuteQueryOperator(         # classic provider operator
    task_id="create_table", conn_id=CONN_ID,
    sql="CREATE TABLE IF NOT EXISTS day5_demo (id SERIAL PRIMARY KEY, name TEXT);")

@task                                           # modern TaskFlow + a Hook
def read_rows() -> int:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    rows = hook.get_records("SELECT id, name FROM day5_demo ORDER BY id;")
    return len(rows)

create_table >> insert_rows >> read_rows()
```

Set up the connection once (id `my_postgres`):

```bash
docker compose exec airflow-scheduler airflow connections add my_postgres \
  --conn-type postgres --conn-host postgres --conn-login airflow \
  --conn-password airflow --conn-schema airflow --conn-port 5432
```

Key ideas:

- A **Connection** keeps credentials out of DAG code; tasks reference it by
  `conn_id`. Manage connections in the UI (Admin → Connections) or via the CLI.
- `SQLExecuteQueryOperator` runs SQL (there is no `@task.sql`, so it is used as
  a classic operator). A `PostgresHook` opens the connection in Python to fetch
  rows — the modern + classic mix in one pipeline.
- Verified live: `create_table → insert_rows → read_rows` all succeeded and rows
  (`alice`, `bob`) were written to and read back from Postgres.

## Day 6 — Dynamic Task Mapping

Earlier DAGs had a fixed set of tasks. Day 6 creates tasks **at runtime**:
`.expand()` runs the same task once per item in a list — the map/reduce pattern.

```python
@task
def get_files() -> list[str]:
    return ["users.csv", "orders.csv", "products.csv"]   # length only known at runtime

@task
def process(file: str) -> int:
    return len(file) * 10

@task
def summarize(counts: list[int]) -> None:
    print(f"processed {len(counts)} files, total rows = {sum(counts)}")

file_list = get_files()
counts = process.expand(file=file_list)   # one mapped instance per file
summarize(counts)                         # reduce: receives all results
```

Key ideas:

- `.expand()` fans a single task out into **N mapped instances**, one per list
  element, run in parallel. 3 files → 3 instances; 100 files → 100 instances.
- The count is **not hard-coded** — it comes from the upstream task's output at
  runtime.
- A downstream task that accepts the mapped output (`counts: list[int]`)
  **reduces** all instances back into one.
- Verified live: `process` expanded to `map_index` 0/1/2 (returning `90`, `100`,
  `120`); `summarize` reduced them to a total of `310`.

## Day 7 — Sensors (waiting for a condition)

Earlier DAGs ran immediately. Day 7 introduces **sensors** — special tasks
that repeatedly check ("poke") a condition and only succeed once it is met.
This is how a pipeline waits for a file to land, a partition to appear, or an
external system to be ready before it proceeds.

```python
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime

WATCHED_FILE = "/tmp/day7_ready.flag"

@dag(dag_id="airflow_day7", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False, tags=["day7", "sensors"])
def airflow_day7():

    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_flag() -> PokeReturnValue:          # custom sensor
        import os
        return PokeReturnValue(is_done=os.path.exists(WATCHED_FILE))

    wait_with_filesensor = FileSensor(               # ready-made sensor
        task_id="wait_with_filesensor", filepath=WATCHED_FILE,
        fs_conn_id="fs_default", poke_interval=5, timeout=60, mode="reschedule")
```

Key ideas:

- `@task.sensor` wraps your own Python check; return
  `PokeReturnValue(is_done=True)` when the condition is satisfied. Classic
  provider sensors (like `FileSensor`) are ready-made for common systems.
- **`poke_interval`** is how often to re-check; **`timeout`** caps the total
  wait so a sensor never blocks forever.
- **`mode="poke"`** holds a worker slot the whole time (fine for short waits);
  **`mode="reschedule"`** frees the slot between checks (efficient for long
  waits — prefer it in production).
- Verified live: an upstream task created the flag file, both sensors detected
  it (`file exists = True`), and the downstream task then ran.

Set up the filesystem connection once (id `fs_default`):

```bash
docker compose exec airflow-scheduler airflow connections add fs_default \
  --conn-type fs --conn-extra '{"path":"/"}'
```

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
