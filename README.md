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
└── airflow_day8/
    └── dags/
        └── airflow_day8.py   # TaskGroups: extract / transform / load groups
└── airflow_day9/
    └── dags/
        ├── airflow_day9_producer.py   # Assets: producer updates an Asset (outlets)
        └── airflow_day9_consumer.py   # Assets: consumer scheduled ON the Asset
└── airflow_day10/
    └── dags/
        └── airflow_day10.py   # Trigger rules (one_failed / all_done) + callbacks
└── airflow_day11/
    └── dags/
        └── airflow_day11.py   # Mini project: sensor -> ingest group -> map -> Postgres -> finalize
└── airflow_day12/
    └── dags/
        └── airflow_day12.py   # Production config: Variables (JSON) + Connections (no hardcoding)
└── airflow_day13/
    └── dags/
        └── airflow_day13.py   # Data quality: validate (null/negative) -> stop pipeline if bad
└── airflow_day14/
    └── dags/
        └── airflow_day14.py   # Idempotency: UPSERT (ON CONFLICT) so re-runs don't duplicate
```

> Each `airflow_dayN` directory is meant to be a self-contained environment.
> Days 2–14 currently ship only the DAG; run them inside a running Airflow
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

## Day 8 — TaskGroups (organizing large DAGs)

As DAGs grow, the graph gets crowded. A **TaskGroup** bundles related tasks
into a single collapsible unit in the Airflow UI. It is purely organizational —
it does not change how tasks run, it just makes big pipelines easier to read.

```python
from airflow.sdk import dag, task, task_group
from datetime import datetime

@dag(dag_id="airflow_day8", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False, tags=["day8", "taskgroups"])
def airflow_day8():

    @task_group(group_id="extract")
    def extract_group():
        @task
        def pull_orders() -> int: return 100
        @task
        def pull_customers() -> int: return 50
        return {"orders": pull_orders(), "customers": pull_customers()}

    @task_group(group_id="transform")
    def transform_group(orders: int, customers: int):
        @task
        def clean(orders: int, customers: int) -> int:
            return orders + customers
        return clean(orders, customers)

    extracted = extract_group()
    transform_group(extracted["orders"], extracted["customers"])
```

Key ideas:

- Create a group with the `@task_group(group_id="...")` decorator; tasks
  defined inside become members of that group.
- In the UI, group members appear nested under the group name
  (`extract.pull_orders`, `transform.clean`, `load.write_result`).
- TaskGroups are **cosmetic/organizational** — execution logic is unchanged,
  and data still flows between groups via normal XCom.
- Verified live: `extract` produced 100 + 50, `transform` combined them to
  `150`, and `load` wrote the final total of `150`.

## Day 9 — Datasets / Assets (data-driven scheduling)

Earlier DAGs ran on a time schedule. Day 9 makes a DAG run **when data is
ready** instead of on the clock. A **producer** task declares it updates an
**Asset**; a **consumer** DAG is scheduled on that Asset and triggers
automatically whenever the Asset is updated.

```python
from airflow.sdk import dag, task, Asset

sales_asset = Asset("s3://demo/sales_data")

# PRODUCER: outlets marks the asset updated when the task succeeds
@dag(dag_id="airflow_day9_producer", schedule="@daily", ...)
def producer():
    @task(outlets=[sales_asset])
    def refresh_sales_data() -> None: ...

# CONSUMER: scheduled ON the asset, not on time
@dag(dag_id="airflow_day9_consumer", schedule=[sales_asset], ...)
def consumer():
    @task
    def build_report() -> None: ...
```

Key ideas:

- `outlets=[asset]` on a task marks the Asset updated when the task succeeds.
- `schedule=[asset]` makes a DAG **data-driven** — it runs when the Asset
  updates, with no cron and no manual trigger.
- Verified live: triggering only the producer caused the consumer to run
  automatically with `run_type = asset_triggered`.

## Day 10 — Trigger Rules + Callbacks

By default a task runs only if **all** upstream tasks succeed. Day 10 shows how
to change that with **trigger rules**, and how to react to outcomes with
**callbacks**.

```python
@task(on_success_callback=notify_success)
def step_ok() -> str: return "ok"

@task(on_failure_callback=notify_failure, retries=0)
def step_fails() -> str:
    raise ValueError("Simulated failure")

@task(trigger_rule="one_failed")   # runs only if an upstream FAILED
def cleanup() -> None: ...

@task(trigger_rule="all_done")     # runs no matter what happened
def finalize() -> None: ...

[step_ok(), step_fails()] >> cleanup()
[step_ok(), step_fails()] >> finalize()
```

Key ideas:

- **Trigger rules** control when a task runs: `all_success` (default),
  `one_failed`, `all_failed`, `one_success`, `none_failed`, `all_done`,
  `always`.
- **Callbacks** (`on_success_callback` / `on_failure_callback`) run custom
  logic on a task's outcome — useful for alerts.
- Verified live: `step_fails` failed, yet `cleanup` (`one_failed`) and
  `finalize` (`all_done`) both still ran, proving the resilience pattern.

## Day 11 — Mini Project (capstone)

Day 11 combines the previous concepts into one end-to-end pipeline:

```
wait_for_input (sensor)  ->  ingest.get_files (TaskGroup)
    ->  process.expand (dynamic mapping)  ->  load_total (Postgres)  ->  finalize (all_done)
```

```python
@dag(dag_id="airflow_day11", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False,
     default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
     tags=["day11", "project"])
def airflow_day11():

    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_input() -> PokeReturnValue:
        return PokeReturnValue(is_done=os.path.exists(FLAG_FILE))

    @task_group(group_id="ingest")
    def ingest_group():
        @task
        def get_files() -> list[str]:
            return ["orders_us.csv", "orders_uk.csv", "orders_in.csv"]
        return get_files()

    @task
    def process(file: str) -> int:
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
```

Concepts combined: **sensors** (Day 7), **TaskGroups** (Day 8), **dynamic
mapping** (Day 6), **Postgres load** (Day 5), **retries** (Day 4), and
**trigger rules** (Day 10).

Run it: create the flag file the sensor waits for, then trigger the DAG:

```bash
docker compose exec airflow-scheduler touch /tmp/day11_orders.flag
docker compose exec airflow-scheduler airflow dags trigger airflow_day11
```

Verified live: the sensor detected the file, `process` mapped to `map_index`
0/1/2, and `load_total` wrote a total of `390`
(`len("orders_xx.csv") * 10 * 3`) into the `day11_summary` table in Postgres.

## Day 12 — Variables & Connections (production config)

Production DAGs must not hardcode paths, file lists, thresholds, or
credentials. Day 12 moves configuration **out of the code**: structured config
lives in an Airflow **Variable** (JSON), and database access uses a
**Connection** referenced by `conn_id`.

```python
from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "my_postgres"

@dag(dag_id="airflow_day12", schedule=None, catchup=False, tags=["day12", "config"])
def airflow_day12():

    @task
    def load_config() -> dict:
        # structured config as JSON, editable in the UI without code changes
        return Variable.get("day12_config", deserialize_json=True)

    @task
    def show_files(cfg: dict) -> int:
        return len(cfg["source_files"])          # uses config, not hardcoded

    @task
    def check_db(cfg: dict) -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)   # creds via conn_id
        db = hook.get_first("SELECT current_database();")[0]
        print(f"env={cfg['env']} db={db}")

    cfg = load_config()
    show_files(cfg)
    check_db(cfg)
```

Set the Variable once (JSON value):

```bash
docker compose exec airflow-scheduler airflow variables set day12_config \
  '{"source_files": ["sales_jan.csv", "sales_feb.csv"], "min_rows": 5, "env": "dev"}'
```

Key ideas:

- **Variables** hold config; `Variable.get(key, deserialize_json=True)` returns
  a dict. Change config in the UI/CLI without touching code.
- **Connections** hold credentials/endpoints; tasks reference them by `conn_id`
  through a Hook — secrets never live in the DAG.
- Verified live: `load_config` read the JSON Variable (`env=dev`,
  `source_files=[…]`), and `check_db` connected via the `my_postgres`
  connection with no credentials in code.

## Day 13 — Data Quality (validate before you load)

Never load bad data into your warehouse. Day 13 adds a **validation gate**:
read the data, check it, and **stop the pipeline** (raise an error) if it fails
the checks — so bad data never reaches downstream systems.

```python
from airflow.sdk import dag, task
import csv

DATA_FILE = "/tmp/day13_sales.csv"

@dag(dag_id="airflow_day13", schedule=None, catchup=False, tags=["day13", "data-quality"])
def airflow_day13():

    @task
    def read_data() -> list[dict]:
        with open(DATA_FILE) as f:
            return list(csv.DictReader(f))

    @task
    def validate(rows: list[dict]) -> list[dict]:
        errors = []
        for row in rows:
            amount = row.get("amount")
            if amount is None or amount.strip() == "":
                errors.append(f"Row {row['id']} has empty amount")
            elif float(amount) < 0:
                errors.append(f"Row {row['id']} has negative amount: {amount}")
        if errors:
            raise ValueError("Data validation failed: " + "; ".join(errors))
        return rows

    @task
    def load(rows: list[dict]) -> None:
        print(f"Loaded {len(rows)} valid rows")

    load(validate(read_data()))
```

Key ideas:

- The `validate` task is the **data-quality gate**: if it raises, downstream
  tasks become `upstream_failed` and the bad data is never loaded.
- A **designed failure** (validation stopping bad data) is different from a
  **bug failure** — here, `validate` failing on bad input is the intended,
  correct behavior.
- Verified live: with bad rows (null + negative amount) `validate` **failed**
  and `load` was skipped; with clean data all tasks **succeeded** and `load`
  reported `Loaded 5 valid rows`.
- Note (CeleryExecutor): tasks run on the **worker**, so input files must be on
  shared storage (S3, a mounted volume) rather than one container's local `/tmp`.

## Day 14 — Idempotency + UPSERT (safe to re-run)

A production pipeline must be **idempotent**: running it multiple times
produces the same result, without duplicates. In Day 11, each run inserted a
new row (re-running created duplicate totals). Day 14 fixes that with an
**UPSERT** — `INSERT ... ON CONFLICT ... DO UPDATE` — so there is exactly one
row per key, always holding the latest value.

```python
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

CONN_ID = "my_postgres"

@dag(dag_id="airflow_day14", schedule=None, catchup=False, tags=["day14", "idempotency"])
def airflow_day14():

    @task
    def create_table() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS day14_regional_totals (
            region TEXT PRIMARY KEY, total INT, updated_at TIMESTAMPTZ DEFAULT now());""")

    @task
    def upsert_totals() -> None:
        data = [("US", 300), ("UK", 550), ("IN", 175)]
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        for region, total in data:
            hook.run(
                "INSERT INTO day14_regional_totals (region, total) VALUES (%s, %s) "
                "ON CONFLICT (region) DO UPDATE SET total = EXCLUDED.total, updated_at = now();",
                parameters=(region, total))

    create_table() >> upsert_totals()
```

Key ideas:

- The conflict target (`region`) must be a **PRIMARY KEY / UNIQUE** column for
  `ON CONFLICT` to work.
- `EXCLUDED.total` refers to the value that the failed INSERT tried to add;
  `DO UPDATE` applies it to the existing row.
- Because of the UPSERT, re-running the DAG keeps the row count constant (one
  row per region) instead of inserting duplicates — the pipeline is idempotent.

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
