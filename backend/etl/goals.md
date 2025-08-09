🧪 STEP-BY-STEP PLAN TO ACHIEVE ALL 6 GOALS
✅ GOAL 1: ETL vs ELT, Patterns
🔨 Task:

    Use ETL pattern → Transform in Python before loading.

    Simulate batch ingestion by reading a CSV once.

    Use a config flag for ELT simulation (load raw then transform in SQL via DuckDB/Postgres).

📄 Code:

/data/input/users.csv  → batch loaded once via Airflow

✅ GOAL 2: Schema Management
🔨 Task:

    Auto-detect schema from CSV using pandas.dtypes.

    Write a schema.json to log column types.

    Support schema evolution: If a column is missing, throw a warning, don’t crash.

    Use a versioned schema file: schemas/v1/users_schema.json

📄 Code Snippet:

def detect_schema(df: pd.DataFrame):
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    version = datetime.now().strftime('%Y%m%d%H')
    with open(f'schemas/users_schema_{version}.json', 'w') as f:
        json.dump(schema, f, indent=2)

✅ GOAL 3: Orchestration & Modularity
🔨 Task:

    Use Airflow to orchestrate the flow.

    Write DAG with 3 tasks: extract, transform, load.

    Use Airflow logging for errors, retries, and visibility.

    Optional: use FastAPI to trigger ingestion manually.

📄 DAG Sketch (Airflow):

@dag(schedule='@daily', catchup=False)
def csv_to_pg_dag():
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> transform_task >> load_task

✅ GOAL 4: Modular Ingestion Design
🔨 Task:

    Write 3 decoupled scripts:

        extract.py → load CSV

        transform.py → clean + normalize

        load.py → push to PostgreSQL

    Use config files (config.yaml) to inject parameters.

    Enable streaming mode later using file watcher or Kafka.

✅ GOAL 5: Pandas + SQLAlchemy + DuckDB + FastAPI
🔨 Task:

    Pandas → CSV handling + transformation

    SQLAlchemy → Connect + load to PostgreSQL

    DuckDB (Optional) → Replace Pandas for better perf

    FastAPI → Build a simple /upload_csv endpoint

📄 Example:

# load.py
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@localhost/db")

df.to_sql("users", engine, if_exists="append", index=False)

✅ GOAL 6: Schema Detection, CSV Ingestion, Error Handling
🔨 Task:

    Implement schema detection as in Goal 2.

    Catch malformed rows using try/except inside Pandas.

    Log errors to /logs/errors.csv or DeadLetterQueue.csv.

📄 Error Handling:

try:
    df = pd.read_csv('input.csv')
except pd.errors.ParserError as e:
    log_error(f"Malformed file: {e}")
    move_to_deadletter('input.csv')