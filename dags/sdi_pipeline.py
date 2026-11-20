"""DAG 1 – sdi_pipeline: the full Sales & Demand Intelligence pipeline.

Runs every 5 minutes. The first task is a FILE WATCHER (sensor): if no CSV file is waiting in
data/landing, the run is skipped. When files arrive, all steps run in order:

 wait_for_files -> setup_databases -> ingest_batch_files -> ingest_stream_files -> validate_staging
   -> load_dimensions -> load_facts -> build_summary_tables -> collect_storage_metrics -> run_analytical_queries
"""
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from sdi import ingest, validate, build_mart, query_layer, storage_metrics

default_args = dict(owner="sdi", retries=1, retry_delay=dt.timedelta(minutes=1))

with DAG(
    dag_id="sdi_pipeline",
    description="SDI: landing -> Postgres staging -> DuckDB star schema -> query layer",
    start_date=dt.datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sdi", "milestone1"],
) as dag:

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=ingest.files_waiting,
        poke_interval=30,          # look into data/landing every 30 seconds
        timeout=240,               # give up after 4 min -> run is 'skipped' (next run in 5 min)
        mode="reschedule",         # do not block a worker slot while waiting
        soft_fail=True,
    )

    def task(task_id, fn):
        return PythonOperator(task_id=task_id, python_callable=fn)

    setup = task("setup_databases", ingest.setup_databases)
    batch = task("ingest_batch_files", ingest.ingest_batch_files)
    stream = task("ingest_stream_files", ingest.ingest_stream_files)
    check = task("validate_staging", validate.validate_staging)
    dims = task("load_dimensions", build_mart.load_dimensions)
    facts = task("load_facts", build_mart.load_facts)
    summary = task("build_summary_tables", query_layer.build_summary_tables)
    storage = task("collect_storage_metrics", storage_metrics.collect_storage_metrics)
    queries = task("run_analytical_queries", query_layer.run_queries)

    (wait_for_files >> setup >> batch >> stream >> check
     >> dims >> facts >> summary >> storage >> queries)
