"""Data ingestion layer: CSV files in data/landing -> PostgreSQL staging tables.

* Batch mode  : the whole file is read with pandas and bulk-copied into staging (fast COPY).
* Stream mode : files whose name contains '_stream' are fed in small micro-batches to simulate
                live events; each micro-batch is checked for schema problems and timed.
Every load writes to the monitoring tables that feed the Grafana dashboards.

This module is the ingestion "API": ingest_batch_file(path) and ingest_stream_file(path) can be
called by Airflow, by a script, or (through Airflow's REST API) by any external system.
"""
import io
import re
import shutil
import time
import datetime as dt
from pathlib import Path

import pandas as pd

from . import config
from .db import pg, pg_query, run_sql_file

META_COLS = ["batch_id", "dq_status", "dq_reason", "loaded_at"]


# ---------------------------------------------------------------- helpers
def snake(name):
    """SalesTransactionID -> sales_transaction_id ; GTIN14 -> gtin14"""
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    return s.lower()


def setup_databases():
    """Create schemas, log tables and helper functions (safe to run every time)."""
    run_sql_file(config.SQL_DIR / "postgres" / "01_staging_and_monitoring.sql")


def staging_table_for(path):
    for prefix, table in config.FILE_TABLES.items():
        if Path(path).name.lower().startswith(prefix):      # not case-sensitive
            return table
    return None


def landing_files():
    """All CSV files waiting in the landing folder that we know how to load (sorted: 01, 04, 05, 06).
    A file is only taken when its size stays the same for 2 seconds, so a file that is still being
    copied into the folder is left for the next run instead of being loaded half-finished."""
    files = [p for p in sorted(config.LANDING.glob("*.csv")) if staging_table_for(p)]
    if not files:
        return []
    sizes = {p: p.stat().st_size for p in files}
    time.sleep(2)
    return [p for p in files if p.exists() and p.stat().st_size == sizes[p] and sizes[p] > 0]


def files_waiting():
    """Used by the Airflow file-watcher sensor: True as soon as a known CSV is in data/landing."""
    files = landing_files()
    if files:
        print("Files waiting:", [f.name for f in files])
    return bool(files)


def ensure_staging_table(cur, table, columns):
    """Create the staging table from the CSV header (all TEXT) and add any new columns."""
    cols = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f"""CREATE TABLE IF NOT EXISTS staging.{table} ({cols},
                    batch_id INTEGER, dq_status TEXT DEFAULT 'pending', dq_reason TEXT,
                    loaded_at TIMESTAMP DEFAULT now())""")
    for c in columns:
        cur.execute(f'ALTER TABLE staging.{table} ADD COLUMN IF NOT EXISTS "{c}" TEXT')
    cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{table}_batch ON staging.{table} (batch_id, dq_status)")


def copy_frame(cur, table, df):
    """Bulk insert a dataframe into a staging table with PostgreSQL COPY (much faster than INSERTs)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ", ".join(f'"{c}"' for c in df.columns)
    cur.copy_expert(f"COPY staging.{table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)


def new_batch(cur, file_name, table, mode):
    cur.execute("""INSERT INTO monitoring.etl_batches (file_name, staging_table, mode, status)
                   VALUES (%s, %s, %s, 'ingesting') RETURNING batch_id""", (file_name, table, mode))
    return cur.fetchone()[0]


def move(path, folder):
    folder.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    target = folder / f"{stamp}__{Path(path).name}"
    shutil.move(str(path), str(target))
    return target


def read_csv(path, chunksize=None):
    """Read everything as text so nothing is lost or changed before validation.
    With chunksize the file is read in pieces (keeps memory low for the 220 MB sales file)."""
    def fix(df):
        df.columns = [snake(c) for c in df.columns]
        return df
    if chunksize:
        return (fix(c) for c in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=chunksize))
    return fix(pd.read_csv(path, dtype=str, keep_default_na=False))


# ---------------------------------------------------------------- batch ingestion
def ingest_batch_file(path):
    path, table = Path(path), staging_table_for(path)
    started = dt.datetime.now()
    t0 = time.perf_counter()
    batch_id, rows_read, status, err = None, 0, "success", None
    try:
        with pg() as conn, conn.cursor() as cur:             # one transaction: all rows or nothing
            for i, df in enumerate(read_csv(path, chunksize=100_000)):
                if i == 0:
                    ensure_staging_table(cur, table, list(df.columns))
                    batch_id = new_batch(cur, path.name, table, "batch")
                df["batch_id"] = batch_id
                copy_frame(cur, table, df)
                rows_read += len(df)                          # "Rows processed per batch" = len(df)
            cur.execute("UPDATE monitoring.etl_batches SET status='ingested', rows_loaded=%s WHERE batch_id=%s",
                        (rows_read, batch_id))
        move(path, config.ARCHIVE)
    except Exception as e:                                   # log the failure, keep the pipeline running
        status, err = "failed", str(e)[:500]
        if path.exists():
            move(path, config.FAILED)
    duration = time.perf_counter() - t0                       # "Batch load duration" = end - start
    with pg() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO monitoring.batch_ingestion_log
                       (batch_id, file_name, staging_table, started_at, ended_at, duration_sec, rows_read,
                        rows_loaded, status, error_message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (batch_id, path.name, table, started, dt.datetime.now(), round(duration, 3), rows_read,
                     rows_read if status == "success" else 0, status, err))
    print(f"[batch] {path.name}: {status}, {rows_read:,} rows in {duration:.1f}s")
    return dict(file=path.name, status=status, rows=rows_read, seconds=round(duration, 2), batch_id=batch_id)


def ingest_batch_files():
    """Airflow task: load every non-stream file in data/landing."""
    results = [ingest_batch_file(p) for p in landing_files() if config.STREAM_MARKER not in p.name]
    print("Batch ingestion summary:", results)
    return results


# ---------------------------------------------------------------- stream ingestion
KEY_FIELDS = {
    "stg_pharmacy_sales": ("sales_transaction_id", "gtin14", "pharmacy_id", "sales_date"),
    "stg_hospital_demand": ("hospital_demand_record_id", "gtin14", "hospital_id", "demand_month"),
}
DATE_FIELD = {"stg_pharmacy_sales": "sales_date", "stg_hospital_demand": "demand_month"}


def schema_problems(df, table):
    """Return a Series with the reason a row is malformed ('' = row is fine)."""
    reason = pd.Series("", index=df.index)
    for col in KEY_FIELDS.get(table, ()):
        if col not in df.columns:
            reason[:] = f"missing column {col}"
            return reason
        reason = reason.where(df[col].str.strip() != "", reason + f"null {col}; ")
    date_col = DATE_FIELD.get(table)
    if date_col in df.columns:
        ok = pd.to_datetime(df[date_col], format="%Y-%m-%d", errors="coerce").notna()
        reason = reason.where(ok | (df[date_col] == ""), reason + f"bad {date_col}; ")
    return reason


def ingest_stream_file(path):
    path, table = Path(path), staging_table_for(path)
    df = read_csv(path)
    with pg() as conn, conn.cursor() as cur:
        ensure_staging_table(cur, table, list(df.columns))
        batch_id = new_batch(cur, path.name, table, "stream")
    loaded = dropped = 0
    n = config.STREAM_MICRO_BATCH
    stream_start = time.perf_counter()
    for i, start in enumerate(range(0, len(df), n), 1):
        chunk = df.iloc[start:start + n].copy()
        emitted_at = dt.datetime.now()                        # simulated event creation time
        t0 = time.perf_counter()
        reasons = schema_problems(chunk, table)
        bad, good = chunk[reasons != ""], chunk[reasons == ""].copy()
        good["batch_id"] = batch_id
        with pg() as conn, conn.cursor() as cur:
            if len(good):
                copy_frame(cur, table, good)
            for idx, row in bad.iterrows():                   # malformed / dropped records
                cur.execute("""INSERT INTO monitoring.stream_rejects (batch_id, micro_batch_no, reason, raw_record)
                               VALUES (%s,%s,%s,%s)""", (batch_id, i, reasons[idx].strip(), row.to_json()))
        inserted_at = dt.datetime.now()                       # committed to the database
        secs = max(time.perf_counter() - t0, 1e-6)
        with pg() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO monitoring.stream_ingestion_log (batch_id, file_name, micro_batch_no,
                           events_received, events_loaded, malformed_count, emitted_at, inserted_at, latency_sec,
                           throughput_eps) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (batch_id, path.name, i, len(chunk), len(good), len(bad), emitted_at, inserted_at,
                         round((inserted_at - emitted_at).total_seconds(), 3), round(len(chunk) / secs, 2)))
        loaded += len(good)
        dropped += len(bad)
        time.sleep(config.STREAM_PAUSE_SEC)                   # gap between micro-batches = "live" feel
    total = time.perf_counter() - stream_start
    with pg() as conn, conn.cursor() as cur:
        cur.execute("UPDATE monitoring.etl_batches SET status='ingested', rows_loaded=%s WHERE batch_id=%s",
                    (loaded, batch_id))
    move(path, config.ARCHIVE)
    print(f"[stream] {path.name}: {loaded:,} loaded, {dropped} malformed dropped, "
          f"{len(df) / total:.0f} events/sec overall")
    return dict(file=path.name, loaded=loaded, dropped=dropped, seconds=round(total, 1), batch_id=batch_id)


def ingest_stream_files():
    """Airflow task: stream every '*_stream*.csv' file in data/landing."""
    results = [ingest_stream_file(p) for p in landing_files() if config.STREAM_MARKER in p.name]
    print("Stream ingestion summary:", results)
    return results
