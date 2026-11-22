"""SDI Ingestion API – the front door of the pipeline.

Other systems (or you, in the demo) send data here; the API checks it and places it in data/landing,
where the Airflow file watcher picks it up. It can also start the generator and the pipeline in Airflow.

Endpoints (try them all in the browser at http://localhost:8000/docs):
    GET  /health                  is the API alive?
    POST /files                   upload a CSV file (e.g. a new month of sales) -> data/landing
    POST /records/{source}        send rows as JSON (source = sales or demand) -> data/landing
    POST /generate/{refresh}      start the data generator in Airflow (refresh 1, 2, ...)
    POST /pipeline/run            start the pipeline in Airflow now (instead of waiting up to 5 minutes)
    GET  /status                  files waiting, last loads, last Airflow runs
    GET  /requests                the last calls made to this API (also shown in Grafana)
Every call is logged in monitoring.api_requests.
"""
import csv
import datetime as dt
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List

import psycopg2
from fastapi import FastAPI, File, HTTPException, Query, UploadFile

from sdi import config                       # same folders, database settings and file names as the pipeline
from sdi.api_trigger import call, trigger   # small client for the Airflow REST API

app = FastAPI(title="SDI Ingestion API",
              description="Send data into the Sales & Demand Intelligence pipeline and control it.",
              version="1.0")

# columns that must be present for a file to be accepted (per staging table)
REQUIRED = {
    "stg_master_data": ["MedicineID", "GTIN14"],
    "stg_warehouse_data": ["InventorySnapshotID", "WarehouseID", "SnapshotDate"],
    "stg_pharmacy_sales": ["SalesTransactionID", "SalesDate", "GTIN14", "PharmacyID", "QuantitySoldPacks"],
    "stg_hospital_demand": ["HospitalDemandRecordID", "DemandMonth", "HospitalID", "GTIN14"],
}
SOURCES = {"sales": "05_pharmacy_sales_data", "demand": "06_hospital_demand_data"}   # for /records


# ------------------------------------------------------------------ helpers
def log_request(endpoint, file_name=None, rows=None, status="success", message=""):
    """Write one row into monitoring.api_requests (never lets a logging problem break the API)."""
    try:
        with psycopg2.connect(**config.PG) as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO monitoring.api_requests (endpoint, file_name, rows_received, status, message)
                           VALUES (%s, %s, %s, %s, %s)""", (endpoint, file_name, rows, status, message[:500]))
    except Exception as e:
        print("could not log request:", e)


def stamp():
    """Time stamp + random part, so two calls in the same second never get the same file name."""
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]


def prefix_of(name):
    """'05_Pharmacy_Sales_Data_march.csv' -> '05_pharmacy_sales_data' (or None if unknown)."""
    return next((p for p in config.FILE_TABLES if name.lower().startswith(p)), None)


def publish(tmp_path, final_name):
    """Rename the finished '.part' file into its real name -> only now can the file watcher see it."""
    final = config.LANDING / final_name
    os.replace(tmp_path, final)
    return final


@app.on_event("startup")
def create_log_table():
    try:
        with psycopg2.connect(**config.PG) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS monitoring")
            cur.execute("""CREATE TABLE IF NOT EXISTS monitoring.api_requests (
                               request_id SERIAL PRIMARY KEY, requested_at TIMESTAMP DEFAULT now(),
                               endpoint TEXT, file_name TEXT, rows_received INTEGER, status TEXT, message TEXT)""")
    except Exception as e:
        print("database not ready yet:", e)


# ------------------------------------------------------------------ endpoints
@app.get("/health")
def health():
    return {"status": "ok", "landing_folder": str(config.LANDING), "time": dt.datetime.now().isoformat()}


@app.post("/files")
def upload_file(file: UploadFile = File(..., description="CSV file whose name starts with 01_, 04_, 05_ or 06_ "
                                                         "(e.g. 05_pharmacy_sales_data_july.csv)"),
                stream: bool = Query(False, description="true = load it as a stream (micro-batches) "
                                                        "instead of one batch")):
    """Upload a CSV file. It is checked, saved into data/landing and loaded by the next pipeline run."""
    name = Path(file.filename or "").name
    prefix = prefix_of(name)
    if not name.lower().endswith(".csv") or prefix is None:
        log_request("/files", name, status="rejected", message="unknown file name")
        raise HTTPException(400, f"File name must end with .csv and start with one of: {list(config.FILE_TABLES)}")

    config.LANDING.mkdir(parents=True, exist_ok=True)
    marker = config.STREAM_MARKER if stream else ""
    final_name = f"{prefix}_api_{stamp()}{marker}.csv"
    tmp = config.LANDING / (final_name + ".part")         # '.part' files are ignored by the pipeline
    with open(tmp, "wb") as out:
        shutil.copyfileobj(file.file, out)                 # works for big files too (streams to disk)

    with open(tmp, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    missing = [c for c in REQUIRED[config.FILE_TABLES[prefix]] if c not in header]
    if missing or rows == 0:
        tmp.unlink(missing_ok=True)
        msg = f"missing columns {missing}" if missing else "file has no data rows"
        log_request("/files", name, rows, "rejected", msg)
        raise HTTPException(400, f"File rejected: {msg}")

    publish(tmp, final_name)
    log_request("/files", final_name, rows, "success", "stream" if stream else "batch")
    return {"saved_as": final_name, "rows": rows, "mode": "stream" if stream else "batch",
            "next_step": "The pipeline loads it within 5 minutes, or call POST /pipeline/run to start now."}


@app.post("/records/{source}")
def send_records(source: str, records: List[Dict[str, Any]]):
    """Send rows as JSON, e.g. new sales from shop tills. source = 'sales' (loaded as a stream) or 'demand'.
    Each row uses the same column names as the original CSV file (SalesTransactionID, SalesDate, GTIN14 ...)."""
    if source not in SOURCES:
        raise HTTPException(404, "source must be 'sales' or 'demand'")
    if not records:
        raise HTTPException(400, "send at least one record")
    prefix = SOURCES[source]
    with open(config.SOURCE / f"{prefix}.csv", newline="", encoding="utf-8-sig") as f:
        columns = next(csv.reader(f))                      # same column order as the original file
    unknown = sorted({k for r in records for k in r} - set(columns))
    missing = [c for c in REQUIRED[config.FILE_TABLES[prefix]] if any(c not in r for r in records)]
    if unknown or missing:
        msg = f"unknown columns {unknown}" if unknown else f"every record needs {missing}"
        log_request(f"/records/{source}", None, len(records), "rejected", msg)
        raise HTTPException(400, msg)

    marker = config.STREAM_MARKER if source == "sales" else ""
    final_name = f"{prefix}_api_{stamp()}{marker}.csv"
    tmp = config.LANDING / (final_name + ".part")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, restval="")
        writer.writeheader()
        writer.writerows(records)
    publish(tmp, final_name)
    log_request(f"/records/{source}", final_name, len(records), "success", "")
    return {"saved_as": final_name, "rows": len(records), "mode": "stream" if marker else "batch"}


@app.post("/generate/{refresh}")
def generate(refresh: int):
    """Start the data generator DAG in Airflow (refresh 1 = Jan-Mar 2026, refresh 2 = Apr-Jun 2026, ...)."""
    if refresh < 1:
        raise HTTPException(400, "refresh must be 1 or higher")
    try:
        run = trigger("sdi_generate_data", {"refresh": refresh})
    except Exception as e:
        log_request(f"/generate/{refresh}", status="failed", message=str(e))
        raise HTTPException(502, f"Airflow could not be reached: {e}")
    log_request(f"/generate/{refresh}", status="success", message=run["dag_run_id"])
    return {"started": "sdi_generate_data", "run_id": run["dag_run_id"], "state": run["state"], "refresh": refresh}


@app.post("/pipeline/run")
def run_pipeline():
    """Start the sdi_pipeline DAG now."""
    try:
        run = trigger("sdi_pipeline")
    except Exception as e:
        log_request("/pipeline/run", status="failed", message=str(e))
        raise HTTPException(502, f"Airflow could not be reached: {e}")
    log_request("/pipeline/run", status="success", message=run["dag_run_id"])
    return {"started": "sdi_pipeline", "run_id": run["dag_run_id"], "state": run["state"]}


@app.get("/status")
def status():
    """Files waiting in data/landing, the last 10 loads and the last Airflow runs."""
    waiting = [{"file": p.name, "mb": round(p.stat().st_size / 1048576, 2)}
               for p in sorted(config.LANDING.glob("*.csv"))]
    loads = []
    try:
        with psycopg2.connect(**config.PG) as conn, conn.cursor() as cur:
            cur.execute("""SELECT batch_id, file_name, mode, status, rows_loaded, ingested_at
                           FROM monitoring.etl_batches ORDER BY batch_id DESC LIMIT 10""")
            loads = [dict(zip(["batch_id", "file", "mode", "status", "rows", "ingested_at"], r)) for r in cur.fetchall()]
    except Exception:
        pass                                               # no loads yet
    runs = {}
    for dag_id in ("sdi_generate_data", "sdi_pipeline"):
        try:
            data = call("GET", f"/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=3")["dag_runs"]
            runs[dag_id] = [{"run_id": r["dag_run_id"], "state": r["state"]} for r in data]
        except Exception as e:
            runs[dag_id] = f"Airflow not reachable: {e}"
    return {"files_waiting": waiting, "last_loads": loads, "airflow_runs": runs}


@app.get("/requests")
def requests_log(limit: int = 50):
    """The last calls made to this API."""
    with psycopg2.connect(**config.PG) as conn, conn.cursor() as cur:
        cur.execute("""SELECT request_id, requested_at, endpoint, file_name, rows_received, status, message
                       FROM monitoring.api_requests ORDER BY request_id DESC LIMIT %s""", (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
