"""Central settings for the SDI (Sales & Demand Intelligence) pipeline.

Every path and connection setting lives here, so nothing is hard-coded anywhere else.
Inside Docker the defaults below are correct; for local testing you can override them
with environment variables (see README).
"""
import os
from pathlib import Path

# ---------------------------------------------------------------- folders
HOME = Path(os.getenv("SDI_HOME", "/opt/airflow"))
DATA = HOME / "data"
LANDING = DATA / "landing"        # drop CSV files here -> the pipeline picks them up
ARCHIVE = DATA / "archive"        # files move here after a successful load
FAILED = DATA / "failed"          # files move here if loading fails
GENERATED = DATA / "generated"    # generator statistics (for your PDF report)
SOURCE = DATA / "source"          # the ORIGINAL 4 CSV files (generator reads these)
SQL_DIR = HOME / "sql"
MART_FILE = Path(os.getenv("SDI_MART_FILE", str(HOME / "mart" / "sdi_mart.duckdb")))

# ---------------------------------------------------------------- PostgreSQL (staging + monitoring)
PG = dict(
    host=os.getenv("PHARMA_DB_HOST", "postgres"),
    port=int(os.getenv("PHARMA_DB_PORT", "5432")),
    dbname=os.getenv("PHARMA_DB_NAME", "pharma"),
    user=os.getenv("PHARMA_DB_USER", "airflow"),
    password=os.getenv("PHARMA_DB_PASSWORD", "airflow"),
)

# ---------------------------------------------------------------- which file goes to which staging table
# file name prefix -> staging table
FILE_TABLES = {
    "01_master_data": "stg_master_data",
    "04_warehouse_data": "stg_warehouse_data",
    "05_pharmacy_sales_data": "stg_pharmacy_sales",
    "06_hospital_demand_data": "stg_hospital_demand",
}
STREAM_MARKER = "_stream"          # files with this in the name are ingested as a stream

# ---------------------------------------------------------------- streaming simulation
STREAM_MICRO_BATCH = int(os.getenv("SDI_STREAM_MICRO_BATCH", "500"))   # rows per micro-batch
STREAM_PAUSE_SEC = float(os.getenv("SDI_STREAM_PAUSE_SEC", "0.5"))     # pause between micro-batches

# ---------------------------------------------------------------- business rules
BULK_POSTING_FACTOR = 10           # a day with > 10x the median daily sales count = bulk-posted
CALENDAR_START, CALENDAR_END = "2023-01-01", "2027-12-31"
