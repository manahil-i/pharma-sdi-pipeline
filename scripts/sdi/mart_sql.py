"""Run SQL on the DuckDB data mart from the command line (read-only, so it cannot break anything).

Examples (inside Docker):
  docker compose exec airflow-scheduler python -m sdi.mart_sql "SELECT COUNT(*) FROM fact_sales"
  docker compose exec airflow-scheduler python -m sdi.mart_sql check            <- health check of the whole mart
  docker compose exec airflow-scheduler python -m sdi.mart_sql /opt/airflow/sql/queries/q2_forecast_accuracy_flu_season.sql
  docker compose exec airflow-scheduler python -m sdi.mart_sql "EXPLAIN ANALYZE SELECT ..."   <- query plan + timings
  docker compose exec -it airflow-scheduler python -m sdi.mart_sql shell        <- type queries live (demo)
"""
import sys
import time
import uuid
from pathlib import Path

import pandas as pd

from .db import mart, pg

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)

CHECKS = {
    "Row counts": """SELECT 'fact_sales' AS table_name, COUNT(*) AS rows FROM fact_sales UNION ALL
                     SELECT 'fact_demand', COUNT(*) FROM fact_demand UNION ALL
                     SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL SELECT 'dim_medicine', COUNT(*) FROM dim_medicine UNION ALL
                     SELECT 'dim_pharmacy', COUNT(*) FROM dim_pharmacy UNION ALL SELECT 'dim_hospital', COUNT(*) FROM dim_hospital UNION ALL
                     SELECT 'dim_location', COUNT(*) FROM dim_location UNION ALL SELECT 'dim_warehouse', COUNT(*) FROM dim_warehouse""",
    "Date range per fact": """SELECT 'fact_sales' AS fact, MIN(date_key) AS first_day, MAX(date_key) AS last_day FROM fact_sales
                              UNION ALL SELECT 'fact_demand', MIN(date_key), MAX(date_key) FROM fact_demand""",
    "Rows per load batch": """SELECT 'fact_sales' AS fact, load_batch_id, COUNT(*) AS fact_rows FROM fact_sales GROUP BY ALL
                              UNION ALL SELECT 'fact_demand', load_batch_id, COUNT(*) FROM fact_demand GROUP BY ALL ORDER BY 1, 2""",
    "Bulk-posted days (data-quality flag)": """SELECT date_key, SUM(txn_count) AS sales_lines, ROUND(SUM(net_sales)) AS net_sales
                                               FROM fact_sales WHERE is_bulk_posted = 'Y' GROUP BY 1 ORDER BY 1""",
    "Unknown (-1) keys in facts (should be 0)": """SELECT
        (SELECT COUNT(*) FROM fact_sales WHERE -1 IN (medicine_key, pharmacy_key, location_key, warehouse_key)) AS sales_unknown,
        (SELECT COUNT(*) FROM fact_demand WHERE -1 IN (medicine_key, hospital_key, location_key, warehouse_key)) AS demand_unknown""",
    "SCD Type 2 history (dimension members with more than one version)": """
        SELECT 'pharmacy' AS dim, pharmacy_id AS id, pharmacy_key AS sk, pharmacy_type AS tracked_value,
               row_effective_from, row_effective_to, is_current FROM dim_pharmacy
        WHERE pharmacy_id IN (SELECT pharmacy_id FROM dim_pharmacy GROUP BY 1 HAVING COUNT(*) > 1)
        UNION ALL
        SELECT 'hospital', hospital_id, hospital_key, hospital_type, row_effective_from, row_effective_to, is_current FROM dim_hospital
        WHERE hospital_id IN (SELECT hospital_id FROM dim_hospital GROUP BY 1 HAVING COUNT(*) > 1)
        UNION ALL
        SELECT 'medicine', gtin14, medicine_key, presc_class || ' / ' || legal_control, row_effective_from, row_effective_to, is_current
        FROM dim_medicine WHERE gtin14 IN (SELECT gtin14 FROM dim_medicine GROUP BY 1 HAVING COUNT(*) > 1)
        ORDER BY 1, 2, 3""",
}


def run(sql, con):
    """Run one query and print the answer. Returns the number of rows."""
    if sql.strip().upper().startswith("EXPLAIN"):          # query plans: print the plan as text, not as a table
        rows = con.execute(sql).fetchall()
        for row in rows:
            print(row[-1])
        return len(rows)
    df = con.execute(sql).df()
    print(df.to_string(index=False))
    return len(df)


SHELL_HELP = """SDI data mart – live query shell (read-only)
  Type any SQL and end it with ;   (it may span several lines)
  Every query is timed and shown on the Grafana 'Query Performance' dashboard
  tables;            list the tables of the mart
  describe <table>;  show the columns of a table
  exit;              leave the shell
"""


SESSION = "live-" + uuid.uuid4().hex[:6]        # one id per terminal session


def log_live_query(sql, ms, status, err, rows=0, title=None):
    """Save a query you ran into monitoring.query_log (variant 'live') -> every panel of the
    Grafana 'Query Performance' dashboard includes it (counts, error rate, times, tables)."""
    name = "Live: " + (title or " ".join(sql.split()))[:70]
    try:
        with pg() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO monitoring.query_log (run_id, query_name, widget, variant, started_at,
                           duration_ms, rows_returned, status, error_message)
                           VALUES (%s, %s, 'live query', 'live', now(), %s, %s, %s, %s)""",
                        (SESSION, name, round(ms, 2), rows, status, err))
    except Exception as e:                       # logging must never stop the answer from being shown
        print("(not logged:", e, ")")


def run_live(sql, title=None):
    """Run one query, print the answer and its time, and log it for the dashboard."""
    t0 = time.perf_counter()
    status, err, rows = "success", None, 0
    try:
        with mart(read_only=True) as con:
            rows = run(sql, con)
    except Exception as e:
        status, err = "failed", str(e)[:300]
        print(f"Error: {e}")
    ms = (time.perf_counter() - t0) * 1000
    print(f"({ms:.0f} ms)\n")
    log_live_query(sql, ms, status, err, rows, title)


def shell():
    """Interactive prompt for live demos. A fresh read-only connection is opened for every query,
    so the pipeline is never blocked between queries."""
    print(SHELL_HELP)
    buf = []
    while True:
        try:
            line = input("sdi> " if not buf else " ..> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        buf.append(line)
        if not line.rstrip().endswith(";"):
            continue
        sql = " ".join(buf).strip().rstrip(";").strip()
        buf = []
        if sql.lower() in ("exit", "quit"):
            break
        if sql.lower() == "tables":
            sql = "SELECT table_name, estimated_size AS approx_rows FROM duckdb_tables() ORDER BY 1"
        elif sql.lower().startswith("describe "):
            sql = f"SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = '{sql.split()[1]}'"
        run_live(sql)


def main(arg):
    if arg == "shell":
        return shell()
    if arg == "check":                           # health check: not logged as a query
        with mart(read_only=True) as con:
            for title, sql in CHECKS.items():
                print(f"\n=== {title}")
                run(sql, con)
    elif arg.endswith(".sql") and Path(arg).exists():
        text = open(arg, encoding="utf-8").read()
        if "-- @raw" in text:                    # one of the 6 query-layer files: run its raw version
            from .query_layer import parse_query_file
            name, _, variants = parse_query_file(Path(arg))
            print(f"=== {name}")
            run_live(variants["raw"], name)
        else:                                    # a plain file with several queries separated by ;
            for sql in [q.strip() for q in text.split(";")]:
                body = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--")).strip()
                if body:
                    title = next((l.strip("- ") for l in sql.splitlines() if l.strip().startswith("--")), "")
                    print(f"\n=== {title}")
                    run_live(body, title)
    else:
        run_live(arg)


if __name__ == "__main__":
    main(" ".join(sys.argv[1:]) or "check")
