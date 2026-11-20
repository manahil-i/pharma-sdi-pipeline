"""Analytical query layer + query optimisation + query metrics.

build_summary_tables(): rebuilds the pre-aggregated summary tables (sql/duckdb/02_summary_tables.sql)
run_queries():          runs every query in sql/queries/ twice –
                          'raw'     = joins the big fact tables (no optimisation)
                          'summary' = reads the small pre-aggregated table (optimised)
                        and logs execution time, rows returned and success/failure to
                        monitoring.query_log in PostgreSQL (the Grafana query dashboard reads it).
Each query file has the form:
    -- name: ...
    -- widget: ...
    -- @raw
    <sql>
    -- @summary
    <sql>
"""
import re
import time
import uuid
import datetime as dt

from . import config
from .db import mart, pg

QUERY_DIR = config.SQL_DIR / "queries"
REPEATS = 3          # each variant runs 3 times; we log every run (median is shown on the dashboard)


def build_summary_tables():
    t0 = time.perf_counter()
    with mart() as con:
        con.execute(open(config.SQL_DIR / "duckdb" / "02_summary_tables.sql", encoding="utf-8").read())
        sizes = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                 for t in ("agg_sales_month", "agg_demand_month", "agg_stock_cover",
                           "agg_coldchain_wh_quarter", "agg_sales_day")}
    print(f"[summary tables] rebuilt in {time.perf_counter() - t0:.2f}s:", sizes)
    return sizes


def parse_query_file(path):
    text = open(path, encoding="utf-8").read()
    meta = dict(re.findall(r"^--\s*(name|widget|question):\s*(.+)$", text, flags=re.M))
    parts = re.split(r"^--\s*@(raw|summary)\s*$", text, flags=re.M)
    variants = {parts[i]: parts[i + 1].strip().rstrip(";") for i in range(1, len(parts) - 1, 2)}
    return meta.get("name", path.stem), meta.get("widget", ""), variants


def run_queries():
    run_id = uuid.uuid4().hex[:8]
    logs, results = [], {}
    with mart(read_only=True) as con:
        for path in sorted(QUERY_DIR.glob("*.sql")):
            name, widget, variants = parse_query_file(path)
            for variant, sql in variants.items():
                try:
                    con.execute(sql).fetchall()     # warm-up run (not logged) so every query starts with a warm cache
                except Exception:
                    pass
                times = []
                for _ in range(REPEATS):
                    started = dt.datetime.now()
                    t0 = time.perf_counter()
                    try:
                        rows = con.execute(sql).fetchall()
                        status, err, n = "success", None, len(rows)
                    except Exception as e:      # a failing query is logged, not fatal
                        status, err, n = "failed", str(e)[:500], 0
                    ms = (time.perf_counter() - t0) * 1000
                    logs.append((run_id, name, widget, variant, started, round(ms, 2), n, status, err))
                    times.append(ms)
                results[f"{name} [{variant}]"] = f"{status}, {n} rows, median {sorted(times)[len(times) // 2]:.1f} ms"
    with pg() as conn, conn.cursor() as cur:
        cur.executemany("""INSERT INTO monitoring.query_log (run_id, query_name, widget, variant, started_at,
                           duration_ms, rows_returned, status, error_message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        logs)
    for k, v in results.items():
        print(f"[query] {k}: {v}")
    return results


def show(query_file, variant="summary"):
    """Handy for testing: print the result of one query file as a table."""
    _, _, variants = parse_query_file(QUERY_DIR / query_file)
    with mart(read_only=True) as con:
        print(con.execute(variants[variant]).df().to_string(index=False))


if __name__ == "__main__":
    build_summary_tables()
    run_queries()
