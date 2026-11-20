"""Storage metrics: how much data is stored in each layer (feeds the Grafana storage dashboard).

For every table we record row count, size in MB, the source domain (sales / demand / master /
warehouse / monitoring) and the first and last business date it covers, in monitoring.storage_log.
"""
import os
import datetime as dt

from . import config
from .db import mart, pg

STAGING = {  # staging table -> (domain, date column)
    "stg_master_data": ("Master data", "effective_from_date"),
    "stg_warehouse_data": ("Warehouse", "snapshot_date"),
    "stg_pharmacy_sales": ("Pharmacy sales", "sales_date"),
    "stg_hospital_demand": ("Hospital demand", "demand_month"),
}
MART = {  # mart table -> (domain, date expression)
    "fact_sales": ("Pharmacy sales", "date_key"), "fact_demand": ("Hospital demand", "date_key"),
    "dim_date": ("Calendar", "date_key"), "dim_medicine": ("Master data", None), "dim_pharmacy": ("Pharmacy sales", None),
    "dim_hospital": ("Hospital demand", None), "dim_location": ("Location", None), "dim_warehouse": ("Warehouse", None),
    "agg_sales_month": ("Summary", None), "agg_demand_month": ("Summary", None), "agg_stock_cover": ("Summary", None),
    "agg_coldchain_wh_quarter": ("Summary", None), "agg_sales_day": ("Summary", None),
}


def key_to_date(k):
    return dt.datetime.strptime(str(k), "%Y%m%d").date() if k else None


def collect_storage_metrics():
    now = dt.datetime.now()
    rows = []
    with pg() as conn, conn.cursor() as cur:
        for table, (domain, date_col) in STAGING.items():
            cur.execute("SELECT to_regclass(%s)", (f"staging.{table}",))
            if cur.fetchone()[0] is None:
                continue
            cur.execute(f"""SELECT COUNT(*), pg_total_relation_size('staging.{table}') / 1048576.0,
                                   MIN(CASE WHEN staging.is_date({date_col}) THEN {date_col}::date END),
                                   MAX(CASE WHEN staging.is_date({date_col}) THEN {date_col}::date END)
                            FROM staging.{table}""")
            n, mb, dmin, dmax = cur.fetchone()
            rows.append(("staging (PostgreSQL)", table, domain, n, round(float(mb), 2), dmin, dmax))
        cur.execute("""SELECT relname, n_live_tup, pg_total_relation_size(relid) / 1048576.0
                       FROM pg_stat_user_tables WHERE schemaname = 'monitoring'""")
        for name, n, mb in cur.fetchall():
            rows.append(("monitoring (PostgreSQL)", name, "Monitoring", n, round(float(mb), 2), None, None))
        cur.execute("SELECT pg_database_size(current_database()) / 1048576.0")
        rows.append(("staging (PostgreSQL)", "_whole_database", "All", None, round(float(cur.fetchone()[0]), 2), None, None))

    if config.MART_FILE.exists():
        with mart(read_only=True) as con:
            # DuckDB stores compressed column blocks; we share the file size by each table's estimated size
            est = dict(con.execute("SELECT table_name, estimated_size FROM duckdb_tables()").fetchall())
            total_est = sum(v or 0 for v in est.values()) or 1
            file_mb = os.path.getsize(config.MART_FILE) / 1048576
            for table, (domain, date_col) in MART.items():
                if table not in est:
                    continue
                n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                dmin = dmax = None
                if date_col:
                    lo, hi = con.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table} WHERE {date_col} > 0").fetchone()
                    dmin, dmax = key_to_date(lo), key_to_date(hi)
                rows.append(("mart (DuckDB)", table, domain, n, round(file_mb * (est[table] or 0) / total_est, 2), dmin, dmax))
            rows.append(("mart (DuckDB)", "_whole_file", "All", None, round(file_mb, 2), None, None))

    with pg() as conn, conn.cursor() as cur:
        cur.executemany("""INSERT INTO monitoring.storage_log (captured_at, layer, table_name, source_domain, row_count,
                           size_mb, min_date, max_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [(now, *r) for r in rows])
    for r in rows:
        print(f"[storage] {r[0]:<24} {r[1]:<26} rows={r[3]}  {r[4]} MB  {r[5]} .. {r[6]}")
    return len(rows)


if __name__ == "__main__":
    collect_storage_metrics()
