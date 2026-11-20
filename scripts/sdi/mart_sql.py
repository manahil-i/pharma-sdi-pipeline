"""Run SQL on the DuckDB data mart from the command line (read-only, so it cannot break anything).

Examples (inside Docker):
  docker compose exec airflow-scheduler python -m sdi.mart_sql "SELECT COUNT(*) FROM fact_sales"
  docker compose exec airflow-scheduler python -m sdi.mart_sql check            <- health check of the whole mart
  docker compose exec airflow-scheduler python -m sdi.mart_sql /opt/airflow/sql/queries/q2_forecast_accuracy_flu_season.sql
  docker compose exec airflow-scheduler python -m sdi.mart_sql "EXPLAIN ANALYZE SELECT ..."   <- query plan + timings
"""
import sys
from pathlib import Path

import pandas as pd

from .db import mart

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
    if sql.strip().upper().startswith("EXPLAIN"):          # query plans: print the plan as text, not as a table
        for row in con.execute(sql).fetchall():
            print(row[-1])
        return
    print(con.execute(sql).df().to_string(index=False))


def main(arg):
    with mart(read_only=True) as con:
        if arg == "check":
            for title, sql in CHECKS.items():
                print(f"\n=== {title}")
                run(sql, con)
        elif arg.endswith(".sql") and Path(arg).exists():
            from .query_layer import parse_query_file
            _, _, variants = parse_query_file(Path(arg))
            run(variants.get("raw") or open(arg).read(), con)
        else:
            run(arg, con)


if __name__ == "__main__":
    main(" ".join(sys.argv[1:]) or "check")
