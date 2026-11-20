"""Validation layer: check staged rows before they enter the data mart.

Each rule is a SQL condition. Rows that break a rule are marked dq_status = 'rejected' with the
reason; all other rows of the batch become 'valid'. Counts per rule go to monitoring.dq_log.
Order matters: master data is validated first because sales/demand check medicines against it.
"""
from . import config
from .db import pg

N = "staging.is_num"          # helper functions created in sql/postgres/01_staging_and_monitoring.sql
D = "staging.is_date"
blank = lambda c: f"COALESCE({c}, '') = ''"
# CASE guarantees the cast only runs on numeric text (OR does not guarantee evaluation order)
not_pos = lambda c: f"(CASE WHEN {N}({c}) THEN {c}::NUMERIC <= 0 ELSE TRUE END)"
negative = lambda c: f"(CASE WHEN {N}({c}) THEN {c}::NUMERIC < 0 ELSE TRUE END)"

RULES = {
    "stg_master_data": [
        ("missing_gtin", blank("gtin14")),
        ("non_positive_manufacturing_cost", not_pos("unit_manufacturing_cost_usd")),
        ("non_positive_wholesale_price", not_pos("wholesale_price_usd")),
        ("non_positive_retail_price", not_pos("recommended_retail_price_usd")),
    ],
    "stg_warehouse_data": [
        ("missing_warehouse_id", blank("warehouse_id")),
        ("invalid_snapshot_date", f"NOT {D}(snapshot_date)"),
    ],
    "stg_hospital_demand": [
        ("missing_hospital_id", blank("hospital_id")),
        ("missing_gtin", blank("gtin14")),
        ("invalid_demand_month", f"NOT {D}(demand_month)"),
        ("negative_requested_qty", negative("requested_quantity_packs")),
        ("negative_forecast_qty", negative("forecast_demand_packs")),
        ("negative_consumed_qty", negative("consumed_quantity_packs")),
        ("unknown_medicine", "gtin14 NOT IN (SELECT gtin14 FROM staging.stg_master_data WHERE dq_status = 'valid')"),
        ("duplicate_hospital_month", """EXISTS (SELECT 1 FROM staging.stg_hospital_demand o
                 WHERE o.hospital_id = t.hospital_id AND o.gtin14 = t.gtin14 AND o.demand_month = t.demand_month
                 AND ((o.dq_status = 'valid' AND o.batch_id < t.batch_id)
                      OR (o.batch_id = t.batch_id AND o.ctid < t.ctid)))"""),
    ],
    "stg_pharmacy_sales": [
        ("missing_transaction_id", blank("sales_transaction_id")),
        ("missing_gtin", blank("gtin14")),
        ("missing_pharmacy_id", blank("pharmacy_id")),
        ("invalid_sales_date", f"NOT {D}(sales_date)"),
        ("non_positive_quantity", not_pos("quantity_sold_packs")),
        ("non_positive_price", not_pos("unit_retail_price_usd")),
        ("negative_net_sales", negative("net_sales_amount_usd")),
        ("unknown_medicine", "gtin14 NOT IN (SELECT gtin14 FROM staging.stg_master_data WHERE dq_status = 'valid')"),
        # duplicate = same ID already loaded in an earlier batch, OR appears earlier in this same batch
        ("duplicate_transaction", """EXISTS (SELECT 1 FROM staging.stg_pharmacy_sales o
                 WHERE o.sales_transaction_id = t.sales_transaction_id
                 AND ((o.dq_status = 'valid' AND o.batch_id < t.batch_id)
                      OR (o.batch_id = t.batch_id AND o.ctid < t.ctid)))"""),
    ],
}
ORDER = ["stg_master_data", "stg_warehouse_data", "stg_hospital_demand", "stg_pharmacy_sales"]


def validate_staging():
    """Airflow task: validate every batch with status 'ingested'."""
    summary = []
    with pg() as conn, conn.cursor() as cur:
        # indexes that keep the duplicate checks fast
        for sql in ["CREATE INDEX IF NOT EXISTS ix_sales_txn ON staging.stg_pharmacy_sales (sales_transaction_id)",
                    "CREATE INDEX IF NOT EXISTS ix_demand_key ON staging.stg_hospital_demand (hospital_id, gtin14, demand_month)",
                    "CREATE INDEX IF NOT EXISTS ix_master_gtin ON staging.stg_master_data (gtin14)"]:
            table_name = sql.split("ON ")[1].split(" ")[0]
            cur.execute("SELECT to_regclass(%s)", (table_name,))
            if cur.fetchone()[0]:
                cur.execute(sql)
        for table in ORDER:
            cur.execute("""SELECT batch_id FROM monitoring.etl_batches
                           WHERE staging_table = %s AND status = 'ingested' ORDER BY batch_id""", (table,))
            for (batch_id,) in cur.fetchall():
                rejected = 0
                for rule, cond in RULES[table]:
                    cur.execute(f"""UPDATE staging.{table} t
                                    SET dq_status = 'rejected',
                                        dq_reason = COALESCE(dq_reason || '; ', '') || %s
                                    WHERE t.batch_id = %s AND ({cond})""", (rule, batch_id))
                    failed = cur.rowcount
                    rejected += failed
                    cur.execute("""INSERT INTO monitoring.dq_log (batch_id, staging_table, rule_name, failed_rows)
                                   VALUES (%s, %s, %s, %s)""", (batch_id, table, rule, failed))
                cur.execute(f"""UPDATE staging.{table} SET dq_status = 'valid'
                                WHERE batch_id = %s AND dq_status = 'pending'""", (batch_id,))
                valid = cur.rowcount
                cur.execute("""UPDATE monitoring.etl_batches SET status = 'validated', validated_at = now()
                               WHERE batch_id = %s""", (batch_id,))
                summary.append(dict(table=table, batch_id=batch_id, valid=valid, rejected_rule_hits=rejected))
                print(f"[validate] {table} batch {batch_id}: {valid:,} valid, {rejected} rule failures")
    return summary
