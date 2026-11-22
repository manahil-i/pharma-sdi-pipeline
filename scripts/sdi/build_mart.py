"""Transformation & dimensional modelling layer: PostgreSQL staging -> DuckDB star schema.

load_dimensions(): builds/updates the 6 dimensions (SCD Type 1 and Type 2)
load_facts():      builds FactSales (daily) and FactDemand (monthly) with surrogate-key lookups
New data is ADDED (incremental load) – the mart is never emptied, so history is kept.
"""
import io
import datetime as dt

import pandas as pd

from . import config, reference as ref
from .db import pg, mart

START_OF_HISTORY = dt.date(2023, 1, 1)


# =============================================================== reading from staging
def validated_batches(table):
    with pg() as conn, conn.cursor() as cur:
        cur.execute("""SELECT batch_id FROM monitoring.etl_batches
                       WHERE staging_table = %s AND status = 'validated' ORDER BY batch_id""", (table,))
        return [r[0] for r in cur.fetchall()]


def read_staging(table, batch_ids, columns):
    """Valid rows of the given batches, read with a fast COPY (all text)."""
    if not batch_ids:
        return pd.DataFrame(columns=columns + ["batch_id"])
    cols = ", ".join(columns + ["batch_id"])
    ids = ",".join(str(b) for b in batch_ids)
    buf = io.StringIO()
    with pg() as conn, conn.cursor() as cur:
        cur.copy_expert(f"""COPY (SELECT {cols} FROM staging.{table}
                            WHERE dq_status = 'valid' AND batch_id IN ({ids})) TO STDOUT WITH (FORMAT csv, HEADER)""", buf)
    buf.seek(0)
    return pd.read_csv(buf, dtype=str, keep_default_na=False)


def mark_loaded(batch_ids):
    if batch_ids:
        with pg() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE monitoring.etl_batches SET status = 'loaded', loaded_at = now()
                           WHERE batch_id = ANY(%s)""", (batch_ids,))


# =============================================================== mart setup
UNKNOWN_ROWS = {
    "dim_medicine": "(-1, 'UNKNOWN', 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'N/A', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'N', 'N', 0, 'Unknown', DATE '2023-01-01', DATE '9999-12-31', 'Y')",
    "dim_pharmacy": "(-1, 'UNKNOWN', 'N/A', 'Unknown', 'Unknown', 'Unknown', 'N/A', DATE '2023-01-01', DATE '9999-12-31', 'Y')",
    "dim_hospital": "(-1, 'UNKNOWN', 'N/A', 'Unknown', 'UNK', 'Unknown', NULL, 'N/A', NULL, 'N/A', NULL, 'N/A', 'N/A', NULL, DATE '2023-01-01', DATE '9999-12-31', 'Y')",
    "dim_location": "(-1, 'Unknown', 'NA', 'Unknown', 'Unknown', 'Unknown', 'NA', NULL, NULL, NULL, 'Unknown', 'Unknown', NULL, NULL, 'Unknown', 0, 0)",
    "dim_warehouse": "(-1, 'UNKNOWN', 'N/A', 'Unknown', 'UNK', 'Unknown', 'Unknown', 'N', NULL, 'Unknown')",
}


def ensure_mart(con):
    """Create the tables, the 'Unknown' (-1) member rows and the calendar."""
    con.execute(open(config.SQL_DIR / "duckdb" / "01_create_mart.sql", encoding="utf-8").read())
    for table, values in UNKNOWN_ROWS.items():
        key = table.replace("dim_", "") + "_key"
        if con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} = -1").fetchone()[0] == 0:
            con.execute(f"INSERT INTO {table} VALUES {values}")
    if con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0] == 0:
        cal = pd.DataFrame(ref.calendar_rows(config.CALENDAR_START, config.CALENDAR_END), columns=ref.DATE_COLUMNS)
        con.register("cal_df", cal)
        con.execute(f"INSERT INTO dim_date SELECT {', '.join(ref.DATE_COLUMNS)} FROM cal_df")
        con.unregister("cal_df")


# =============================================================== generic dimension merge (SCD 1 + SCD 2)
def merge_dimension(con, dim, sk, nk, inc, type1, type2, seq, scd2=True):
    """Merge incoming rows into a dimension.
    type1 columns: overwritten in place (no history).
    type2 columns: a change closes the current row (row_effective_to, is_current='N')
                   and inserts a new row with a new surrogate key.
    inc must contain nk, all type1/type2 columns and 'eff_from' (date the new version starts)."""
    if inc.empty:
        return dict(new=0, changed=0)
    con.register("inc_df", inc)
    con.execute("CREATE OR REPLACE TEMP TABLE inc AS SELECT * FROM inc_df")
    con.unregister("inc_df")
    cols = type1 + type2
    if type1:
        sets = ", ".join(f"{c} = inc.{c}" for c in type1)
        diff = " OR ".join(f"{dim}.{c} IS DISTINCT FROM inc.{c}" for c in type1)
        con.execute(f"UPDATE {dim} SET {sets} FROM inc WHERE {dim}.{nk} = inc.{nk} AND ({diff})")
    changed = 0
    insert_cols = ", ".join([sk, nk] + cols + (["row_effective_from", "row_effective_to", "is_current"] if scd2 else []))
    scd_vals = ", CAST(eff_from AS DATE), DATE '9999-12-31', 'Y'" if scd2 else ""
    if scd2 and type2:
        diff2 = " OR ".join(f"d.{c} IS DISTINCT FROM inc.{c}" for c in type2)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE chg AS SELECT inc.* FROM inc
                        JOIN {dim} d ON d.{nk} = inc.{nk} AND d.is_current = 'Y' WHERE {diff2}""")
        changed = con.execute("SELECT COUNT(*) FROM chg").fetchone()[0]
        if changed:
            con.execute(f"""UPDATE {dim} SET row_effective_to = CAST(chg.eff_from - INTERVAL 1 DAY AS DATE), is_current = 'N'
                            FROM chg WHERE {dim}.{nk} = chg.{nk} AND {dim}.is_current = 'Y'""")
            con.execute(f"""INSERT INTO {dim} ({insert_cols})
                            SELECT nextval('{seq}'), {nk}, {', '.join(cols)}{scd_vals} FROM chg""")
    new = con.execute(f"SELECT COUNT(*) FROM inc WHERE {nk} NOT IN (SELECT {nk} FROM {dim})").fetchone()[0]
    if new:
        con.execute(f"""INSERT INTO {dim} ({insert_cols})
                        SELECT nextval('{seq}'), {nk}, {', '.join(cols)}{scd_vals}
                        FROM inc WHERE {nk} NOT IN (SELECT {nk} FROM {dim}) ORDER BY {nk}""")
    return dict(new=new, changed=changed)


def is_empty(con, dim, key):
    return con.execute(f"SELECT COUNT(*) FROM {dim} WHERE {key} <> -1").fetchone()[0] == 0


def latest_per_key(df, key, order_col):
    return df.sort_values([order_col, "batch_id"]).groupby(key, as_index=False).tail(1)


# =============================================================== dimension builders
def build_dim_medicine(con, batches):
    cols = ["gtin14", "internal_sku", "generic_name", "therapeutic_class", "disease_category", "icd10_category",
            "dosage_form", "strength", "route_of_administration", "pack_size", "base_unit_of_measure",
            "prescription_class", "controlled_medicine_flag", "cold_chain_flag", "shelf_life_months",
            "unit_manufacturing_cost_usd", "wholesale_price_usd", "recommended_retail_price_usd", "effective_from_date"]
    m = read_staging("stg_master_data", batches, cols)
    if m.empty:
        return {}
    m = m.sort_values("batch_id").groupby("gtin14", as_index=False).tail(1)
    rrp = m.recommended_retail_price_usd.astype(float)
    inc = pd.DataFrame({
        "gtin14": m.gtin14, "internal_sku": m.internal_sku, "med_name": m.generic_name.str.strip(),
        "med_class": m.therapeutic_class.str.strip(), "disease_cat": m.disease_category.str.strip(),
        "icd10_code": m.icd10_category, "therapy_type": m.disease_category.map(ref.THERAPY_TYPE).fillna("Unknown"),
        "dosage_strength": m.dosage_form + " " + m.strength, "admin_route": m.route_of_administration,
        "pack_size": m.pack_size + " " + m.base_unit_of_measure, "presc_class": m.prescription_class,
        "legal_control": m.controlled_medicine_flag, "cold_chain": m.cold_chain_flag,
        "shelf_life": m.shelf_life_months.astype(int), "price_tier": rrp.map(ref.price_tier),
        "eff_from": pd.to_datetime(m.effective_from_date).dt.date})
    if is_empty(con, "dim_medicine", "medicine_key"):      # first versions must cover all history
        inc["eff_from"] = START_OF_HISTORY
    type2 = ["presc_class", "legal_control"]
    type1 = [c for c in inc.columns if c not in type2 + ["gtin14", "eff_from"]]
    res = merge_dimension(con, "dim_medicine", "medicine_key", "gtin14", inc, type1, type2, "seq_medicine")
    prices = pd.DataFrame({"gtin14": m.gtin14, "manuf_cost": m.unit_manufacturing_cost_usd.astype(float),
                           "wholesale_price": m.wholesale_price_usd.astype(float), "retail_price": rrp})
    con.register("p_df", prices)
    con.execute("INSERT OR REPLACE INTO ref_medicine_price SELECT * FROM p_df")
    con.unregister("p_df")
    return res


def build_dim_warehouse(con, batches):
    cols = ["warehouse_id", "warehouse_gln", "warehouse_name", "storage_condition", "warehouse_total_capacity_packs"]
    w = read_staging("stg_warehouse_data", batches, cols)
    if w.empty:
        return {}
    rows = []
    for wid, g in w.groupby("warehouse_id"):
        name = g.warehouse_name.iloc[-1]
        cap = int(float(g.warehouse_total_capacity_packs.iloc[-1]))
        rows.append(dict(warehouse_id=wid, warehouse_gln=g.warehouse_gln.iloc[-1], warehouse_name=name,
                         warehouse_short=ref.WAREHOUSE_SHORT.get(name, name[:6].upper()),
                         warehouse_region=ref.warehouse_region(name),
                         warehouse_type="Cold Chain Hub" if "Cold Chain" in name else "Distribution Center",
                         cold_chain_capable="Y" if (g.storage_condition == "2-8 C").any() else "N",
                         capacity_packs=cap, capacity_band=ref.band3(cap, *ref.CAPACITY_BANDS[:2], ref.CAPACITY_BANDS[2]),
                         eff_from=START_OF_HISTORY))
    inc = pd.DataFrame(rows)
    type1 = [c for c in inc.columns if c not in ("warehouse_id", "eff_from")]
    return merge_dimension(con, "dim_warehouse", "warehouse_key", "warehouse_id", inc, type1, [], "seq_warehouse", scd2=False)


def build_dim_pharmacy(con, sales):
    if sales.empty:
        return {}
    p = latest_per_key(sales, "pharmacy_id", "sales_date")
    # a new version starts on the first date its (latest) pharmacy_type is seen in this batch
    first = (sales.merge(p[["pharmacy_id", "pharmacy_type"]], on=["pharmacy_id", "pharmacy_type"])
             .groupby("pharmacy_id").sales_date.min())
    empty = is_empty(con, "dim_pharmacy", "pharmacy_key")
    inc = pd.DataFrame({
        "pharmacy_id": p.pharmacy_id, "pharmacy_gln": p.pharmacy_gln, "pharmacy_name": p.pharmacy_name,
        "pharmacy_short": p.pharmacy_name.map(ref.pharmacy_short), "pharmacy_type": p.pharmacy_type,
        "parent_hospital_id": p.hospital_id.replace("", "N/A"),
        "eff_from": [START_OF_HISTORY if empty else dt.date.fromisoformat(first[i]) for i in p.pharmacy_id]})
    return merge_dimension(con, "dim_pharmacy", "pharmacy_key", "pharmacy_id", inc,
                           ["pharmacy_gln", "pharmacy_name", "pharmacy_short", "parent_hospital_id"],
                           ["pharmacy_type"], "seq_pharmacy")


def build_dim_hospital(con, demand):
    if demand.empty:
        return {}
    h = latest_per_key(demand, "hospital_id", "demand_month")
    # a new version starts in the first month its (latest) hospital_type is seen in this batch
    first = (demand.merge(h[["hospital_id", "hospital_type"]], on=["hospital_id", "hospital_type"])
             .groupby("hospital_id").demand_month.min())
    empty = is_empty(con, "dim_hospital", "hospital_key")
    beds, icu, occ = h.licensed_beds.astype(int), h.icu_beds.astype(int), h.average_occupancy_rate_pct.astype(float)
    inc = pd.DataFrame({
        "hospital_id": h.hospital_id, "hospital_gln": h.hospital_gln, "hospital_name": h.hospital_name,
        "hospital_short": h.hospital_name.map(ref.hospital_short), "hospital_type": h.hospital_type,
        "licensed_beds": beds, "licensed_bed_size_band": beds.map(lambda v: ref.band3(v, *ref.BED_BANDS)),
        "icu_beds": icu, "icu_bed_size_band": icu.map(lambda v: ref.band3(v, *ref.ICU_BANDS)),
        "occupancy_rate": occ, "occupancy_tier": occ.map(lambda v: ref.band3(v, *ref.OCCUPANCY_TIERS)),
        "trauma_level": h.trauma_level.replace({"None": "Non-trauma", "": "Non-trauma"}),
        "catchment_pop": h.catchment_population.astype(int),
        "eff_from": [START_OF_HISTORY if empty else dt.date.fromisoformat(first[i]) for i in h.hospital_id]})
    type1 = [c for c in inc.columns if c not in ("hospital_id", "hospital_type", "eff_from")]
    return merge_dimension(con, "dim_hospital", "hospital_key", "hospital_id", inc, type1, ["hospital_type"], "seq_hospital")


def build_dim_location(con, places):
    """places: DataFrame with city, state, country (from sales + demand)."""
    if places.empty:
        return {}
    rows = []
    for (city, state, country) in places.drop_duplicates().itertuples(index=False):
        state_name, region, division = ref.STATES.get(state, ("Unknown", "Unknown", "Unknown"))
        pop, area, tz, lat, lon, climate = ref.CITIES.get((city, state), (None, None, "Unknown", None, None, "Unknown"))
        rows.append(dict(city_state=f"{city}|{state}", city=city, state=state, state_name=state_name, region=region,
                         census_division=division, country=country or "US", population_2020=pop, land_area_sqmi=area,
                         pop_density=round(pop / area) if pop and area else None, city_size_band=ref.city_size_band(pop),
                         time_zone=tz, latitude=lat, longitude=lon, climate_group=climate, eff_from=START_OF_HISTORY))
    inc = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TEMP VIEW loc_keyed AS SELECT *, city || '|' || state AS city_state FROM dim_location")
    new = inc[~inc.city_state.isin(con.execute("SELECT city_state FROM loc_keyed").df().city_state)]
    for r in new.itertuples(index=False):
        con.execute("""INSERT INTO dim_location VALUES (nextval('seq_location'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)""",
                    [r.city, r.state, r.state_name, r.region, r.census_division, r.country, r.population_2020,
                     r.land_area_sqmi, r.pop_density, r.city_size_band, r.time_zone, r.latitude, r.longitude,
                     r.climate_group])
    return dict(new=len(new), changed=0)


SALES_COLS = ["sales_transaction_id", "sales_date", "sales_channel", "pharmacy_id", "pharmacy_gln", "pharmacy_name",
              "pharmacy_type", "hospital_id", "pharmacy_city", "pharmacy_state", "country_code", "source_warehouse_id",
              "gtin14", "quantity_sold_packs", "gross_sales_amount_usd", "discount_amount_usd",
              "net_sales_amount_usd", "cost_of_goods_sold_usd", "gross_margin_usd", "returned_quantity_packs",
              "ecommerce_delivery_status", "prescription_id"]
DEMAND_COLS = ["demand_month", "hospital_id", "hospital_gln", "hospital_name", "hospital_type", "city", "state",
               "country_code", "licensed_beds", "icu_beds", "average_occupancy_rate_pct", "trauma_level",
               "catchment_population", "gtin14", "forecast_demand_packs", "requested_quantity_packs",
               "approved_quantity_packs", "received_quantity_packs", "emergency_request_quantity_packs",
               "opening_stock_packs", "dispensed_quantity_packs", "consumed_quantity_packs", "wastage_quantity_packs",
               "unfulfilled_demand_packs", "stockout_days", "estimated_disease_cases", "disease_related_admissions",
               "outpatient_encounters"]


def load_dimensions():
    """Airflow task: update all six dimensions from the validated staging batches."""
    master_b, wh_b = validated_batches("stg_master_data"), validated_batches("stg_warehouse_data")
    sales_b, dem_b = validated_batches("stg_pharmacy_sales"), validated_batches("stg_hospital_demand")
    sales = read_staging("stg_pharmacy_sales", sales_b, SALES_COLS)
    demand = read_staging("stg_hospital_demand", dem_b, DEMAND_COLS)
    out = {}
    with mart() as con:
        ensure_mart(con)
        con.execute("BEGIN TRANSACTION")     # all dimensions change together, or (on error) not at all
        out["dim_medicine"] = build_dim_medicine(con, master_b)
        out["dim_warehouse"] = build_dim_warehouse(con, wh_b)
        out["dim_pharmacy"] = build_dim_pharmacy(con, sales)
        out["dim_hospital"] = build_dim_hospital(con, demand)
        places = pd.concat([sales[["pharmacy_city", "pharmacy_state", "country_code"]].set_axis(["city", "state", "country"], axis=1),
                            demand[["city", "state", "country_code"]].set_axis(["city", "state", "country"], axis=1)])
        out["dim_location"] = build_dim_location(con, places)
        if not sales.empty:   # which warehouse serves each city (most frequent) – used for hospital rows
            cw = (sales.groupby(["pharmacy_city", "pharmacy_state"]).source_warehouse_id
                  .agg(lambda s: s.value_counts().index[0]).reset_index())
            cw.columns = ["city", "state", "warehouse_id"]
            con.register("cw_df", cw)
            con.execute("INSERT OR REPLACE INTO map_city_warehouse SELECT * FROM cw_df")
            con.unregister("cw_df")
        con.execute("COMMIT")
    mark_loaded(master_b + wh_b)          # these files feed dimensions only
    print("[dimensions]", out)
    return out


# =============================================================== facts
FACT_SALES_SQL = """
INSERT INTO fact_sales
SELECT CAST(strftime(s.sales_date, '%Y%m%d') AS INTEGER)      AS date_key,
       COALESCE(m.medicine_key, -1), COALESCE(p.pharmacy_key, -1), COALESCE(l.location_key, -1),
       COALESCE(w.warehouse_key, -1), s.sales_channel,
       CASE WHEN s.sales_date IN (SELECT d FROM bulk_days) THEN 'Y' ELSE 'N' END AS is_bulk_posted,
       SUM(s.qty), SUM(s.gross), SUM(s.discount), SUM(s.net), SUM(s.cogs), SUM(s.margin),
       ROUND(SUM(s.qty * COALESCE(pr.manuf_cost, 0)), 2)       AS manuf_cost_value,
       SUM(s.returned), SUM(CASE WHEN s.delivery_status = 'Returned to sender' THEN s.qty ELSE 0 END),
       COUNT(*)                                                 AS txn_count,
       COUNT(NULLIF(s.prescription_id, ''))                     AS rx_count,
       MAX(s.batch_id)                                          AS load_batch_id
FROM s
-- SCD Type 2: use the dimension version that was valid on the sales date
LEFT JOIN dim_medicine m ON m.gtin14 = s.gtin14
                        AND s.sales_date BETWEEN m.row_effective_from AND m.row_effective_to
LEFT JOIN dim_pharmacy p ON p.pharmacy_id = s.pharmacy_id
                        AND s.sales_date BETWEEN p.row_effective_from AND p.row_effective_to
LEFT JOIN dim_location l ON l.city = s.city AND l.state = s.state
LEFT JOIN dim_warehouse w ON w.warehouse_id = s.warehouse_id
LEFT JOIN ref_medicine_price pr ON pr.gtin14 = s.gtin14
GROUP BY ALL
ORDER BY date_key
ON CONFLICT DO UPDATE SET
    units_sold = units_sold + EXCLUDED.units_sold, gross_sales = gross_sales + EXCLUDED.gross_sales,
    discount_amt = discount_amt + EXCLUDED.discount_amt, net_sales = net_sales + EXCLUDED.net_sales,
    cogs = cogs + EXCLUDED.cogs, pharmacy_margin = pharmacy_margin + EXCLUDED.pharmacy_margin,
    manuf_cost_value = manuf_cost_value + EXCLUDED.manuf_cost_value,
    units_returned = units_returned + EXCLUDED.units_returned,
    undelivered_units = undelivered_units + EXCLUDED.undelivered_units,
    txn_count = txn_count + EXCLUDED.txn_count, rx_count = rx_count + EXCLUDED.rx_count,
    load_batch_id = EXCLUDED.load_batch_id
"""

FACT_DEMAND_SQL = """
INSERT INTO fact_demand
SELECT CAST(strftime(d.demand_month, '%Y%m%d') AS INTEGER) AS date_key,
       COALESCE(m.medicine_key, -1), COALESCE(h.hospital_key, -1), COALESCE(l.location_key, -1),
       COALESCE(w.warehouse_key, -1),
       d.forecast, d.requested, d.approved, d.received, d.emergency, d.opening, d.dispensed, d.consumed,
       d.wasted, d.unfulfilled, d.stockout_days, d.cases, d.admissions, d.opd, d.batch_id
FROM d
-- SCD Type 2: use the dimension version that was valid in the demand month
LEFT JOIN dim_medicine m ON m.gtin14 = d.gtin14
                        AND d.demand_month BETWEEN m.row_effective_from AND m.row_effective_to
LEFT JOIN dim_hospital h ON h.hospital_id = d.hospital_id
                        AND d.demand_month BETWEEN h.row_effective_from AND h.row_effective_to
LEFT JOIN dim_location l ON l.city = d.city AND l.state = d.state
LEFT JOIN map_city_warehouse cw ON cw.city = d.city AND cw.state = d.state
LEFT JOIN dim_warehouse w ON w.warehouse_id = cw.warehouse_id
ORDER BY date_key
ON CONFLICT DO UPDATE SET
    forecast_qty = EXCLUDED.forecast_qty, requested_qty = EXCLUDED.requested_qty, approved_qty = EXCLUDED.approved_qty,
    received_qty = EXCLUDED.received_qty, emergency_qty = EXCLUDED.emergency_qty, opening_qty = EXCLUDED.opening_qty,
    dispensed_qty = EXCLUDED.dispensed_qty, consumed_qty = EXCLUDED.consumed_qty, wasted_qty = EXCLUDED.wasted_qty,
    unfulfilled_qty = EXCLUDED.unfulfilled_qty, stockout_days = EXCLUDED.stockout_days,
    disease_cases = EXCLUDED.disease_cases, admissions = EXCLUDED.admissions, opd_visits = EXCLUDED.opd_visits,
    load_batch_id = EXCLUDED.load_batch_id
"""


def load_facts():
    """Airflow task: add the validated sales and demand batches to the two fact tables."""
    sales_b, dem_b = validated_batches("stg_pharmacy_sales"), validated_batches("stg_hospital_demand")
    sales = read_staging("stg_pharmacy_sales", sales_b, SALES_COLS)
    demand = read_staging("stg_hospital_demand", dem_b, DEMAND_COLS)
    out = {}
    with mart() as con:
        ensure_mart(con)
        con.execute("BEGIN TRANSACTION")     # both facts load together, or (on error) not at all -> safe to retry
        if not sales.empty:
            con.register("sales_df", sales)
            con.execute("""CREATE OR REPLACE TEMP TABLE s AS SELECT
                CAST(sales_date AS DATE) AS sales_date, sales_channel, pharmacy_id, pharmacy_city AS city,
                pharmacy_state AS state, source_warehouse_id AS warehouse_id, gtin14,
                CAST(quantity_sold_packs AS INTEGER) AS qty, CAST(gross_sales_amount_usd AS DECIMAL(14,2)) AS gross,
                CAST(discount_amount_usd AS DECIMAL(14,2)) AS discount, CAST(net_sales_amount_usd AS DECIMAL(14,2)) AS net,
                CAST(cost_of_goods_sold_usd AS DECIMAL(14,2)) AS cogs, CAST(gross_margin_usd AS DECIMAL(14,2)) AS margin,
                CAST(returned_quantity_packs AS INTEGER) AS returned, ecommerce_delivery_status AS delivery_status,
                prescription_id, CAST(batch_id AS INTEGER) AS batch_id FROM sales_df""")
            con.unregister("sales_df")
            # bulk-posting rule: a day with more than 10x the median daily number of sales
            con.execute(f"""CREATE OR REPLACE TEMP TABLE bulk_days AS
                WITH daily AS (SELECT sales_date AS d, COUNT(*) AS n FROM s GROUP BY 1),
                     hist AS (SELECT date_key, SUM(txn_count) AS n FROM fact_sales GROUP BY 1),
                     med AS (SELECT median(n) AS m FROM (SELECT n FROM daily UNION ALL SELECT n FROM hist))
                SELECT d FROM daily, med WHERE n > {config.BULK_POSTING_FACTOR} * m""")
            before = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
            con.execute(FACT_SALES_SQL)
            after = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
            out["fact_sales"] = dict(source_rows=len(sales), new_fact_rows=after - before,
                                     bulk_days=[str(r[0]) for r in con.execute("SELECT d FROM bulk_days").fetchall()])
        if not demand.empty:
            con.register("demand_df", demand)
            con.execute("""CREATE OR REPLACE TEMP TABLE d AS SELECT
                CAST(demand_month AS DATE) AS demand_month, hospital_id, city, state, gtin14,
                CAST(forecast_demand_packs AS INTEGER) AS forecast, CAST(requested_quantity_packs AS INTEGER) AS requested,
                CAST(approved_quantity_packs AS INTEGER) AS approved, CAST(received_quantity_packs AS INTEGER) AS received,
                CAST(emergency_request_quantity_packs AS INTEGER) AS emergency, CAST(opening_stock_packs AS INTEGER) AS opening,
                CAST(dispensed_quantity_packs AS INTEGER) AS dispensed, CAST(consumed_quantity_packs AS INTEGER) AS consumed,
                CAST(wastage_quantity_packs AS INTEGER) AS wasted, CAST(unfulfilled_demand_packs AS INTEGER) AS unfulfilled,
                CAST(stockout_days AS SMALLINT) AS stockout_days, CAST(estimated_disease_cases AS INTEGER) AS cases,
                CAST(disease_related_admissions AS INTEGER) AS admissions, CAST(outpatient_encounters AS INTEGER) AS opd,
                CAST(batch_id AS INTEGER) AS batch_id FROM demand_df""")
            con.unregister("demand_df")
            before = con.execute("SELECT COUNT(*) FROM fact_demand").fetchone()[0]
            con.execute(FACT_DEMAND_SQL)
            out["fact_demand"] = dict(source_rows=len(demand),
                                      new_fact_rows=con.execute("SELECT COUNT(*) FROM fact_demand").fetchone()[0] - before)
        # refresh the derived counts in DimLocation (Type 1)
        con.execute("""UPDATE dim_location SET
            pharmacy_count = (SELECT COUNT(DISTINCT p.pharmacy_id) FROM fact_sales f JOIN dim_pharmacy p USING (pharmacy_key)
                              WHERE f.location_key = dim_location.location_key),
            hospital_count = (SELECT COUNT(DISTINCT h.hospital_id) FROM fact_demand f JOIN dim_hospital h USING (hospital_key)
                              WHERE f.location_key = dim_location.location_key)
            WHERE location_key <> -1""")
        con.execute("COMMIT")
    mark_loaded(sales_b + dem_b)
    print("[facts]", out)
    return out
