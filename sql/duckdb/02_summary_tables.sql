-- =====================================================================
-- Query optimisation: pre-aggregated summary tables (materialised views)
-- Rebuilt after every load. Dashboards read these small tables instead of
-- joining the big fact tables every time -> much faster queries.
-- =====================================================================

-- 1) Monthly sales by region, channel and pharmacy type (Q1, KPI cards)
CREATE OR REPLACE TABLE agg_sales_month AS
SELECT d.year, d.year_month, d.month_start, l.region, f.sales_channel, p.pharmacy_type, f.is_bulk_posted,
       SUM(f.net_sales) AS net_sales, SUM(f.gross_sales) AS gross_sales, SUM(f.discount_amt) AS discount_amt,
       SUM(f.cogs) AS cogs, SUM(f.pharmacy_margin) AS pharmacy_margin, SUM(f.manuf_cost_value) AS manuf_cost_value,
       SUM(f.units_sold) AS units_sold, SUM(f.txn_count) AS txn_count
FROM fact_sales f
JOIN dim_date d USING (date_key)
JOIN dim_location l USING (location_key)
JOIN dim_pharmacy p USING (pharmacy_key)
GROUP BY ALL
ORDER BY d.year_month;

-- 2) Monthly hospital demand by disease area and flu season (Q2)
CREATE OR REPLACE TABLE agg_demand_month AS
SELECT d.year, d.year_month, d.flu_season_flag, m.disease_cat, m.cold_chain, h.hospital_type,
       SUM(f.forecast_qty) AS forecast_qty, SUM(f.consumed_qty) AS consumed_qty,
       SUM(ABS(f.forecast_qty - f.consumed_qty)) AS abs_error,
       SUM(f.requested_qty) AS requested_qty, SUM(f.received_qty) AS received_qty,
       SUM(f.unfulfilled_qty) AS unfulfilled_qty, SUM(f.emergency_qty) AS emergency_qty,
       SUM(f.wasted_qty) AS wasted_qty, SUM(f.stockout_days) AS stockout_days, COUNT(*) AS n_rows
FROM fact_demand f
JOIN dim_date d USING (date_key)
JOIN dim_medicine m USING (medicine_key)
JOIN dim_hospital h USING (hospital_key)
GROUP BY ALL
ORDER BY d.year_month;

-- 3) Stock cover (days of supply) band per hospital-medicine-month (Q4)
CREATE OR REPLACE TABLE agg_stock_cover AS
WITH x AS (
    SELECT f.*, d.year_month,
           f.opening_qty / NULLIF(f.consumed_qty / day(last_day(d.date)), 0) AS days_of_supply
    FROM fact_demand f JOIN dim_date d USING (date_key)
)
SELECT year_month,
       CASE WHEN days_of_supply IS NULL THEN '5-No demand'
            WHEN days_of_supply < 7  THEN '1-Under 7 days'
            WHEN days_of_supply < 14 THEN '2-7 to 14 days'
            WHEN days_of_supply < 30 THEN '3-14 to 30 days'
            ELSE '4-30+ days' END AS cover_band,
       COUNT(*) AS n_rows, SUM(stockout_days) AS stockout_days, SUM(unfulfilled_qty) AS unfulfilled_qty,
       SUM(requested_qty) AS requested_qty, SUM(wasted_qty) AS wasted_qty, SUM(consumed_qty) AS consumed_qty,
       median(days_of_supply) AS median_days_of_supply
FROM x
GROUP BY ALL;

-- 4) Cold-chain demand by warehouse and quarter – drill-across BOTH facts (Q3)
CREATE OR REPLACE TABLE agg_coldchain_wh_quarter AS
SELECT d.quarter_label, d.quarter, w.warehouse_short, w.cold_chain_capable, 'Pharmacy' AS source,
       SUM(f.units_sold) AS demand_units
FROM fact_sales f JOIN dim_date d USING (date_key) JOIN dim_medicine m USING (medicine_key)
JOIN dim_warehouse w USING (warehouse_key)
WHERE m.cold_chain = 'Y' AND f.is_bulk_posted = 'N'
GROUP BY ALL
UNION ALL
SELECT d.quarter_label, d.quarter, w.warehouse_short, w.cold_chain_capable, 'Hospital' AS source,
       SUM(f.consumed_qty) AS demand_units
FROM fact_demand f JOIN dim_date d USING (date_key) JOIN dim_medicine m USING (medicine_key)
JOIN dim_warehouse w USING (warehouse_key)
WHERE m.cold_chain = 'Y'
GROUP BY ALL;

-- 5) Daily net sales with calendar flags (Q5 seasonality)
CREATE OR REPLACE TABLE agg_sales_day AS
SELECT d.date_key, d.date, d.year, d.month_no, d.is_weekend, d.is_holiday, d.is_christmas_month,
       d.flu_season_flag, d.season, SUM(f.net_sales) AS net_sales
FROM fact_sales f JOIN dim_date d USING (date_key)
WHERE f.is_bulk_posted = 'N'
GROUP BY ALL
ORDER BY d.date_key;
