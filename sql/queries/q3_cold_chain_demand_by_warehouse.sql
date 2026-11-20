-- name: Q3 Cold-chain demand by warehouse (drill-across both facts)
-- widget: Stacked column – cold-chain demand units by warehouse, coloured by 2-8 C capability
-- question: How much cold-chain demand is served by each warehouse, does it have 2-8 C storage, and when does demand peak?
-- @raw
WITH sales AS (
    SELECT w.warehouse_short, w.cold_chain_capable, d.quarter, SUM(f.units_sold) AS units
    FROM fact_sales f
    JOIN dim_medicine m USING (medicine_key) JOIN dim_warehouse w USING (warehouse_key) JOIN dim_date d USING (date_key)
    WHERE m.cold_chain = 'Y' AND f.is_bulk_posted = 'N'
    GROUP BY ALL
), demand AS (
    SELECT w.warehouse_short, w.cold_chain_capable, d.quarter, SUM(f.consumed_qty) AS units
    FROM fact_demand f
    JOIN dim_medicine m USING (medicine_key) JOIN dim_warehouse w USING (warehouse_key) JOIN dim_date d USING (date_key)
    WHERE m.cold_chain = 'Y'
    GROUP BY ALL
)
SELECT COALESCE(s.warehouse_short, x.warehouse_short) AS warehouse_short,
       COALESCE(s.cold_chain_capable, x.cold_chain_capable) AS cold_chain_capable,
       COALESCE(s.quarter, x.quarter) AS quarter,
       COALESCE(s.units, 0) AS pharmacy_units, COALESCE(x.units, 0) AS hospital_units,
       COALESCE(s.units, 0) + COALESCE(x.units, 0) AS total_demand_units
FROM sales s FULL OUTER JOIN demand x USING (warehouse_short, cold_chain_capable, quarter)
ORDER BY warehouse_short, quarter;
-- @summary
SELECT warehouse_short, cold_chain_capable, quarter,
       SUM(CASE WHEN source = 'Pharmacy' THEN demand_units ELSE 0 END) AS pharmacy_units,
       SUM(CASE WHEN source = 'Hospital' THEN demand_units ELSE 0 END) AS hospital_units,
       SUM(demand_units) AS total_demand_units
FROM agg_coldchain_wh_quarter
GROUP BY ALL
ORDER BY warehouse_short, quarter;
