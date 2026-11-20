-- name: K1 KPI cards
-- widget: KPI cards – Net Sales, Gross Margin %, Forecast Accuracy %, Fill Rate %, Total Demand Units
-- question: What are the headline numbers for the selected period?
-- @raw
SELECT
    (SELECT ROUND(SUM(net_sales), 0) FROM fact_sales WHERE is_bulk_posted = 'N')                        AS net_sales,
    (SELECT ROUND(100 * SUM(pharmacy_margin) / SUM(net_sales), 1) FROM fact_sales WHERE is_bulk_posted = 'N') AS gross_margin_pct,
    (SELECT ROUND(100 * (1 - SUM(ABS(forecast_qty - consumed_qty)) / SUM(consumed_qty)), 1) FROM fact_demand) AS forecast_accuracy_pct,
    (SELECT ROUND(100 * (1 - SUM(unfulfilled_qty) / SUM(requested_qty)), 2) FROM fact_demand)           AS fill_rate_pct,
    (SELECT SUM(units_sold) FROM fact_sales WHERE is_bulk_posted = 'N')
      + (SELECT SUM(consumed_qty) FROM fact_demand)                                                     AS total_demand_units;
-- @summary
SELECT
    (SELECT ROUND(SUM(net_sales), 0) FROM agg_sales_month WHERE is_bulk_posted = 'N')                   AS net_sales,
    (SELECT ROUND(100 * SUM(pharmacy_margin) / SUM(net_sales), 1) FROM agg_sales_month WHERE is_bulk_posted = 'N') AS gross_margin_pct,
    (SELECT ROUND(100 * (1 - SUM(abs_error) / SUM(consumed_qty)), 1) FROM agg_demand_month)             AS forecast_accuracy_pct,
    (SELECT ROUND(100 * (1 - SUM(unfulfilled_qty) / SUM(requested_qty)), 2) FROM agg_demand_month)      AS fill_rate_pct,
    (SELECT SUM(units_sold) FROM agg_sales_month WHERE is_bulk_posted = 'N')
      + (SELECT SUM(consumed_qty) FROM agg_demand_month)                                                AS total_demand_units;
