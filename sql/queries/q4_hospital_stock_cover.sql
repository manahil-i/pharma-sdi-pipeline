-- name: Q4 Hospital stock cover vs stockouts and waste
-- widget: Column chart – days of stock on hand vs stockouts, unmet demand and wastage
-- question: Do hospitals hold the right amount of stock – does low stock cause lost demand and high stock cause waste?
-- @raw
WITH x AS (
    SELECT f.*, f.opening_qty / NULLIF(f.consumed_qty / day(last_day(d.date)), 0) AS days_of_supply
    FROM fact_demand f JOIN dim_date d USING (date_key) JOIN dim_hospital h USING (hospital_key)
)
SELECT CASE WHEN days_of_supply IS NULL THEN '5-No demand'
            WHEN days_of_supply < 7  THEN '1-Under 7 days'
            WHEN days_of_supply < 14 THEN '2-7 to 14 days'
            WHEN days_of_supply < 30 THEN '3-14 to 30 days'
            ELSE '4-30+ days' END                                           AS cover_band,
       COUNT(*)                                                             AS hospital_medicine_months,
       ROUND(AVG(stockout_days), 3)                                         AS avg_stockout_days,
       ROUND(100 * SUM(unfulfilled_qty) / SUM(requested_qty), 2)            AS unmet_demand_pct,
       ROUND(100 * SUM(wasted_qty) / NULLIF(SUM(consumed_qty), 0), 2)       AS wastage_pct
FROM x
GROUP BY ALL
ORDER BY cover_band;
-- @summary
SELECT cover_band, SUM(n_rows) AS hospital_medicine_months,
       ROUND(SUM(stockout_days) / SUM(n_rows), 3)                           AS avg_stockout_days,
       ROUND(100 * SUM(unfulfilled_qty) / SUM(requested_qty), 2)            AS unmet_demand_pct,
       ROUND(100 * SUM(wasted_qty) / NULLIF(SUM(consumed_qty), 0), 2)       AS wastage_pct
FROM agg_stock_cover
GROUP BY ALL
ORDER BY cover_band;
