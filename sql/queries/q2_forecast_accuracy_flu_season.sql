-- name: Q2 Forecast accuracy by disease area and flu season
-- widget: Forecast table – how close the hospital forecast was to real use, by disease area and flu season
-- question: Which disease areas does the forecast miss most in flu season, and does it guess too high (+) or too low (-)?
-- @raw
SELECT m.disease_cat, d.flu_season_flag,
       ROUND(100 * (1 - SUM(ABS(f.forecast_qty - f.consumed_qty)) / SUM(f.consumed_qty)), 1) AS forecast_accuracy_pct,
       ROUND(100 * (SUM(f.forecast_qty) - SUM(f.consumed_qty)) / SUM(f.consumed_qty), 1)    AS over_under_forecast_pct
FROM fact_demand f
JOIN dim_medicine m USING (medicine_key)
JOIN dim_date d USING (date_key)
GROUP BY ALL
ORDER BY forecast_accuracy_pct, disease_cat, flu_season_flag;
-- @summary
SELECT disease_cat, flu_season_flag,
       ROUND(100 * (1 - SUM(abs_error) / SUM(consumed_qty)), 1)                     AS forecast_accuracy_pct,
       ROUND(100 * (SUM(forecast_qty) - SUM(consumed_qty)) / SUM(consumed_qty), 1)   AS over_under_forecast_pct
FROM agg_demand_month
GROUP BY ALL
ORDER BY forecast_accuracy_pct, disease_cat, flu_season_flag;
