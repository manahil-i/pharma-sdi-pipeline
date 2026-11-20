-- name: Q5 Seasonality – December, flu season, holidays, weekends
-- widget: Column chart – average daily net sales by calendar flag
-- question: How strongly do December, flu season and holidays lift pharmacy sales?
-- @raw
WITH daily AS (
    SELECT d.date, d.is_christmas_month, d.flu_season_flag, d.is_holiday, d.is_weekend, SUM(f.net_sales) AS net_sales
    FROM fact_sales f JOIN dim_date d USING (date_key)
    WHERE f.is_bulk_posted = 'N' AND d.date >= DATE '2023-06-01'      -- skip the 2023 ramp-up months
    GROUP BY ALL
)
SELECT 'December' AS flag, is_christmas_month AS value, ROUND(AVG(net_sales), 0) AS avg_daily_net_sales FROM daily GROUP BY ALL
UNION ALL SELECT 'Flu season', flu_season_flag, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
UNION ALL SELECT 'Holiday', is_holiday, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
UNION ALL SELECT 'Weekend', is_weekend, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
ORDER BY flag, value;
-- @summary
WITH daily AS (SELECT * FROM agg_sales_day WHERE date >= DATE '2023-06-01')
SELECT 'December' AS flag, is_christmas_month AS value, ROUND(AVG(net_sales), 0) AS avg_daily_net_sales FROM daily GROUP BY ALL
UNION ALL SELECT 'Flu season', flu_season_flag, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
UNION ALL SELECT 'Holiday', is_holiday, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
UNION ALL SELECT 'Weekend', is_weekend, ROUND(AVG(net_sales), 0) FROM daily GROUP BY ALL
ORDER BY flag, value;
