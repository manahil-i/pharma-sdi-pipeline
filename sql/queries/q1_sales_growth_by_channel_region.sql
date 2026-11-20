-- name: Q1 Sales growth by channel and region
-- widget: Sales table – Net Sales, growth vs the same months last year and Gross Margin % by year, channel and region
-- question: Which sales channel and region is growing or declining most, and did Gross Margin % hold?
-- note: growth compares the SAME months only (e.g. Jan-Jun 2026 vs Jan-Jun 2025), so a year that is
--       not finished yet is never compared with a full year.
-- @raw
WITH monthly AS (
    SELECT d.year, d.month_no, f.sales_channel, l.region,
           SUM(f.net_sales) AS net_sales, SUM(f.pharmacy_margin) AS margin
    FROM fact_sales f
    JOIN dim_date d USING (date_key)
    JOIN dim_location l USING (location_key)
    WHERE f.is_bulk_posted = 'N'
    GROUP BY ALL
), yearly AS (
    SELECT year, sales_channel, region, SUM(net_sales) AS net_sales, SUM(margin) AS margin,
           MAX(month_no) AS last_month
    FROM monthly GROUP BY ALL
)
SELECT y.year, y.sales_channel, y.region, y.last_month AS months_of_data,
       ROUND(y.net_sales, 0)                                        AS net_sales,
       ROUND(100 * y.margin / y.net_sales, 1)                       AS gross_margin_pct,
       ROUND(100 * (y.net_sales / NULLIF(SUM(p.net_sales), 0) - 1), 1) AS growth_vs_last_year_pct
FROM yearly y
LEFT JOIN monthly p ON p.year = y.year - 1 AND p.month_no <= y.last_month
                   AND p.sales_channel = y.sales_channel AND p.region = y.region
GROUP BY y.year, y.sales_channel, y.region, y.last_month, y.net_sales, y.margin
ORDER BY y.sales_channel, y.region, y.year;
-- @summary
WITH monthly AS (
    SELECT year, CAST(right(year_month, 2) AS INTEGER) AS month_no, sales_channel, region,
           SUM(net_sales) AS net_sales, SUM(pharmacy_margin) AS margin
    FROM agg_sales_month WHERE is_bulk_posted = 'N' GROUP BY ALL
), yearly AS (
    SELECT year, sales_channel, region, SUM(net_sales) AS net_sales, SUM(margin) AS margin,
           MAX(month_no) AS last_month
    FROM monthly GROUP BY ALL
)
SELECT y.year, y.sales_channel, y.region, y.last_month AS months_of_data,
       ROUND(y.net_sales, 0)                                        AS net_sales,
       ROUND(100 * y.margin / y.net_sales, 1)                       AS gross_margin_pct,
       ROUND(100 * (y.net_sales / NULLIF(SUM(p.net_sales), 0) - 1), 1) AS growth_vs_last_year_pct
FROM yearly y
LEFT JOIN monthly p ON p.year = y.year - 1 AND p.month_no <= y.last_month
                   AND p.sales_channel = y.sales_channel AND p.region = y.region
GROUP BY y.year, y.sales_channel, y.region, y.last_month, y.net_sales, y.margin
ORDER BY y.sales_channel, y.region, y.year;
