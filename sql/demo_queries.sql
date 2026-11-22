-- 1. Which 5 medicines earned the most in 2025?
SELECT m.med_name, m.disease_cat, ROUND(SUM(f.net_sales)) AS net_sales
FROM fact_sales f
JOIN dim_medicine m USING (medicine_key)
JOIN dim_date d USING (date_key)
WHERE d.year = 2025 AND f.is_bulk_posted = 'N'
GROUP BY ALL ORDER BY net_sales DESC LIMIT 5;

-- 2. Net sales and Gross Margin % per region in 2025
SELECT l.region, ROUND(SUM(f.net_sales)) AS net_sales,
       ROUND(100 * SUM(f.pharmacy_margin) / SUM(f.net_sales), 1) AS gross_margin_pct
FROM fact_sales f
JOIN dim_location l USING (location_key)
JOIN dim_date d USING (date_key)
WHERE d.year = 2025 AND f.is_bulk_posted = 'N'
GROUP BY ALL ORDER BY net_sales DESC;

-- 3. Online vs physical shop: sales per quarter in 2025
SELECT d.quarter_label, f.sales_channel, ROUND(SUM(f.net_sales)) AS net_sales
FROM fact_sales f
JOIN dim_date d USING (date_key)
WHERE d.year = 2025 AND f.is_bulk_posted = 'N'
GROUP BY ALL ORDER BY d.quarter_label, f.sales_channel;

-- 4. Which hospitals ran out of stock most often?
SELECT h.hospital_name, h.hospital_type, SUM(f.stockout_days) AS stockout_days,
       ROUND(100 * (1 - SUM(f.unfulfilled_qty) / SUM(f.requested_qty)), 2) AS fill_rate_pct
FROM fact_demand f
JOIN dim_hospital h USING (hospital_key)
GROUP BY ALL ORDER BY stockout_days DESC LIMIT 5;

-- 5. Cold-chain medicines: pharmacy sales vs hospital use per warehouse (both facts)
SELECT w.warehouse_name, w.cold_chain_capable,
       COALESCE(SUM(CASE WHEN src = 'pharmacy' THEN units END), 0) AS pharmacy_units,
       COALESCE(SUM(CASE WHEN src = 'hospital' THEN units END), 0) AS hospital_units
FROM (SELECT warehouse_key, medicine_key, units_sold AS units, 'pharmacy' AS src FROM fact_sales WHERE is_bulk_posted = 'N'
      UNION ALL
      SELECT warehouse_key, medicine_key, consumed_qty, 'hospital' FROM fact_demand) x
JOIN dim_warehouse w USING (warehouse_key)
JOIN dim_medicine m USING (medicine_key)
WHERE m.cold_chain = 'Y'
GROUP BY ALL ORDER BY hospital_units DESC;

-- 6. History (SCD Type 2): the two versions of pharmacy PHR041 (appears after refresh 2)
SELECT pharmacy_key, pharmacy_id, pharmacy_type, row_effective_from, row_effective_to, is_current
FROM dim_pharmacy WHERE pharmacy_id = 'PHR041';
