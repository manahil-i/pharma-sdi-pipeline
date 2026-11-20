-- =====================================================================
-- Sales & Demand Intelligence (SDI) data mart – DuckDB
-- Two fact tables sharing conformed dimensions (fact constellation).
-- Column names follow the Excel design, written in snake_case for SQL:
--   Med.Name -> med_name, Is.Bulk.Posted -> is_bulk_posted, etc.
-- Safe to run many times (IF NOT EXISTS).
-- =====================================================================

CREATE SEQUENCE IF NOT EXISTS seq_medicine START 1;
CREATE SEQUENCE IF NOT EXISTS seq_pharmacy START 1;
CREATE SEQUENCE IF NOT EXISTS seq_hospital START 1;
CREATE SEQUENCE IF NOT EXISTS seq_location START 1;
CREATE SEQUENCE IF NOT EXISTS seq_warehouse START 1;

-- ---------------------------------------------------------------- DimDate (daily, smart key YYYYMMDD)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, date DATE, day_of_month TINYINT, day_name VARCHAR(10), day_short VARCHAR(3),
    day_of_week TINYINT, is_weekend VARCHAR(1), is_working_day VARCHAR(1), is_holiday VARCHAR(1),
    holiday_name VARCHAR(40), iso_week TINYINT, week_start DATE, month_start DATE, year_month VARCHAR(7),
    month_name VARCHAR(10), month_short VARCHAR(3), month_no TINYINT, month_seq SMALLINT, quarter VARCHAR(2),
    quarter_label VARCHAR(7), half VARCHAR(2), year SMALLINT, is_month_end VARCHAR(1),
    is_christmas_month VARCHAR(1), is_thanksgiving_month VARCHAR(1), is_holiday_season VARCHAR(1),
    season VARCHAR(10), flu_season_flag VARCHAR(1), allergy_season_flag VARCHAR(1)
);

-- ---------------------------------------------------------------- DimMedicine (SCD Type 2 on presc_class, legal_control)
CREATE TABLE IF NOT EXISTS dim_medicine (
    medicine_key INTEGER PRIMARY KEY, gtin14 VARCHAR(14), internal_sku VARCHAR(15), med_name VARCHAR(60),
    med_class VARCHAR(60), disease_cat VARCHAR(60), icd10_code VARCHAR(10), therapy_type VARCHAR(10),
    dosage_strength VARCHAR(60), admin_route VARCHAR(20), pack_size VARCHAR(20), presc_class VARCHAR(15),
    legal_control VARCHAR(1), cold_chain VARCHAR(1), shelf_life TINYINT, price_tier VARCHAR(20),
    row_effective_from DATE, row_effective_to DATE, is_current VARCHAR(1)
);

-- ---------------------------------------------------------------- DimPharmacy (SCD Type 2 on pharmacy_type)
CREATE TABLE IF NOT EXISTS dim_pharmacy (
    pharmacy_key INTEGER PRIMARY KEY, pharmacy_id VARCHAR(10), pharmacy_gln VARCHAR(13), pharmacy_name VARCHAR(60),
    pharmacy_short VARCHAR(15), pharmacy_type VARCHAR(30), parent_hospital_id VARCHAR(10),
    row_effective_from DATE, row_effective_to DATE, is_current VARCHAR(1)
);

-- ---------------------------------------------------------------- DimHospital (SCD Type 2 on hospital_type)
CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_key INTEGER PRIMARY KEY, hospital_id VARCHAR(10), hospital_gln VARCHAR(13), hospital_name VARCHAR(60),
    hospital_short VARCHAR(6), hospital_type VARCHAR(30), licensed_beds SMALLINT, licensed_bed_size_band VARCHAR(20),
    icu_beds SMALLINT, icu_bed_size_band VARCHAR(20), occupancy_rate DECIMAL(5,2), occupancy_tier VARCHAR(25),
    trauma_level VARCHAR(15), catchment_pop INTEGER,
    row_effective_from DATE, row_effective_to DATE, is_current VARCHAR(1)
);

-- ---------------------------------------------------------------- DimLocation (Type 1, enriched)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INTEGER PRIMARY KEY, city VARCHAR(30), state VARCHAR(2), state_name VARCHAR(20), region VARCHAR(15),
    census_division VARCHAR(25), country VARCHAR(2), population_2020 INTEGER, land_area_sqmi DECIMAL(6,1),
    pop_density INTEGER, city_size_band VARCHAR(20), time_zone VARCHAR(10), latitude DECIMAL(8,5),
    longitude DECIMAL(8,5), climate_group VARCHAR(15), pharmacy_count SMALLINT, hospital_count SMALLINT
);

-- ---------------------------------------------------------------- DimWarehouse (Type 1)
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_key INTEGER PRIMARY KEY, warehouse_id VARCHAR(10), warehouse_gln VARCHAR(13),
    warehouse_name VARCHAR(40), warehouse_short VARCHAR(6), warehouse_region VARCHAR(15),
    warehouse_type VARCHAR(25), cold_chain_capable VARCHAR(1), capacity_packs INTEGER, capacity_band VARCHAR(25)
);

-- ---------------------------------------------------------------- helper tables (not shown in BI)
-- unit prices from master data (used to compute Manuf.Cost.Value)
CREATE TABLE IF NOT EXISTS ref_medicine_price (
    gtin14 VARCHAR(14) PRIMARY KEY, manuf_cost DECIMAL(10,2), wholesale_price DECIMAL(10,2), retail_price DECIMAL(10,2)
);
-- which warehouse serves which city (built from sales; used for hospital demand rows)
CREATE TABLE IF NOT EXISTS map_city_warehouse (
    city VARCHAR(30), state VARCHAR(2), warehouse_id VARCHAR(10), PRIMARY KEY (city, state)
);

-- ---------------------------------------------------------------- FactSales (daily grain)
CREATE TABLE IF NOT EXISTS fact_sales (
    date_key INTEGER, medicine_key INTEGER, pharmacy_key INTEGER, location_key INTEGER, warehouse_key INTEGER,
    sales_channel VARCHAR(20),                       -- degenerate dimension
    is_bulk_posted VARCHAR(1),                       -- degenerate data-quality flag
    units_sold INTEGER, gross_sales DECIMAL(14,2), discount_amt DECIMAL(14,2), net_sales DECIMAL(14,2),
    cogs DECIMAL(14,2), pharmacy_margin DECIMAL(14,2), manuf_cost_value DECIMAL(14,2),
    units_returned INTEGER, undelivered_units INTEGER, txn_count INTEGER, rx_count INTEGER,
    load_batch_id INTEGER,                           -- lineage: which pipeline batch loaded it
    PRIMARY KEY (date_key, medicine_key, pharmacy_key, location_key, warehouse_key, sales_channel)
);

-- ---------------------------------------------------------------- FactDemand (monthly snapshot on 1st of month)
CREATE TABLE IF NOT EXISTS fact_demand (
    date_key INTEGER, medicine_key INTEGER, hospital_key INTEGER, location_key INTEGER, warehouse_key INTEGER,
    forecast_qty INTEGER, requested_qty INTEGER, approved_qty INTEGER, received_qty INTEGER, emergency_qty INTEGER,
    opening_qty INTEGER, dispensed_qty INTEGER, consumed_qty INTEGER, wasted_qty INTEGER, unfulfilled_qty INTEGER,
    stockout_days SMALLINT, disease_cases INTEGER, admissions INTEGER, opd_visits INTEGER,
    load_batch_id INTEGER,
    PRIMARY KEY (date_key, medicine_key, hospital_key, location_key, warehouse_key)
);
