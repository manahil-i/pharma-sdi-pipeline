-- =====================================================================
-- PostgreSQL setup for the SDI pipeline (safe to run many times)
--   schema staging    : raw data landed from the CSV files (ODS / staging area)
--   schema monitoring : logs that feed the 4 Grafana dashboards
-- Staging tables themselves are created automatically from each CSV header
-- by scripts/sdi/ingest.py (all columns TEXT = raw landing zone).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- One row per file that was ingested (control table for the whole pipeline)
CREATE TABLE IF NOT EXISTS monitoring.etl_batches (
    batch_id        SERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    staging_table   TEXT NOT NULL,
    mode            TEXT NOT NULL,                 -- 'batch' or 'stream'
    status          TEXT NOT NULL,                 -- ingested -> validated -> loaded (or failed)
    rows_loaded     INTEGER,
    ingested_at     TIMESTAMP DEFAULT now(),
    validated_at    TIMESTAMP,
    loaded_at       TIMESTAMP
);

-- Batch ingestion dashboard
CREATE TABLE IF NOT EXISTS monitoring.batch_ingestion_log (
    log_id          SERIAL PRIMARY KEY,
    batch_id        INTEGER,
    file_name       TEXT,
    staging_table   TEXT,
    started_at      TIMESTAMP,
    ended_at        TIMESTAMP,
    duration_sec    NUMERIC(10,3),
    rows_read       INTEGER,
    rows_loaded     INTEGER,
    status          TEXT,                          -- 'success' or 'failed'
    error_message   TEXT
);

-- Streaming ingestion dashboard (one row per micro-batch)
CREATE TABLE IF NOT EXISTS monitoring.stream_ingestion_log (
    log_id          SERIAL PRIMARY KEY,
    batch_id        INTEGER,
    file_name       TEXT,
    micro_batch_no  INTEGER,
    events_received INTEGER,
    events_loaded   INTEGER,
    malformed_count INTEGER,
    emitted_at      TIMESTAMP,                     -- when the micro-batch was produced
    inserted_at     TIMESTAMP,                     -- when it was committed to the database
    latency_sec     NUMERIC(10,3),
    throughput_eps  NUMERIC(12,2)
);

-- Rows thrown away during stream parsing (schema problems)
CREATE TABLE IF NOT EXISTS monitoring.stream_rejects (
    reject_id       SERIAL PRIMARY KEY,
    batch_id        INTEGER,
    micro_batch_no  INTEGER,
    reason          TEXT,
    raw_record      TEXT,
    rejected_at     TIMESTAMP DEFAULT now()
);

-- Data-quality (validation) results per rule
CREATE TABLE IF NOT EXISTS monitoring.dq_log (
    log_id          SERIAL PRIMARY KEY,
    batch_id        INTEGER,
    staging_table   TEXT,
    rule_name       TEXT,
    failed_rows     INTEGER,
    checked_at      TIMESTAMP DEFAULT now()
);

-- Storage dashboard (one row per table per pipeline run)
CREATE TABLE IF NOT EXISTS monitoring.storage_log (
    log_id          SERIAL PRIMARY KEY,
    captured_at     TIMESTAMP DEFAULT now(),
    layer           TEXT,                          -- 'staging' or 'mart'
    table_name      TEXT,
    source_domain   TEXT,
    row_count       BIGINT,
    size_mb         NUMERIC(12,3),
    min_date        DATE,
    max_date        DATE
);

-- Query dashboard (one row per executed analytical query)
CREATE TABLE IF NOT EXISTS monitoring.query_log (
    log_id          SERIAL PRIMARY KEY,
    run_id          TEXT,
    query_name      TEXT,
    widget          TEXT,
    variant         TEXT,                          -- 'raw' (star join on facts) or 'summary' (pre-aggregated table)
    started_at      TIMESTAMP,
    duration_ms     NUMERIC(12,3),
    rows_returned   INTEGER,
    status          TEXT,                          -- 'success' or 'failed'
    error_message   TEXT
);

-- Helper functions used by validation (return FALSE instead of crashing on bad values)
CREATE OR REPLACE FUNCTION staging.is_date(t TEXT) RETURNS BOOLEAN AS $$
BEGIN
    IF t IS NULL OR t !~ '^\d{4}-\d{2}-\d{2}$' THEN RETURN FALSE; END IF;
    PERFORM t::DATE;
    RETURN TRUE;
EXCEPTION WHEN others THEN
    RETURN FALSE;
END; $$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION staging.is_num(t TEXT) RETURNS BOOLEAN AS $$
    SELECT t IS NOT NULL AND t ~ '^-?[0-9]+(\.[0-9]+)?$';
$$ LANGUAGE sql IMMUTABLE;
