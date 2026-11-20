-- Runs once, the first time the postgres container starts with an empty volume.
-- "airflow" database = Airflow's own metadata. "pharma" database = our staging + monitoring.
CREATE DATABASE pharma OWNER airflow;
