# Pharma Cold-Chain – Sales & Demand Intelligence (SDI) Data Mart

### Group Members - IBA MS-DS KARACHI
Syeda Fizza Rizvi - ERP 24745 || Manahil Imran - ERP 36400


Milestone 1 of the DAWH & BI project: an Airflow-orchestrated pipeline that takes the pharma cold-chain
CSV files all the way to an optimised **analytical query layer**, with 4 monitoring dashboards in Grafana.

```
CSV files ──► data/landing ──► PostgreSQL staging ──► validation ──► DuckDB star schema ──► summary tables ──► queries
   ▲  (file watcher + REST API)    (batch + stream)     (DQ rules)    (SCD 1/2, 2 facts)      (optimisation)      │
   │                                                                                                            ▼
 generator (oversampling) ◄─── Airflow DAGs ───► monitoring logs (PostgreSQL) ───► Grafana: batch · stream · storage · query
```

## Tools
| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.10.4 (Docker, LocalExecutor) |
| Staging + monitoring | PostgreSQL 16 (database `pharma`, schemas `staging` and `monitoring`) |
| Data mart | DuckDB 1.1.3 (file `mart/sdi_mart.duckdb`) |
| Dashboards | Grafana 11.3 (provisioned automatically) |
| Ingestion API | FastAPI + uvicorn (http://localhost:8000/docs) |
| Language | Python 3.11 (pandas, numpy, psycopg2, duckdb) |

## Quick start
1. Put the 4 original files in `data/source/` with exactly these names:
   `01_master_data.csv`, `04_warehouse_data.csv`, `05_pharmacy_sales_data.csv`, `06_hospital_demand_data.csv`
2. `docker compose up -d --build` (first time ≈ 5–10 min)
3. Airflow → http://localhost:8080 (airflow / airflow) · Grafana → http://localhost:3000 (admin / admin) · API → http://localhost:8000/docs
4. Copy the 4 files from `data/source/` to `data/landing/` and un-pause the DAG **sdi_pipeline** → initial load
5. Refresh: trigger **sdi_generate_data** with `{"refresh": 1}` (then 2) → the pipeline picks the new files up

## Folder map
| Path | What it is |
|---|---|
| `dags/sdi_pipeline.py` | main DAG, every 5 min: file sensor → ingest → validate → dims → facts → summaries → storage → queries |
| `dags/sdi_generate_data.py` | generator DAG (manual, parameter `refresh`) |
| `scripts/sdi/config.py` | all settings (paths, DB connection, stream speed, bulk-posting rule) |
| `scripts/sdi/ingest.py` | batch + stream ingestion into PostgreSQL staging, ingestion logs |
| `scripts/sdi/validate.py` | data-quality rules, valid / rejected flag per row |
| `scripts/sdi/build_mart.py` | 6 dimensions (SCD Type 1 + 2) and 2 fact tables (incremental) in DuckDB |
| `scripts/sdi/reference.py` | calendar, US states/cities enrichment, bands and short names |
| `scripts/sdi/query_layer.py` | summary tables + timed query runs (raw vs summary) |
| `scripts/sdi/storage_metrics.py` | rows / MB / date range per table and layer |
| `scripts/sdi/generate_data.py` | data oversampling generator |
| `api/main.py` | own ingestion API: upload CSV, send JSON rows, start generator/pipeline, status |
| `scripts/sdi/api_trigger.py` | REST-API client (trigger refresh / pipeline, show status) |
| `scripts/sdi/mart_sql.py` | run SQL on the mart; `check` = health check |
| `sql/postgres/` | staging + monitoring tables |
| `sql/duckdb/` | star-schema DDL and summary tables |
| `sql/queries/` | the 6 dashboard queries, each with a `-- @raw` and a `-- @summary` version |
| `grafana/` | data source, dashboard provider and the 4 dashboard JSON files |

## Useful commands
```bash
docker compose ps                                                        # are all containers up?
docker compose logs -f airflow-scheduler                                 # live scheduler log
docker compose exec airflow-scheduler python -m sdi.mart_sql check       # mart health check
docker compose exec airflow-scheduler python -m sdi.mart_sql "SELECT * FROM agg_sales_month LIMIT 5"
docker compose exec airflow-scheduler python -m sdi.api_trigger refresh 2
docker compose exec airflow-scheduler python -m sdi.api_trigger status
docker compose down          # stop (data is kept)
docker compose down -v       # stop AND delete the PostgreSQL + Grafana data (full reset; also delete mart/*.duckdb)
```

## Star schema (fact constellation)
* **fact_sales** – grain: day × medicine × pharmacy × sales channel (location and warehouse as extra keys)
* **fact_demand** – grain: month × medicine × hospital
* Conformed dimensions: dim_date, dim_medicine, dim_location, dim_warehouse · own dimensions: dim_pharmacy, dim_hospital
* SCD Type 2: dim_medicine (presc_class, legal_control), dim_pharmacy (pharmacy_type), dim_hospital (hospital_type)
* Data-quality flag: `is_bulk_posted = 'Y'` on days with > 10× the median daily sales count (2024-01-01, 2025-01-01)
