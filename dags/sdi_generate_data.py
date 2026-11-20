"""DAG 2 – sdi_generate_data: the data oversampling generator (run it by hand).

Trigger it with a config such as {"refresh": 1}. It writes new CSV files into data/landing;
the sdi_pipeline DAG notices them within 5 minutes and loads them (= one dashboard refresh).
"""
import datetime as dt

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from sdi import generate_data


def _generate(**context):
    return generate_data.generate(refresh=context["params"]["refresh"])


with DAG(
    dag_id="sdi_generate_data",
    description="Create 3 new months of pharmacy sales (stream file) and hospital demand (batch file)",
    start_date=dt.datetime(2026, 1, 1),
    schedule=None,                 # manual only
    catchup=False,
    params={"refresh": Param(1, type="integer", minimum=1, description="1 = Jan-Mar 2026, 2 = Apr-Jun 2026, ...")},
    tags=["sdi", "generator"],
) as dag:
    PythonOperator(task_id="generate_refresh_files", python_callable=_generate)
