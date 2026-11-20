"""Ingestion API client: start the pipeline through Airflow's REST API (no extra libraries needed).

Inside Docker:
  docker compose exec airflow-scheduler python -m sdi.api_trigger refresh 1   <- generate refresh 1 (Jan-Mar 2026)
  docker compose exec airflow-scheduler python -m sdi.api_trigger pipeline    <- run the pipeline now (don't wait 5 min)
  docker compose exec airflow-scheduler python -m sdi.api_trigger status      <- last runs of both DAGs
"""
import base64
import json
import os
import sys
import urllib.request

API = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AUTH = base64.b64encode(os.getenv("AIRFLOW_API_AUTH", "airflow:airflow").encode()).decode()


def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or "{}")


def unpause(dag_id):
    call("PATCH", f"/dags/{dag_id}?update_mask=is_paused", {"is_paused": False})


def trigger(dag_id, conf=None):
    unpause(dag_id)
    run = call("POST", f"/dags/{dag_id}/dagRuns", {"conf": conf or {}})
    print(f"started {dag_id}: run_id={run['dag_run_id']} state={run['state']} conf={run.get('conf')}")
    return run


def status():
    for dag_id in ("sdi_generate_data", "sdi_pipeline"):
        runs = call("GET", f"/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=5")["dag_runs"]
        print(f"\n{dag_id}")
        for r in runs:
            print(f"  {r['dag_run_id']:<50} {r['state']:<10} conf={r.get('conf')}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "refresh":
        trigger("sdi_generate_data", {"refresh": int(sys.argv[2]) if len(sys.argv) > 2 else 1})
    elif cmd == "pipeline":
        trigger("sdi_pipeline")
    else:
        status()
