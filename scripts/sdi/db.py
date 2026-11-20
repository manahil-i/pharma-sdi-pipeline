"""Small helpers to open the two databases.

PostgreSQL = staging area + monitoring logs (Grafana reads these).
DuckDB     = the data mart (star schema) + query layer.
"""
import contextlib
import duckdb
import psycopg2
from psycopg2.extras import execute_values
from . import config


@contextlib.contextmanager
def pg():
    """Open a PostgreSQL connection, commit on success, roll back on error."""
    conn = psycopg2.connect(**config.PG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextlib.contextmanager
def mart(read_only=False):
    """Open the DuckDB data-mart file (created automatically the first time)."""
    config.MART_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.MART_FILE), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def pg_exec(sql, params=None):
    with pg() as c, c.cursor() as cur:
        cur.execute(sql, params)


def pg_query(sql, params=None):
    with pg() as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def pg_insert_many(table, columns, rows):
    """Insert many rows at once (used by the monitoring logs)."""
    if not rows:
        return
    with pg() as c, c.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s", rows)


def run_sql_file(path):
    """Run a .sql file on PostgreSQL."""
    with pg() as c, c.cursor() as cur:
        cur.execute(open(path, encoding="utf-8").read())
