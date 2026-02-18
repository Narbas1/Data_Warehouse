from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import psycopg


PROJECT_DIR = "/opt/project"
SQL_DIR = f"{PROJECT_DIR}/sql"

def run_sql_file(path: str):
	from dotenv import load_dotenv
	load_dotenv("/opt/project/src/pipeline.env")
	import os
	import psycopg
	from pathlib import Path

	conn_str = (
		f"host={os.getenv('PGHOST')} port={os.getenv('PGPORT', '5432')} dbname={os.getenv('PGDATABASE')} "
		f"user={os.getenv('PGUSER')} password={os.getenv('PGPASSWORD')}"
	)

	sql = Path(path).read_text()

	with psycopg.connect(conn_str) as conn:
		with conn.cursor() as cur:
			cur.execute(sql)
		conn.commit()




with DAG(
    dag_id="crypto_dwh_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",          # manual for now
    catchup=False,
    max_active_runs=1,      # avoid overlapping watermark updates
) as dag:

    ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="python /opt/project/src/ingestion.py",
    )

    silver = PythonOperator(
        task_id="silver_incremental",
        python_callable=run_sql_file,
        op_args=[f"{SQL_DIR}/silver/load_silver_prices.sql"],
    )

    gold = PythonOperator(
        task_id="gold_refresh",
        python_callable=run_sql_file,
        op_args=[f"{SQL_DIR}/gold/gold_latest_refresh.sql"],
    )

    ingest >> silver >> gold
