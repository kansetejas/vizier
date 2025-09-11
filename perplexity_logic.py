from __future__ import annotations
from datetime import datetime
from io import StringIO
import re
import requests
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

POSTGRES_CONN_ID = "data_connection"
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_project"
GDRIVE_FILE_IDS = {
    "patients": "1AzpI596R13CGCE7ZcADcgUJamVDVp9Go",
    "encounters": "1cXeVjljoOPH3GbCcx-41sy40VfCf_cn-",
    "observations": "1CTApdrOuRfVUTC-BcWY8QmWDDDSLQgU7",
}

# ---------------- HELPERS ----------------
def _sanitize_identifier(name: str) -> str:
    """Sanitizes a string to be a valid SQL identifier."""
    name = str(name or "").lower().strip()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if not name:
        name = "col"
    if not re.match(r'^[a-z]', name):
        name = 'c_' + name
    return name[:63]

def _download_from_drive(file_id: str) -> str:
    """Downloads a file from a public Google Drive link."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text

# ---------------- DAG ----------------
@dag(
    dag_id="gdrive_to_dbt_elt_pipeline",
    start_date=datetime(2025, 8, 6),
    schedule=None,
    catchup=False,
    tags=["gdrive", "staging", "dbt", "elt"],
    doc_md="""
    ### ELT Pipeline for Healthcare Data
    This DAG extracts data from Google Drive, loads it into staging tables,
    and then triggers a dbt operation to handle all transformations.
    """,
)
def gdrive_to_dbt_elt_pipeline():
    @task
    def load_to_staging(table_name: str, file_id: str):
        """
        Extracts data from a Google Drive file and loads it into a staging table.
        All columns are loaded as VARCHAR to let dbt handle type casting.
        """
        csv_text = _download_from_drive(file_id)
        df = pd.read_csv(
            StringIO(csv_text),
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA", "null", "NULL"]
        )
        if df.empty:
            raise ValueError(f"Downloaded file for {table_name} is empty.")
            
        original_cols = list(df.columns)
        sanitized_cols = [_sanitize_identifier(c) for c in original_cols]
        df.columns = sanitized_cols
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        staging_table_name = f"staging_{_sanitize_identifier(table_name)}"
        
        # All columns are VARCHAR. dbt will handle casting.
        cols_sql = ", ".join([f'"{c}" VARCHAR' for c in sanitized_cols])
        create_sql = f'CREATE TABLE IF NOT EXISTS public."{staging_table_name}" ({cols_sql});'
        
        pg_hook.run(create_sql, autocommit=True)
        pg_hook.run(f'TRUNCATE TABLE public."{staging_table_name}";', autocommit=True)
        
        # Convert DataFrame to a list of tuples for insert_rows
        rows = [tuple(x) for x in df.replace({pd.NA: None}).to_numpy()]
        if rows:
            pg_hook.insert_rows(
                table=f'public."{staging_table_name}"',
                rows=rows,
                target_fields=sanitized_cols,
                commit_every=1000,
            )
        return f"Loaded {len(df)} rows into {staging_table_name}"

    # Create dynamic tasks for each file
    load_tasks = [
        load_to_staging.override(task_id=f"load_{name}")(name, file_id)
        for name, file_id in GDRIVE_FILE_IDS.items()
    ]

    # **THE FIX IS HERE:** Use `dbt run-operation` to execute the macro
    trigger_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run-operation transform_all_staging_tables --profiles-dir .",
    )
    
    load_tasks >> trigger_dbt_transformations

gdrive_to_dbt_elt_pipeline()
