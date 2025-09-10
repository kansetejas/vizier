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

_BOOLEAN_TOKENS = {
    "true","false","t","f","1","0","yes","no","y","n",
    "male","female","m","f"
}
_BOOLEAN_MAP = {
    "true": True, "t": True, "1": True, "yes": True, "y": True,
    "false": False, "f": False, "0": False, "no": False, "n": False,
    "male": True, "m": True,
    "female": False, "f": False,
}
_INT_RANGES = {
    "SMALLINT": (-32768, 32767),
    "INTEGER": (-2147483648, 2147483647),
}


# ---------------- HELPERS ----------------
def _sanitize_identifier(name: str) -> str:
    name = str(name or "").lower().strip()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "col"
    if not re.match(r"^[a-z]", name):
        name = "c_" + name
    return name[:63]

def _infer_sql_type_from_series(ser: pd.Series) -> str:
    cleaned = ser.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return "VARCHAR(10)"

    lower_uniques = {x.lower() for x in cleaned.unique()}
    if lower_uniques.issubset(_BOOLEAN_TOKENS) and len(lower_uniques) <= 4:
        return "BOOLEAN"

    if cleaned.str.match(r"^[+-]?\d+$").all():
        asints = pd.to_numeric(cleaned, errors="coerce").astype("Int64")
        if asints.isnull().any():
            return "BIGINT"
        minv, maxv = int(asints.min()), int(asints.max())
        for name, (lo, hi) in _INT_RANGES.items():
            if lo <= minv and maxv <= hi:
                return name
        return "BIGINT"

    coerced_num = pd.to_numeric(cleaned, errors="coerce")
    if coerced_num.notna().all():
        return "DOUBLE PRECISION"

    parsed = pd.to_datetime(cleaned, errors="coerce", infer_datetime_format=True)
    if parsed.notna().sum() / len(parsed) >= 0.90:
        times = parsed.dt.time
        if (times == pd.to_datetime("00:00:00").time()).all():
            return "DATE"
        return "TIMESTAMP"

    max_len = cleaned.map(len).max()
    if max_len <= 10:
        return "VARCHAR(10)"
    return f"VARCHAR({max_len})"

def _convert_df_to_rows(df: pd.DataFrame, schema: dict) -> list[tuple]:
    df2 = df.copy()
    for col in schema.keys():
        if col not in df2.columns:
            df2[col] = None

    for col, sql_type in schema.items():
        s = df2[col].astype(object)
        if sql_type == "BOOLEAN":
            df2[col] = (
                s.fillna("")
                 .astype(str)
                 .str.strip()
                 .str.lower()
                 .map(_BOOLEAN_MAP)
            )
            df2[col] = df2[col].where(df2[col].isin([True, False]), None)
        elif sql_type in ("SMALLINT","INTEGER","BIGINT"):
            df2[col] = pd.to_numeric(s, errors="coerce").astype("Int64")
            df2[col] = df2[col].where(~df2[col].isna(), None)
        elif sql_type == "DOUBLE PRECISION":
            df2[col] = pd.to_numeric(s, errors="coerce").astype(float)
            df2[col] = df2[col].where(~df2[col].isna(), None)
        elif sql_type == "DATE":
            parsed = pd.to_datetime(s, errors="coerce", infer_datetime_format=True).dt.date
            df2[col] = parsed.where(~parsed.isna(), None)
        elif sql_type == "TIMESTAMP":
            parsed = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
            df2[col] = parsed.where(~parsed.isna(), None)
        else:  # VARCHAR(n)
            df2[col] = s.where(~s.isna(), None).apply(
                lambda v: str(v).strip() if v is not None and str(v).strip() != "" else None
            )
    ordered = df2[list(schema.keys())]
    tuples = [tuple(x) for x in ordered.replace({pd.NA: None}).to_numpy().tolist()]
    return tuples

def _download_from_drive(file_id: str) -> str:
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text


# ---------------- DAG ----------------
@dag(
    dag_id="gdrive_to_staging_and_structured_v2",
    start_date=datetime(2025, 8, 6),
    schedule=None,
    catchup=False,
    tags=["gdrive", "staging", "dbt", "structured"],
)
def gdrive_to_staging_pipeline():
    @task
    def load_one_file(table_name: str, file_id: str):
        csv_text = _download_from_drive(file_id)
        df = pd.read_csv(
            StringIO(csv_text),
            dtype=str,
            keep_default_na=False,
            na_values=["","NA","null"]
        )
        if df.empty:
            raise ValueError(f"{table_name} is empty")

        original_cols = list(df.columns)
        sanitized_cols = [_sanitize_identifier(c) for c in original_cols]
        df.columns = sanitized_cols

        schema = {col: _infer_sql_type_from_series(df[col]) for col in df.columns}

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        staging_name = f"staging_{_sanitize_identifier(table_name)}"
        cols_sql = ", ".join([f'"{c}" {schema[c]}' for c in schema.keys()])
        create_sql = f'CREATE TABLE IF NOT EXISTS public."{staging_name}" ({cols_sql});'
        pg.run(create_sql, autocommit=True)
        pg.run(f'TRUNCATE TABLE public."{staging_name}";', autocommit=True)

        rows = _convert_df_to_rows(df, schema)
        if rows:
            pg.insert_rows(
                table=f'public."{staging_name}"',
                rows=rows,
                target_fields=list(schema.keys()),
                commit_every=1000
            )
        return f"Loaded {len(df)} rows into {staging_name}"

    # create tasks for each file
    load_tasks = []
    for name, file_id in GDRIVE_FILE_IDS.items():
        t = load_one_file.override(task_id=f"load_{name}")(name, file_id)
        load_tasks.append(t)

    trigger_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run-operation transform_all_staging_tables --profiles-dir ."
    )

    @task
    def build_structured_tables():
        """
        Build structured_* tables from transformed_* tables with:
          - structured_patients: PRIMARY KEY (id)
          - others with 'patient' column: FOREIGN KEY (patient) REFERENCES structured_patients(id)

        Safe for re-runs. Works only on public schema.
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        # Helpers (SQL)
        def table_exists(tbl: str) -> bool:
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name=%s
            """, (tbl,))
            return cur.fetchone() is not None

        def list_columns(tbl: str) -> list[str]:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
            """, (tbl,))
            return [r[0] for r in cur.fetchall()]

        def has_pk(tbl: str) -> bool:
            cur.execute("""
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_schema='public'
                  AND table_name=%s
                  AND constraint_type='PRIMARY KEY'
            """, (tbl,))
            return cur.fetchone() is not None

        def fk_exists(tbl: str, conname: str) -> bool:
            cur.execute("""
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_schema='public'
                  AND table_name=%s
                  AND constraint_name=%s
                  AND constraint_type='FOREIGN KEY'
            """, (tbl, conname))
            return cur.fetchone() is not None

        # Discover transformed tables
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE 'transformed_%'
            ORDER BY table_name
        """)
        transformed_tables = [r[0] for r in cur.fetchall()]

        if not transformed_tables:
            raise RuntimeError("No transformed_% tables found. Ensure dbt produced them.")

        # Ensure patients is first
        patients_src = "transformed_patients"
        if patients_src not in transformed_tables:
            raise RuntimeError("Expected transformed_patients table not found.")
        ordered = [patients_src] + [t for t in transformed_tables if t != patients_src]

        # --- PASS 1: create structured tables (patients first), load data ---
        for src in ordered:
            base = src.replace("transformed_", "", 1)
            structured = f"structured_{base}"

            # Create table if needed (copy exact definition)
            if not table_exists(structured):
                cur.execute(f'''
                    CREATE TABLE public."{structured}" (LIKE public."{src}" INCLUDING ALL);
                ''')
            else:
                # Table exists: keep it; we'll TRUNCATE and insert using column intersection.
                pass

            # TRUNCATE structured table
            cur.execute(f'TRUNCATE TABLE public."{structured}";')

            # Build column intersection for safe insert
            src_cols = list_columns(src)
            dst_cols = list_columns(structured)
            common = [c for c in src_cols if c in dst_cols]
            if not common:
                raise RuntimeError(f"No common columns between {src} and {structured}.")

            cols_csv = ", ".join([f'"{c}"' for c in common])
            cur.execute(f'''
                INSERT INTO public."{structured}" ({cols_csv})
                SELECT {cols_csv} FROM public."{src}";
            ''')

        # --- PASS 2: constraints (PK, then FKs) ---
        # 2a. PRIMARY KEY on structured_patients(id)
        structured_patients = "structured_patients"
        patients_cols = list_columns(structured_patients)
        if "id" not in patients_cols:
            raise RuntimeError("structured_patients must contain 'id' column for PRIMARY KEY.")

        if not has_pk(structured_patients):
            cur.execute(f'''
                ALTER TABLE public."{structured_patients}"
                ADD PRIMARY KEY ("id");
            ''')

        # 2b. FOREIGN KEY(patient) on other structured tables
        for src in ordered:
            base = src.replace("transformed_", "", 1)
            if base == "patients":
                continue

            structured = f"structured_{base}"
            cols = list_columns(structured)
            if "patient" not in cols:
                # No FK needed; skip
                continue

            # Constraint name (trim to 63 chars)
            raw_conname = f'fk_{structured}_patient__structured_patients_id'
            conname = _sanitize_identifier(raw_conname)[:63]

            if not fk_exists(structured, conname):
                # Add FK after data load (will validate existing rows)
                cur.execute(f'''
                    ALTER TABLE public."{structured}"
                    ADD CONSTRAINT "{conname}"
                    FOREIGN KEY ("patient") REFERENCES public."{structured_patients}"("id");
                ''')

        conn.commit()
        cur.close()
        conn.close()

    load_tasks >> trigger_dbt >> build_structured_tables()

gdrive_to_staging_pipeline()