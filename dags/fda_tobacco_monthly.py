from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum

# ====== CONFIGURAÇÃO ======
GCP_PROJECT = "gen-lang-client-0010767843"
BQ_DATASET = "fda"
BQ_TABLE = "fda_tobacco_events"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"  # conexão GCP no Airflow
# ==========================

default_args = {
    "owner": "airflow",
    "retries": 2,
}

@dag(
    dag_id="fda_tobacco_monthly",
    default_args=default_args,
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["fda", "tobacco", "api", "bigquery", "monthly"],
)
def fda_tobacco_dag():

    @task
    def fetch_tobacco_reports_by_month(**context):
        year = context["logical_date"].year
        month = context["logical_date"].month

        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        current = start_date
        all_results = []

        while current < end_date:
            date_str = current.strftime("%m/%d/%Y")
            query = f'date_submitted:"{date_str}"'
            url = "https://api.fda.gov/tobacco/problem.json"  # sem espaços!
            params = {"search": query, "limit": 100}

            try:
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 404:
                    current += timedelta(days=1)
                    continue
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                all_results.extend(results)
            except requests.exceptions.RequestException:
                pass  # log opcional com logger

            current += timedelta(days=1)

        return all_results

    @task
    def process_and_load_to_bq(reports: list):
        # === 1. Transformação ===
        rows = []
        for rep in reports:
            date_submitted = rep.get("date_submitted")
            problems = rep.get("reported_health_problems", [])
            if problems:
                for p in problems:
                    rows.append({
                        "date_submitted": date_submitted,
                        "reported_health_problems": p,
                        "cases_number": 1
                    })

        if not rows:
            print("Nenhum dado para carregar neste mês.")
            return

        df = pd.DataFrame(rows)
        df_summary = (
            df.groupby(["date_submitted", "reported_health_problems"])
            .agg(cases_number=("cases_number", "sum"))
            .reset_index()
        )

        # Garantir que 'date_submitted' seja string (já está no formato MM/DD/YYYY)
        df_summary["date_submitted"] = df_summary["date_submitted"].astype(str)
        df_summary["reported_health_problems"] = df_summary["reported_health_problems"].astype(str)
        df_summary["cases_number"] = df_summary["cases_number"].astype(int)

        print(f"Pronto para carregar {len(df_summary)} registros no BigQuery.")

        # === 2. Credenciais do GCP ===
        bq_hook = BigQueryHook(
            gcp_conn_id=GCP_CONN_ID,
            location=BQ_LOCATION,
            use_legacy_sql=False
        )
        credentials = bq_hook.get_credentials()

        # === 3. Schema explícito (opcional, mas recomendado) ===
        table_schema = [
            {"name": "date_submitted", "type": "STRING"},
            {"name": "reported_health_problems", "type": "STRING"},
            {"name": "cases_number", "type": "INTEGER"},
        ]

        destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

        # === 4. Carregar no BigQuery ===
        df_summary.to_gbq(
            destination_table=destination_table,
            project_id=GCP_PROJECT,
            if_exists="append",
            credentials=credentials,
            table_schema=table_schema,
            location=BQ_LOCATION,
            progress_bar=False,
        )

        print(f"Carregado com sucesso {len(df_summary)} linhas em {GCP_PROJECT}.{destination_table}.")

    # === Orquestração ===
    raw_reports = fetch_tobacco_reports_by_month()
    process_and_load_to_bq(raw_reports)

# ⚠️ IMPORTANTE: variável global deve se chamar EXATAMENTE 'dag'
dag = fda_tobacco_dag()
