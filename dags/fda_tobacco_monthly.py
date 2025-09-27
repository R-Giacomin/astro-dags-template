from datetime import datetime
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Configurações
GCP_PROJECT = "gen-lang-client-0010767843"
BQ_DATASET = "fda"
BQ_TABLE = "fda_tobacco_events"
BQ_LOCATION = "US"

# Parâmetros da DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
}

@dag(
    dag_id="fda_tobacco_monthly",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=["fda", "tobacco", "api", "monthly"],
)
def fda_tobacco_dag():

    @task
    def fetch_tobacco_reports_by_month(year: int, month: int):
        from datetime import datetime as dt, timedelta

        start_date = dt(year, month, 1)
        if month == 12:
            end_date = dt(year + 1, 1, 1)
        else:
            end_date = dt(year, month + 1, 1)

        current = start_date
        all_results = []

        while current < end_date:
            date_str = current.strftime("%m/%d/%Y")
            query = f'date_submitted:"{date_str}"'
            url = "https://api.fda.gov/tobacco/problem.json"
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
                pass  # ou log com logger

            current += timedelta(days=1)

        return all_results

    @task
    def build_and_aggregate_dataframe(reports: list):
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
            # Retorna DataFrame vazio com schema explícito
            df = pd.DataFrame(columns=["date_submitted", "reported_health_problems", "cases_number"])
        else:
            df = pd.DataFrame(rows)

        # Agregação
        df_summary = (
            df.groupby(["date_submitted", "reported_health_problems"])
            .agg(cases_number=("cases_number", "sum"))
            .reset_index()
        )

        # Converte para dict para XCom (JSON serializável)
        return df_summary.to_dict(orient="records")

    # Obter ano/mês da data de execução (data lógica do DAG)
    @task
    def get_year_month(**context):
        logical_date = context["logical_date"]
        return {"year": logical_date.year, "month": logical_date.month}

    # Orquestração
    year_month = get_year_month()
    raw_reports = fetch_tobacco_reports_by_month(
        year=year_month["year"],
        month=year_month["month"]
    )
    aggregated_data = build_and_aggregate_dataframe(raw_reports)

    # Salvar no BigQuery (comentei, mas deixei como exemplo)
    @task
    def save_to_bigquery(data: list):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        df = pd.DataFrame(data)
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", location=BQ_LOCATION)
        hook.insert_all(
            project_id=GCP_PROJECT,
            dataset_id=BQ_DATASET,
            table_id=BQ_TABLE,
            rows=df.to_dict(orient="records"),
            ignore_unknown_values=True,
            skip_invalid_rows=True
        )

    save_to_bigquery(aggregated_data)

    # O resultado final está em `aggregated_data`, salvo automaticamente no XCom
    return aggregated_data

# Instancia a DAG
dag_instance = fda_tobacco_dag()
