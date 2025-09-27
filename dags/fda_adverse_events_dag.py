from __future__ import annotations
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pendulum
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Configurações
GCP_PROJECT = "gen-lang-client-0010767843" 
BQ_DATASET  = "fda"     
BQ_TABLE    = "fda_tobacco_events"
BQ_LOCATION = "US"      

BASE_URL = "https://api.fda.gov/tobacco/problem.json"

# Definição da DAG
@dag(
    dag_id="fda_tobacco_monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # ✅ inicia em jan/2024
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["fda", "tobacco", "monthly"],
)
def fda_tobacco_monthly():

    @task()
    def fetch_and_load_tobacco_data(**kwargs):
        ctx = get_current_context()
        logical_date = ctx["data_interval_start"]

        start_date = logical_date.format("YYYY-MM-DD")
        end_date   = ctx["data_interval_end"].format("YYYY-MM-DD")
        print(f"🔍 Buscando dados de Tabaco de {start_date} até {end_date}")

        all_results = []
        skip = 0
        limit = 100

        while True:
            url = (
                f"{BASE_URL}?search=date_submitted:[{start_date}+TO+{end_date}]"
                f"&limit={limit}&skip={skip}"
            )
            response = requests.get(url)

            if response.status_code in [400, 404, 500]:
                print(f"🔎 Status {response.status_code}: Sem dados no período {start_date} → {end_date}")
                break

            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                print("Nenhum relatório encontrado no período.")
                break

            all_results.extend(results)

            if len(results) < limit:
                break
            skip += limit

        if not all_results:
            print("⚠️ Nenhum dado carregado.")
            return

        # Converter para DataFrame
        df = pd.json_normalize(all_results)
        df["data_inicio"] = start_date
        df["data_fim"]    = end_date

        # Carregar para BigQuery
        bq = BigQueryHook(gcp_conn_id="google_cloud_default", location=BQ_LOCATION)
        client = bq.get_client(project_id=GCP_PROJECT)

        job = client.load_table_from_dataframe(df, f"{BQ_DATASET}.{BQ_TABLE}")
        job.result()
        print(f"✅ {len(df)} registros carregados em {BQ_DATASET}.{BQ_TABLE}")

    fetch_and_load_tobacco_data()

dag = fda_tobacco_monthly()
