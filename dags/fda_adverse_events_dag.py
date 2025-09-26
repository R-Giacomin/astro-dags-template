import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- Configurações da DAG ---
GCP_PROJECT = "gen-lang-client-0010767843"
BQ_DATASET = "fda"
BQ_TABLE = "fda_data"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

API_BASE_URL = "https://api.fda.gov/drug/event.json"
API_LIMIT = 100  # Máximo de registros por página
API_MAX_RECORDS = 10000 # Limite máximo de registros por consulta da API

@dag(
    dag_id='fda_adverse_events_weekly',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval='@weekly',
    catchup=False,
    tags=['fda', 'bigquery'],
    doc_md="""
    ### DAG de Eventos Adversos da FDA

    Esta DAG recupera dados semanais de eventos adversos a medicamentos da API openFDA
    e os carrega no BigQuery.

    **Funcionalidades:**
    - **Execução Semanal:** A DAG é executada uma vez por semana.
    - **Busca de Dados da Semana Anterior:** Cada execução busca os dados da semana anterior à data de execução.
    - **Data de Início Fixa:** A busca de dados não ocorre para datas anteriores a 01/01/2025.
    - **Paginação:** A DAG implementa a paginação para buscar todos os registros, respeitando o limite da API.
    - **Carregamento no BigQuery:** Os dados são carregados em uma tabela do BigQuery.
    """
)
def fda_adverse_events_dag():
    """DAG para buscar dados da API openFDA e carregar no BigQuery."""

    @task
    def fetch_fda_data(**kwargs):
        """
        Busca dados de eventos adversos da API openFDA para a semana anterior.
        Implementa a paginação para obter todos os resultados.
        """
        data_interval_start = kwargs['data_interval_start']
        data_interval_end = kwargs['data_interval_end']

        # Garante que a data de início não seja anterior a 2025-01-01
        start_date_dt = max(data_interval_start, pendulum.datetime(2025, 1, 1, tz="UTC"))
        
        start_date = start_date_dt.strftime('%Y%m%d')
        end_date = data_interval_end.strftime('%Y%m%d')

        print(f"Buscando dados de {start_date} a {end_date}")

        all_results = []
        skip = 0
        total_records_fetched = 0

        while True:
            if total_records_fetched >= API_MAX_RECORDS:
                print(f"Atingido o limite máximo de {API_MAX_RECORDS} registros para esta consulta.")
                break

            search_query = f'receiveddate:[{start_date}+TO+{end_date}]'
            params = {
                'search': search_query,
                'limit': API_LIMIT,
                'skip': skip
            }

            try:
                response = requests.get(API_BASE_URL, params=params)
                response.raise_for_status()  # Lança exceção para códigos de erro HTTP
                data = response.json()

                results = data.get('results', [])
                if not results:
                    print("Não foram encontrados mais resultados. Finalizando a busca.")
                    break

                all_results.extend(results)
                total_records_fetched += len(results)
                print(f"Registros buscados nesta página: {len(results)}. Total acumulado: {total_records_fetched}")

                # Prepara para a próxima página
                skip += API_LIMIT

            except requests.exceptions.RequestException as e:
                print(f"Erro ao chamar a API openFDA: {e}")
                # Decide se deve parar ou tentar novamente (neste caso, paramos)
                break
            except ValueError:
                print("Erro ao decodificar a resposta JSON da API.")
                break

        if not all_results:
            print("Nenhum dado foi retornado pela API para o período.")
            return pd.DataFrame() # Retorna DataFrame vazio para a próxima tarefa lidar com isso

        print(f"Busca finalizada. Total de {len(all_results)} registros recuperados.")
        return pd.DataFrame(all_results)

    @task
    def load_to_bigquery(df: pd.DataFrame):
        """
        Carrega um DataFrame do Pandas para uma tabela do Google BigQuery.
        A tabela será criada se não existir.
        """
        if df.empty:
            print("DataFrame vazio. Nenhuma ação será tomada no BigQuery.")
            return

        print(f"Carregando {len(df)} registros para a tabela {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

        # Converte todas as colunas para string para evitar problemas de tipo no BigQuery
        # A API da FDA pode retornar tipos mistos em algumas colunas
        for col in df.columns:
            df[col] = df[col].astype(str)

        try:
            hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
            client = hook.get_client()

            # Carrega o DataFrame para a tabela do BigQuery
            df.to_gbq(
                destination_table=f'{BQ_DATASET}.{BQ_TABLE}',
                project_id=GCP_PROJECT,
                if_exists='append',  # Adiciona os dados se a tabela já existir
                credentials=hook.get_credentials(),
                progress_bar=False
            )
            print("Carga para o BigQuery concluída com sucesso.")

        except Exception as e:
            print(f"Erro ao carregar dados para o BigQuery: {e}")
            raise

    # --- Define o fluxo da DAG ---
    fda_data_df = fetch_fda_data()
    load_to_bigquery(fda_data_df)

# Instancia a DAG
fda_dag = fda_adverse_events_dag()
