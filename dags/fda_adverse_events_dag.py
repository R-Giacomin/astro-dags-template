import pendulum
from datetime import timedelta

# Importações do Airflow
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Bibliotecas para processamento de dados e requisições 
import requests
import pandas as pd
import pandas_gbq # Necessário para o método df.to_gbq()

# ====== CONFIGURAÇÃO GERAL ======
# Variáveis de Configuração do Usuário
GCP_PROJECT = "gen-lang-client-0010767843" 
BQ_DATASET  = "fda"     
BQ_TABLE    = "fda_data"
BQ_LOCATION = "US"      # Localização do dataset: "US" ou "EU"
GCP_CONN_ID = "google_cloud_default" # ID da Conexão do Airflow

# Constantes da API
API_BASE_URL = "https://api.fda.gov/drug/event.json"
API_LIMIT = 100         # Máximo de registros por página
API_MAX_RECORDS = 10000 # Limite máximo de registros por consulta da API (100 * 100 iterações)

# Data de início mais antiga permitida (01/01/2025)
API_DATE_START_CONSTRAINT = pendulum.datetime(2025, 1, 1, tz="UTC") 

# =================================

@dag(
    dag_id='fda_adverse_events_weekly',
    start_date=API_DATE_START_CONSTRAINT, # A DAG só começa a rodar a partir desta data
    schedule='@weekly', # Executa uma vez por semana
    catchup=True, # Permite que a DAG execute retroativamente desde o start_date
    tags=['fda', 'bigquery', 'api'],
    doc_md="""
    ### DAG de Eventos Adversos da FDA (Carregamento Semanal)

    Esta DAG recupera dados semanais de eventos adversos a medicamentos da API openFDA
    e os carrega no Google BigQuery. Implementa paginação e normalização de JSON.
    """
)
def fda_adverse_events_dag():
    """DAG para buscar dados da API openFDA e carregar no BigQuery."""

    @task
    def fetch_fda_data(**kwargs) -> pd.DataFrame:
        """
        Busca dados de eventos adversos da API openFDA para o intervalo da execução.
        Implementa paginação e normaliza o JSON para um DataFrame plano, pronto para o BQ.
        """
        # Captura o intervalo de tempo definido pelo Airflow para a execução semanal
        data_interval_start = kwargs['data_interval_start']
        data_interval_end = kwargs['data_interval_end']

        # 1. Garante que a data de início nunca seja anterior a 2025-01-01
        start_date_dt = max(data_interval_start, API_DATE_START_CONSTRAINT)
        
        # Formato de data exigido pela API: AAAAMMDD
        start_date = start_date_dt.strftime('%Y%m%d')
        end_date = data_interval_end.strftime('%Y%m%d')

        print(f"Buscando dados no intervalo: {start_date} até {end_date}")

        all_results = []
        skip = 0
        total_records_fetched = 0

        while True:
            # 2. Verifica o limite de paginação (10.000 registros)
            if skip >= API_MAX_RECORDS:
                print(f"Atingido o limite de paginação da API openFDA ({API_MAX_RECORDS} registros) para este período.")
                break

            search_query = f'receiveddate:[{start_date}+TO+{end_date}]'
            params = {
                'search': search_query,
                'sort': 'receiveddate:desc', 
                'limit': API_LIMIT,
                'skip': skip
            }

            try:
                # Realiza a requisição
                response = requests.get(API_BASE_URL, params=params, timeout=30)
                response.raise_for_status() # Verifica erros HTTP
                data = response.json()

                results = data.get('results', [])
                
                if not results:
                    print("Não foram encontrados mais resultados para este período. Finalizando a busca.")
                    break

                all_results.extend(results)
                total_records_fetched += len(results)
                print(f"Registros buscados nesta página: {len(results)}. Total acumulado: {total_records_fetched}")

                # 3. Prepara para a próxima página ou finaliza
                if len(results) < API_LIMIT:
                    break
                
                skip += API_LIMIT

            except requests.exceptions.RequestException as e:
                print(f"Erro ao chamar a API openFDA na requisição com skip={skip}: {e}")
                break
            except ValueError:
                print("Erro ao decodificar a resposta JSON da API.")
                break

        print(f"Busca finalizada. Total de {len(all_results)} registros recuperados.")

        if not all_results:
            return pd.DataFrame() 

        # 4. NORMALIZAÇÃO: Converte o JSON aninhado em um DataFrame plano
        df = pd.json_normalize(all_results, errors='ignore')

        # Converte a coluna de data para o formato datetime para garantir compatibilidade com BQ
        if 'receiveddate' in df.columns:
            df['receiveddate'] = pd.to_datetime(df['receiveddate'], format='%Y%m%d', errors='coerce')
            
        return df # Retornado para ser salvo automaticamente no XCom

    @task
    def load_to_bigquery(df: pd.DataFrame):
        """
        Carrega o DataFrame, que foi normalizado, para a tabela do Google BigQuery.
        """
        if df.empty:
            print("DataFrame vazio. Nenhuma ação será tomada no BigQuery.")
            return

        print(f"Carregando {len(df)} registros para a tabela {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        
        try:
            # df.to_gbq é usado para carregamento robusto
            df.to_gbq(
                destination_table=f'{BQ_DATASET}.{BQ_TABLE}',
                project_id=GCP_PROJECT,
                if_exists='append', # Adiciona os dados se a tabela já existir
                credentials=hook.get_credentials(),
                progress_bar=False,
                chunksize=10000 
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
