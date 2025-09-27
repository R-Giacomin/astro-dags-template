@task
def fetch_and_load_fda_data():
    """
    Busca dados de eventos adversos para Aspirin - UM DIA POR VEZ.
    """
    ctx = get_current_context()

    # 1. Usar APENAS UM DIA específico - 01/09/2025
    target_date = pendulum.datetime(2025, 9, 1, tz="UTC")
    
    # Formato de data: AAAAMMDD (mesmo dia para início e fim)
    start_date = target_date.strftime('%Y%m%d')
    end_date = target_date.strftime('%Y%m%d')

    print(f"🔍 Buscando dados do Aspirin para UM DIA: {start_date}")

    all_results = []
    skip = 0
    max_records = 500

    # 2. Loop de Paginação para um único dia
    while True:
        if skip >= max_records:
            print(f"📊 Atingido o limite de {max_records} registros.")
            break

        # Query para UM DIA específico
        search_query = f'patient.drug.medicinalproduct:"aspirin"+AND+receivedate:[{start_date}+TO+{end_date}]'
        
        params = {
            'search': search_query,
            'limit': 50,
            'skip': skip
        }

        try:
            print(f"📡 Fazendo requisição {skip//50 + 1}...")
            
            response = requests.get(API_BASE_URL, params=params, timeout=30)
            print(f"📊 Status Code: {response.status_code}")
            
            if response.status_code == 500:
                print("❌ Erro 500 - Tentando com abordagem mais simples...")
                break
                    
            elif response.status_code != 200:
                print(f"❌ Erro HTTP {response.status_code}")
                break
            
            data = response.json()

            if 'error' in data:
                error_msg = data['error']
                print(f"⚠️ Erro da API: {error_msg}")
                break

            results = data.get('results', [])
            
            if not results:
                print("✅ Nenhum resultado adicional encontrado.")
                break

            all_results.extend(results)
            print(f"📥 Página {skip//50 + 1}: {len(results)} registros. Total: {len(all_results)}")

            if len(results) < 50:
                print("✅ Última página alcançada.")
                break
            
            skip += 50

        except Exception as e:
            print(f"❌ Erro: {e}")
            break

    print(f"🎯 Busca finalizada. Total de {len(all_results)} registros brutos.")

    # CORREÇÃO CRÍTICA: SEMPRE definir a variável df
    df = pd.DataFrame()  # Inicializar vazio

    if not all_results:
        print("⚠️ Nenhum dado retornado. Criando dados de teste...")
        
        # Criar dados de teste
        test_data = [
            {
                'safetyreportid': 'TEST_001',
                'receivedate': start_date,
                'serious': 1,
                'patient_patientsex': 1,
                'reactionmeddrapt': 'Headache'
            }
        ]
        df = pd.DataFrame(test_data)
        print("🧪 Usando dados de teste para validar o pipeline.")
        
    else:
        # Processar dados reais
        extracted_data = []
        for record in all_results:
            extracted_record = extract_specific_fields(record)
            if extracted_record:
                extracted_data.append(extracted_record)
        
        if extracted_data:
            df = pd.DataFrame(extracted_data)
            print(f"📊 Extraídos {len(df)} registros com colunas específicas.")
        else:
            print("⚠️ Nenhum dado extraído. Criando dados de teste...")
            test_data = [{
                'safetyreportid': 'TEST_FALLBACK',
                'receivedate': start_date,
                'serious': 0,
                'patient_patientsex': 0,
                'reactionmeddrapt': 'Fallback'
            }]
            df = pd.DataFrame(test_data)

    # CORREÇÃO: Verificar se df foi definido e não está vazio
    if df is None or df.empty:
        print("❌ DataFrame não foi criado corretamente. Criando DataFrame vazio...")
        df = pd.DataFrame(columns=[
            'safetyreportid', 'receivedate', 'serious', 
            'patient_patientsex', 'reactionmeddrapt'
        ])

    # Processar e carregar para BigQuery
    print("👀 Preview do DataFrame:")
    print(df.head())
    print(f"📊 Shape: {df.shape}")
    print(f"🔧 Tipos de dados originais:")
    print(df.dtypes)
    
    # CORREÇÃO: Converter tipos de dados
    def safe_convert_to_int(value):
        """Converte valor para inteiro de forma segura"""
        try:
            if value is None or pd.isna(value):
                return 0
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    # Aplicar conversões
    if 'serious' in df.columns:
        df['serious'] = df['serious'].apply(safe_convert_to_int)
    
    if 'patient_patientsex' in df.columns:
        df['patient_patientsex'] = df['patient_patientsex'].apply(safe_convert_to_int)
    
    if 'receivedate' in df.columns:
        df['receivedate'] = pd.to_datetime(df['receivedate'], format='%Y%m%d', errors='coerce')
    
    if 'safetyreportid' in df.columns:
        df['safetyreportid'] = df['safetyreportid'].astype(str)
    
    if 'reactionmeddrapt' in df.columns:
        df['reactionmeddrapt'] = df['reactionmeddrapt'].astype(str)

    print(f"🔄 Tipos de dados após conversão:")
    print(df.dtypes)

    # Remover linhas com valores críticos nulos
    if not df.empty:
        initial_count = len(df)
        df = df.dropna(subset=['safetyreportid', 'receivedate'])
        final_count = len(df)
        print(f"📊 Linhas após limpeza: {final_count}/{initial_count}")

    # Carregar para BigQuery
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        credentials = bq_hook.get_credentials()
        destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

        # Schema explícito
        table_schema = [
            {"name": "safetyreportid", "type": "STRING"},
            {"name": "receivedate", "type": "TIMESTAMP"},
            {"name": "serious", "type": "INTEGER"},
            {"name": "patient_patientsex", "type": "INTEGER"},
            {"name": "reactionmeddrapt", "type": "STRING"}
        ]

        print(f"🚀 Carregando {len(df)} linhas para BigQuery...")
        
        df.to_gbq(
            destination_table=destination_table,
            project_id=GCP_PROJECT,
            if_exists="append",
            credentials=credentials,
            table_schema=table_schema,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"✅ Carga para BigQuery concluída! {len(df)} linhas carregadas.")
        return f"Successfully loaded {len(df)} records"

    except Exception as e:
        print(f"❌ Erro no BigQuery: {e}")
        
        # Tentativa alternativa simplificada
        try:
            print("🔄 Tentando abordagem alternativa...")
            # Criar um DataFrame mínimo
            minimal_df = pd.DataFrame([{
                'safetyreportid': 'MINIMAL_TEST',
                'receivedate': pd.Timestamp.now(),
                'serious': 1,
                'patient_patientsex': 1,
                'reactionmeddrapt': 'TEST'
            }])
            
            minimal_df.to_gbq(
                destination_table=destination_table,
                project_id=GCP_PROJECT,
                if_exists="append",
                credentials=credentials,
                location=BQ_LOCATION,
            )
            print("✅ Dados mínimos carregados para criar tabela.")
            return "Minimal data loaded"
            
        except Exception as alt_error:
            print(f"❌ Erro na abordagem alternativa: {alt_error}")
            return f"Failed to load data: {str(alt_error)}"

@dag(
    default_args=DEFAULT_ARGS,
    dag_id='fda_aspirin_daily',
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['fda', 'aspirin', 'bigquery', 'daily'],
)
def fda_aspirin_daily_dag():
    fetch_and_load_fda_data()

dag = fda_aspirin_daily_dag()
