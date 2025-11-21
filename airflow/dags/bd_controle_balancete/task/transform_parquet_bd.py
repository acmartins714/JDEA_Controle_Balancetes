import pandas as pd
import boto3
import io
import logging
from datetime import date
import firebirdsql

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    logger.info(f"Iniciando transformações para a planilha: {sheet_name}")

    if sheet_name == "Balancetes_Mensais":
        df.columns = [col.strip().lower() for col in df.columns]
        df['NÚM. PROTOCOLO'] = df['NÚM. PROTOCOLO'].str.upper()
        df['DATA ENTRADA'] = pd.to_datetime(df['DATA ENTRADA'], errors='coerce')
        df['SUBCATEGORIA'] = df['SUBCATEGORIA'].str()
        df['ORIGEM'] = df['ORIGEM'].str()
        df['EXERCÍCIO'] = df['EXERCÍCIO'].str()
        df['SETOR'] = df['SETOR'].str()
        df['SIT. JUNTADA'] = df['SIT. JUNTADA'].str()
        df['ESTÁGIO'] = df['ESTÁGIO'].str()
        df['INTERESSADOS'] = df['INTERESSADOS'].str()
        df['DIGITAL'] = df['DIGITAL'].str()
        df['ASSUNTO'] = df['ASSUNTO'].str()

    else:
        logger.warning("Nenhuma transformação específica definida para Balancetes_Mensais")

    return df

def process_silver_layer(bucket_bronze: str, endpoint_url: str, access_key: str, secret_key: str):
    logger.info("Iniciando processamento da camada Silver")

    # realizando conexão com o firebird para gravação dos dados obtidos (repositório bronze)
    # daods são baixados e gravados em sua forma bruta
    try:
        conexaoFDB = firebirdsql.connect(
            host="192.168.15.245",
            database="D://jdea//ControleBalancetes//airflow//dags//bd_controle_balancete//controlebalancete.fdb",
            user="SYSDBA",
            password="masterkey",
            port=63050,
            charset="ISO8859_1"
        )
        logger.info("Conexão estabelecida com sucesso!")

    except firebirdsql.Error as err:
        logger.info(f"Erro ao conectar ao banco de dados: {err}")

    cursor = conexaoFDB.cursor()
    cursorEntidade = conexaoFDB.cursor()

    # Conecta ao MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:

        # Lê os dados da camada Bronze
        response = minio_client.get_object(Bucket=bucket_bronze, Key="balancete.parquet")
        df_bronze = pd.read_parquet(io.BytesIO(response['Body'].read()))
        logger.info("Leitura concluída da camada Bronze para camada Prata")

        ### Aplicando transformações na camada silver

        # obrtendo a competência para processamento (o padrão é pegar mês e ano da tata atual e diminuir 1 do mês )
        # As exceções ainda estão sendo discutidas
        dataAtual = date.today()
        mes = str(dataAtual.month - 1).zfill(2)
        ano = str(dataAtual.year)
        competencia = mes + ano
        meses = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JUNHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO',
                 'NOVEMBRO', 'DEZEMBRO']
        mesReferencia = meses[int(mes) - 1]

        # Função para localização dos códigos dos órgãos da Elmar em
        # Comparação com o TCE-PB
        def localizaCodigoOrgao(nomeOrgao):

            try:
                cursorEntidade.execute(
                    'SELECT CODIGO FROM ENTIDADES WHERE UPPER(RAZAOSOCIAL) = \'' + nomeOrgao.upper() + '\'')
                codigoOrgao = cursorEntidade.fetchall()[0][0]
            except Exception as e:
                print("Ocorreu um erro ao tentar recuperar o código da entidade: {} - Erro: {}".format(nomeOrgao, e))

            if codigoOrgao == '':
                print('Código órgão não localizado - ', nomeOrgao)
            else:
                return codigoOrgao

        # varrendo o datafrane e gravando em banco de dados Firebird
        for i in range(df_bronze.shape[0]):
            sql = 'UPDATE OR INSERT INTO BALANCETES (' + \
                  '  COD_ORGAO, ' + \
                  '  COMPETENCIA, ' + \
                  '  SISTEMA, ' + \
                  '  DATA, ' + \
                  '  PROTOCOLO ' + \
                  ') VALUES (' + \
                  '  \'' + localizaCodigoOrgao(str(df_bronze.values[i][3])) + '\', ' + \
                  '  \'' + competencia + '\', ' + \
                  '  3, ' + \
                  '  \'' + str(df_bronze.values[i][1])[3:6] + str(df_bronze.values[i][1])[0:3] + str(df_bronze.values[i][1])[6:] + '\', ' + \
                  '  \'' + str(df_bronze.values[i][0]) + '\')'

            cursor.execute(sql)
            cursor.transaction.commit()

        # Encerrando a conexão com o Firebird
        cursor.close()
        conexaoFDB.close()

        logger.info("Dados salvos com sucesso no banco de dados (camada Silver)")

    except Exception as e:
        logger.error(f"Erro durante o processamento da camada Silver: {e}")
        raise
