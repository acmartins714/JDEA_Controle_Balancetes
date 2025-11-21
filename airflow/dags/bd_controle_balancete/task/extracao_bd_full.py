from time import sleep
from datetime import date
import pandas as pd
import boto3
import io
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from playwright.sync_api import sync_playwright, expect

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def excel_to_minio_etl_parquet_full(plan_name: str, bucket_bronze: str, endpoint_url, access_key, secret_key):
    # Configuração do cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    '''
    # Realizar raspagem dos dados necessários no site do TCE-PB
    # realizando raspagem no site do TCE-PB para obter os dados atualizados de remessa de balancetes

    ano = '2025'
    mesReferencia = 'OUTUBRO'

    with (sync_playwright() as p):

        # definindo o navegador que será utilizado (chromium, firefox ou webkit(safari)
        browser = p.firefox.launch(headless=True)  # Após a fase de testes fazer headless=True (Não mostrar o browse)

        # aqui podem ser definidos alguns parâmetros do navegador
        # para mais detalhes: https://playwright.dev/python/docs/api/class-browsercontext
        # contexto = browser.new_context() # não foi necessário para está aplicação

        # Criação e abertura da página que vai ser trabalhada
        page = browser.new_page()
        page.goto("https://tramita.tce.pb.gov.br/tramita/pages/main.jsf")

        # Localiza e clica no botão Listar Documentos (Abre a folha de pesquisa)
        botao = page.get_by_title("Listar Documentos", exact=True)
        botao.click()
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        logging.error("preenchendo os campos necessários no formulário de pesquisa")  # ajustar saída para o log do Airflow

        # Localiza e preenche o campo categoria
        cbx_categoria = page.locator("iframe[name=\"body\"]").content_frame.get_by_label("Categoria", exact=True)
        expect(cbx_categoria).to_be_visible()
        cbx_categoria.select_option("1")
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        # Localiza e preenche o campo sub-categoria
        cbx_subCategoria = page.locator("iframe[name=\"body\"]").content_frame.get_by_label("Subcategoria")
        expect(cbx_subCategoria).to_be_visible()
        cbx_subCategoria.select_option("16")
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        # Localiza e preenche o campo exercício
        edt_ano = page.locator("iframe[name=\"body\"]").content_frame.get_by_role("textbox", name="Exercício")
        expect(edt_ano).to_be_visible()
        edt_ano.fill(str(ano))  # preenchendo campo exercício com o ano de referencia
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        # Localiza e preenche o campo assunto
        edt_assunto = page.locator("iframe[name=\"body\"]").content_frame.get_by_role("textbox", name="Assunto")
        expect(edt_ano).to_be_visible()
        edt_assunto.fill(mesReferencia)  # preenchendo o campo assunto com o mês de refencia
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        # Localiza e clica no botão Procurar (Efetivação da busca com os dados preenchidos)
        btb_pesquisa = page.locator("iframe[name=\"body\"]").content_frame.get_by_role("button", name="Procurar")
        expect(btb_pesquisa).to_be_visible()
        btb_pesquisa.click()
        sleep(5)  # aguardando um tempo para simular um comportamento humano

        logging.INFO("Dados consultados com sucesso!")
        logging.INFO("Baixando a planilha!")

        # Criando evento para exibicão do local e nome de arquivo baixado (A informação exibida são de local e nome de arquivos temporários)
        page.on("download",
                lambda download: logging.error(download.path()))  # ajustar saída para o log do Airflow (Não é necessário)

        # Fazendo o download da planilha com os dados retornados na pesquisa
        # Dizendo ao Playwright para aguardar o final do download
        with page.expect_download() as download_info:
            # iniciando o download
            btb_excel = page.locator("iframe[name=\"body\"]").content_frame.get_by_title("Exportar p/ Excel", exact=True)
            btb_excel.click()

        # Obtendo os dados do download realizado
        download = download_info.value
        # Salvando o arquivo baixado (Fornecendo local e nome do arquivo)
        download.save_as("/opt/airflow/data/Balancetes_Mensais.xls")  # + download.suggested_filename)
        sleep(2)  # aguardando um tempo para simular um comportamento humano

        # Fechando o contextos e o browse
        page.close()
        browser.close()
    '''
    # Abrir planilha do Excel e tranformar os dados em um dataFrame para gravar em parquet.
    def get_excel_sheet_data(plan_name):
        try:
          df = pd.read_csv(plan_name, encoding="latin-1", sep="\t", skiprows=14)
          return df
        except Exception as e:
          logging.error(f"Erro ao obter dados da planilha do TCE-PB: {e}")
          raise


    # Escrita dos dados no minio
    try:
        df = get_excel_sheet_data(plan_name)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=bucket_bronze, Key="balancete.parquet", Body=parquet_buffer.getvalue())
    except Exception as e:
        logging.error(f"Erro ao processar a planilha {plan_name}: {e}")
        raise

