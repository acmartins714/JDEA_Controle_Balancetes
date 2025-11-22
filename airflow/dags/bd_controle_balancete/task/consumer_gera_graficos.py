import io
import logging
from datetime import date
import matplotlib.pyplot as plt
import numpy as np
import firebirdsql

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_consumer_layer():
    logger.info("Iniciando a geração dos gráficos a partir dos dados obtidos (Consumer gold layer) ")

    # obrtendo a competência para processamento (o padrão é pegar mês e ano da tata atual e diminuir 1 do mês )
    # As exceções ainda estão sendo discutidas
    dataAtual = date.today()
    mes = str(dataAtual.month - 1).zfill(2)
    ano = str(dataAtual.year)
    competencia = mes + ano
    meses = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO', 'JUNHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO',
             'NOVEMBRO', 'DEZEMBRO']
    mesReferencia = meses[int(mes) - 1]

    # Obtendo dados do firebird
    try:
        # realizando conexão com o firebird para obtenção de dados para geração dos graficos
        try:

            conexaoFDB = firebirdsql.connect(
                host="10.253.67.215", #192.168.15.245
                database="D://jdea//ControleBalancetes//airflow//dags//bd_controle_balancete//controlebalancete.fdb",
                user="SYSDBA",
                password="masterkey",
                port=63050,
                charset="ISO8859_1"
            )

            cursor = conexaoFDB.cursor()
            cursorEntidade = conexaoFDB.cursor()

            logger.info("Conexão estabelecida com sucesso!")
        except firebirdsql.Error as err:
            logger.info(f"Erro ao conectar ao banco de dados: {err}")

        # Apresentação dos gráficos

        # Gerando valores do gráfico
        categorias = ['Prefeitura', 'Câmara', 'Independente', 'Indireta']

        # Lista com os sistemas participantes do controle

        sistemas = [
            ["3","CONTABILIDADE"],
            ["1","FOLHA"],["18","FARMÁCIA"],
            ["19","FROTA"]]

        cursor = conexaoFDB.cursor()

        for sistema in range(len(sistemas[0])):

            try:
                sql = 'Select ' + \
                      '  1 as Id, ' + \
                      '  \'Total de Entidades\' as Descricao, ' + \
                      '  Coalesce((Select Count(Cod_Orgao) From Clientes Where SubString(Cod_Orgao From 1 For 1) = \'1\' and Sistema = ' + sistemas[sistema][0] + '), 0.00) as QuantidadeCamara, ' + \
                      '  Coalesce((Select Count(Cod_Orgao) as QuantidadePrefeitura From Clientes Where SubString(Cod_Orgao From 1 For 1) = \'2\' and Sistema = ' + sistemas[sistema][0] + '), 0.00) as QuantidadePrefeitura, ' + \
                      '  Coalesce((Select Count(Cod_Orgao) as QuantidadeIndiretas From Clientes Where SubString(Cod_Orgao From 1 For 1) in (\'3\',\'4\',\'5\',\'6\') and Sistema = ' + sistemas[sistema][0] + '), 0.00) as QuantidadeIndiretas, ' + \
                      '  Coalesce((Select Count(Cod_Orgao) as QuantidadeIndiretas From Clientes Where SubString(Cod_Orgao From 1 For 1) = \'7\' and Sistema = ' + sistemas[sistema][0] + '), 0.00) as QuantidadeIndependentes ' + \
                      'From ' + \
                      '  RDB$DATABASE ' + \
                      'Union All ' + \
                      'Select ' + \
                      '  2 as Id, ' + \
                      '  \'Entidades Entregues\' as Descricao, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'1\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = Clientes.Cod_Orgao)), 0.00) as QuantidadeCamara, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'2\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = \'201\' || SubString(Clientes.Cod_Orgao From 4 For 3))), 0.00)  as QuantidadePrefeitura, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) in (\'3\',\'4\',\'5\',\'6\') and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = \'201\' || SubString(Clientes.Cod_Orgao From 4 For 3))), 0.00) as QuantidadeIndiretas, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'7\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and Exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = Clientes.Cod_Orgao)), 0.00) as QuantidadeIndependentes ' + \
                      'From ' + \
                      '  RDB$DATABASE ' + \
                      'Union All ' + \
                      'Select ' + \
                      '  3 as Id, ' + \
                      '  \'Entidades Pendentes\' as Descricao, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'1\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and not Exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = Clientes.Cod_Orgao)), 0.00) as QuantidadeCamara, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'2\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and not Exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = Clientes.Cod_Orgao)), 0.00) as QuantidadePrefeitura, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) in (\'3\',\'4\',\'5\',\'6\') and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and not exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = \'201\' || SubString(Clientes.Cod_Orgao From 4 For 3))), 0.00) as QuantidadeIndiretas, ' + \
                      '  Coalesce((Select Count(Clientes.Cod_Orgao) From Clientes Where SubString(Clientes.Cod_Orgao From 1 For 1) = \'7\' and Clientes.Sistema = ' + sistemas[sistema][0] + ' and Clientes.UF = \'PB\' and not Exists(Select 1 From Balancetes Where Balancetes.Competencia = \'' + competencia + '\' and Balancetes.Cod_Orgao = Clientes.Cod_Orgao)), 0.00) as QuantidadeIndependentes ' + \
                      'From ' + \
                      '  RDB$DATABASE '

                cursor.execute(sql)
                dadosGrafico = cursor.fetchall()

            except firebirdsql.Error as err:
                logger.info(f"Erro ao executar consulta ao banco de dados (Sistema: {sistemas[sistema][0]} - {sistemas[sistema][1]}): {err}")

            balancetesEntregues = []
            balancetesPendentes = []
            contador = 0
            qtdEntregas = 0
            qtdPendentes = 0

            for linha in dadosGrafico:

                if contador == 1:
                    qtdEntregas = linha[2] + linha[3] + linha[4] + linha[5]  # Guarda a quantidade total de balancetes recebidos
                    balancetesEntregues.append(linha[3])  # Câmara 2
                    balancetesEntregues.append(linha[2])  # Prefeito 3
                    balancetesEntregues.append(linha[5])  # Indiretas 4
                    balancetesEntregues.append(linha[4])  # Independentes 5
                else:
                    if contador == 2:
                        qtdPendentes = linha[2] + linha[3] + linha[4] + linha[
                            5]  # Guarda a quantidade total de balancetes pendentes
                        balancetesPendentes.append(linha[3])
                        balancetesPendentes.append(linha[2])
                        balancetesPendentes.append(linha[5])
                        balancetesPendentes.append(linha[4])

                contador = contador + 1

            # Definir a largura das barras e as posições
            bar_largura = 0.35
            posicoes = np.arange(len(categorias))

            # Criar o gráfico
            fig, ax = plt.subplots()
            barra1 = ax.bar(posicoes - bar_largura / 2, balancetesEntregues, bar_largura, label='Entregues', color='green')

            for barra in barra1:
                altura = int(barra.get_height())
                plt.annotate(f'{altura}',  # Texto a ser exibido
                             xy=(barra.get_x() + barra.get_width() / 2, altura),  # Posição (x, y)
                             ha='center',  # Alinhamento horizontal: centralizado
                             va='bottom')  # Alinhamento vertical: abaixo do ponto xy

            barra2 = ax.bar(posicoes + bar_largura / 2, balancetesPendentes, bar_largura, label='Pendentes', color='red')

            for barra in barra2:
                altura = int(barra.get_height())
                plt.annotate(f'{altura}',  # Texto a ser exibido
                             xy=(barra.get_x() + barra.get_width() / 2, altura),  # Posição (x, y)
                             ha='center',  # Alinhamento horizontal: centralizado
                             va='bottom')  # Alinhamento vertical: abaixo do ponto xy

            # Adicionar rótulos, título e legenda
            ax.set_ylabel('Quantidade de entes')

            percPendente = (qtdPendentes / (qtdPendentes + qtdEntregas)) * 100
            percEntregue = 100 - percPendente

            ax.set_title('[ CLIENTES PB - ' + sistemas[sistema][1] + ' - Envio dos balancetes: ' + \
                         competencia[0:2] + '/' + \
                         competencia[-4:] + ' ]' + \
                         '\nPendente: ' + f'{percPendente:.2f}' + '% / Enviado: ' + f'{percEntregue:.2f}' + '%')

            ax.set_xticks(posicoes)
            ax.set_xticklabels(categorias)
            ax.legend()

            # Exibir o gráfico
            plt.tight_layout()  # Ajusta o layout para evitar sobreposição
            # plt.show()
            plt.savefig('/opt/airflow/data/GraficoSistema' + sistemas[sistema][0] + '_' + competencia + '.png', dpi=300)

    except Exception as e:
        logger.error(f"Erro ao obter dados do Firebird: {e}")
        raise
    finally:
        # Encerrando a conexão com o Firebird
        cursor.close()
        conexaoFDB.close()
        logger.info("Conexão com o Firebird encerrada.")
