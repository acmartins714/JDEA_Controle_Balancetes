FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN playwright install

# Usando o Usuário root para instalar as dependencias do playwright
USER root
RUN playwright install-deps
RUN playwright install webkit
RUN playwright install firefox

# retornoando o comando ao usuário padrão do airflow
USER airflow