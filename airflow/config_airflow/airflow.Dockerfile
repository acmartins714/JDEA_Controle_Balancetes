FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

RUN playwright install
RUN playwright install-deps
RUN playwright webkit
RUN playwright firefox
RUN apt-get update && apt-get upgrade -y