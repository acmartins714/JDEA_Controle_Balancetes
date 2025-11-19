FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN apt-get update && apt-get upgrade -y
RUN pip install playwright
RUN playwright install
RUN playwright install-deps
RUN playwright webkit
RUN playwright firefox
