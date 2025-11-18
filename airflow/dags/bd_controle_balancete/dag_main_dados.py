from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from bd_controle_balancete.task.extracao_bd_full import excel_to_minio_etl_parquet_full

# Argumentos iniciais.
default_args = {
    'owner': 'Alexandre',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# Parametro para dags
@dag(
    dag_id='dag_main_tcepbElmar_dados',
    default_args=default_args,
    description='Dag curso JDEA - TCE-PB / Elmar',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['TCE-PB', 'extração', 'transformação', 'etl', 'raw'],
)

def main_dag():
    plan_name = 'Balancetes_Mensais.xls'  # Caminho para a planilha do TCE-PB
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    bucket_bronze = 'raw' # Bucket Bronze

    # TaskGroup: Extração dos dados da planilha foenecida pelo TCE-PB após raspagem de dados
    with TaskGroup("group_task_parquet_full", tooltip="Tasks processadas do excel para minio, salvando em .parquet") as group_task_parquet_full:
        PythonOperator(
            task_id='task_parquet_full_balancetes',
            python_callable=excel_to_minio_etl_parquet_full,
            op_args=[plan_name, bucket_bronze, endpoint_url, access_key, secret_key]
        )
    # Fluxo de dependência
    group_task_parquet_full

main_dag_instance = main_dag()