from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.operators.python_operator import PythonOperator
from dataplatform.operators.spark.docker_spark_submit_operator import DockerSparkSubmitOperator
from airflow.hooks.base_hook import BaseHook

CENSO_FILE = "microdados_censo_escolar_2020"

default_args = {
    "retries": 4,
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 20),
    "retry_delay": timedelta(seconds=10),
}


def load_file(destination, source, conn_id='hdfs_http', overwrite=True):
    hook = WebHDFSHook(conn_id)
    hook.load_file(source, destination, overwrite)

def hdfs_conn(conn_id='hdfs'):
    conn = BaseHook.get_connection(conn_id)
    return f"hdfs://{conn.host}:{conn.port}"

with DAG(
    dag_id="censo_escolar",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
) as dag:
    download_file = BashOperator(
        task_id="download_file",
        bash_command=f"curl https://download.inep.gov.br/dados_abertos/{CENSO_FILE}.zip -o /tmp/{CENSO_FILE}.zip"
    )

    unzip_file = BashOperator(
        task_id="unzip_file",
        bash_command=f"unzip -o /tmp/{CENSO_FILE}.zip -d /tmp/microdados > /dev/null"
    )

    filter_files = BashOperator(
        task_id="filter_files",
        bash_command="mkdir /tmp/matriculas && mv /tmp/microdados/microdados_educacao_basica_2020/DADOS/matricula_* /tmp/matriculas"
    )

    load_to_hdfs = PythonOperator(
        task_id="load_to_hdfs",
        python_callable=load_file,
        op_kwargs={
            'destination': '/spark/data/matriculas',
            'source': '/tmp/matriculas'
        }
    )

    parse_data = DockerSparkSubmitOperator(
        task_id="parse_data",
        application="/spark/scripts/transform.py",
        conn_id='spark',
        application_args=[
            '--database', 'censo',
            '--table', 'alunos' ,
            '--csv-path', '/spark/data/matriculas',
            '--dwh-table-path', f"{hdfs_conn()}/user/hive/warehouse/censo"
        ]
    )

    download_file >> unzip_file >> filter_files >> load_to_hdfs >> parse_data
