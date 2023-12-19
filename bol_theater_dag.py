import sys
sys.path.append("/opt/hadoop/airflow/dags/bol_theater/HSE_MLDS_project_bigdata")
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from parsing import parsing


DEFAULT_ARGS = {
    "owner": "Sophie Buzaeva",
    "email_on_failure": False,
    "email_on_retry": False,
    "retry": 3,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="bol_theater",
    schedule_interval="0 * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["bigdata_project"],
    default_args=DEFAULT_ARGS
)

task_get_data = PythonOperator(task_id="get_data", python_callable=parsing.get_data, dag=dag)

task_get_data