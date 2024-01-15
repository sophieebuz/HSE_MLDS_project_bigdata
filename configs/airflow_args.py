from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

airflow_args = {
    'owner': 'Sophie Buzaeva and Dmitry Kulikov',
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
    'tags': ['bolshoi_theater'],
}
