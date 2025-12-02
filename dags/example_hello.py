from datetime import datetime

from airflow.models import DAG
from airflow.providers.standard.operators.empty import EmptyOperator


dag = DAG('example_hello', start_date=datetime(2024, 1, 1))
t1 = EmptyOperator(task_id='t1', dag=dag)
