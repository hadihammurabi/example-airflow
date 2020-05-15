from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import fns.hello as hello

graph_args = {
  'owner': 'Airflow',
  'depends_on_past': False,
  'start_date': days_ago(2),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
}

graph = DAG(
  dag_id='hello',
  description='Hello DAG',
  default_args=graph_args,
  schedule_interval='*/10 * * * *',
  tags=['example']
)

collector = PythonOperator(
  task_id='collector',
  provide_context=True,
  python_callable=hello.collector,
  dag=graph
)

printer = PythonOperator(
  task_id='printer',
  provide_context=True,
  python_callable=hello.printer,
  dag=graph
)

start = DummyOperator(task_id='start', dag=graph)
finish = DummyOperator(task_id='finish', dag=graph)

start >> collector >> printer >> finish
