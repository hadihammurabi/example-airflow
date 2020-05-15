from datetime import timedelta, datetime

from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine, Table, Column, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import fns.detikcom_scraper.engine as detikcom_scraper

db = create_engine('mysql://root:root@localhost:3306/airflow', pool_recycle=3600)
Base = declarative_base()

class Detikcom(Base):
  __tablename__ = 'detikcom_index'
  id = Column(Integer, primary_key=True) 
  title = Column(String(255), nullable=False) 
  url = Column(String(255), nullable=False) 
  used = Column(Boolean, default=False)

Base.metadata.create_all(bind=db)

graph_args = {
  'owner': 'Airflow',
  'depends_on_past': False,
  'start_date': days_ago(0),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
}

graph = DAG(
  dag_id='detikcom_index',
  description='detik.com index scraper',
  default_args=graph_args,
  schedule_interval='*/30 * * * *',
  tags=['example', 'scraper']
)

def collect_callable(ds, **context):
  page = Variable.get('detikcom_index_page')
  page = int(page)

  if page > 24:
    page = 1

  print('PAGE: {}'.format(page))
  indexes = detikcom_scraper.get_and_parse_index(date=datetime.today().strftime('%m/%d/%Y'), page=page)
  Variable.set('detikcom_index_page', page + 1)
  return indexes

collect = PythonOperator(
  task_id='collect',
  provide_context=True,
  python_callable=collect_callable,
  dag=graph
)

def save_callable(ds, **context):
  datas = context['task_instance'].xcom_pull(task_ids='collect')
  Session = sessionmaker()
  Session.configure(bind=db)
  session = Session()
  for data in datas:
    news = Detikcom(title=data['title'], url=data['url'])
    session.add(news)
  session.commit()

save = PythonOperator(
  task_id='save',
  provide_context=True,
  python_callable=save_callable,
  dag=graph
)

collect >> save
