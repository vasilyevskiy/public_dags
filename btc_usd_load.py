from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import requests
from sqlalchemy import create_engine, Table, Column, Date, String, Float, MetaData
from datetime import timedelta, datetime

dag_id = "btc_usd_dag"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2022, 1, 1, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

pg_engine = create_engine(BaseHook.get_connection("my_postgres").get_uri())  # get connection from internal db
base = Variable.get("base_cur")  # currencies as variables, can be changed if needed
tgt = Variable.get("target_cur")
url = "https://api.exchangerate.host/{}?base={}&symbols={}"  # historical api

# create table if not exists
metadata_obj = MetaData()
tgt_table = Table('btc_usd_cur', metadata_obj,
    Column('pair', String),
    Column('date', Date),
    Column('value', Float),)
metadata_obj.create_all(pg_engine)


def get_data(dt) -> dict:
    response = requests.get(url.format(dt, base, tgt))
    raw_data = response.json()
    res = raw_data['rates']
    res['pair'] = 'BTC/USD'
    res['date'] = raw_data['date']
    res['value'] = res.pop('USD')
    return res


def load_data_to_db(current_date) -> None:
    data = get_data(current_date)  # pass dag's execution date as parameter to function
    ins = tgt_table.insert()
    pg_engine.execute(ins, data)


with DAG(
        dag_id, default_args=default_args, schedule_interval="0 */3 * * *", catchup=True
) as dag:
    get_and_load_data_to_db = PythonOperator(
        task_id="load_data_to_db",
        python_callable=load_data_to_db,
        dag=dag,
        op_kwargs={
            'current_date': '{{ ds }}'  # using Jinja macro
        },
        catchup=True
    )


    get_and_load_data_to_db