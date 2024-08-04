import pandas as pd
import os
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def upload_product():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine=pg_hook.get_sqlalchemy_engine()
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', 'product_info.csv')

    df = pd.read_csv(path, encoding='cp1251')
    df = df[df['effective_from_date'] != '2023-03-15']
    df['effective_from_date'] = pd.to_datetime(df['effective_from_date'], format='%Y-%m-%d')
    df['effective_to_date'] = df['effective_to_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df.to_sql('product', engine, schema="rd", if_exists="append", index=False)

def upload_deal():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine=pg_hook.get_sqlalchemy_engine()
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', 'deal_info.csv')

    df = pd.read_csv(path, encoding='cp1251')
    df['effective_from_date'] = pd.to_datetime(df['effective_from_date'], format='%Y-%m-%d')
    df['effective_to_date'] = df['effective_to_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df.to_sql('deal_info', engine, schema="rd", if_exists="append", index=False)

with DAG(
        dag_id='data_from_csv_loan_holiday',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']
) as dag:
    start = DummyOperator(
        task_id="start"
    )
    with TaskGroup('product') as product:
        load_product = PythonOperator(
            task_id='load_product_info',
            python_callable=upload_product,
        )
    with TaskGroup('deal') as deal:
        load_deal = PythonOperator(
            task_id='load_deal_info',
            python_callable=upload_deal,
        )
    with TaskGroup('update_dm') as dm:
        task_1_2 = SQLExecuteQueryOperator(
            task_id='delete_dublicates_update_dm',
            conn_id='postgres-conn',
            sql='script_loan_holiday.sql'
        )

    end = DummyOperator(
        task_id="end"
    )

    start >> [product, deal] >> dm >> end
