import pandas as pd
import os
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def upload_currency():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine=pg_hook.get_sqlalchemy_engine()
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', 'dict_currency.csv')

    df = pd.read_csv(path)
    df['effective_from_date'] = pd.to_datetime(df['effective_from_date'], format='%Y-%m-%d')
    df['effective_to_date'] = df['effective_to_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df.to_sql('dict_currency', engine, schema="dm", if_exists="append", index=False)

with DAG(
        dag_id='update_dm_balance_turnovers',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']
) as dag:
    start = DummyOperator(
        task_id="start"
    )
    with TaskGroup('upload_currency') as currency:
        load_product = PythonOperator(
            task_id='load_product_info',
            python_callable=upload_currency,
        )
    with TaskGroup('truncate_currency') as truncate:
        truncate = SQLExecuteQueryOperator(
            task_id='truncate_table_dict_currency',
            conn_id='postgres-conn',
            sql='truncate table dm.dict_currency'
        )
    with TaskGroup('update_all_tables') as dm:
        update_rd = SQLExecuteQueryOperator(
            task_id='update_rd',
            conn_id='postgres-conn',
            sql='update_account_balance.sql'
        )
        update_dm = SQLExecuteQueryOperator(
            task_id='update_dm',
            conn_id='postgres-conn',
            sql='update_dm_turnover.sql'
        )
        update_rd >> update_dm

    end = DummyOperator(
        task_id="end"
    )

    start >>truncate >> currency >> dm >> end
