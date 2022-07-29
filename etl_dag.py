import pandas as pd


from datetime import datetime, timedelta
from airflow import DAG

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


yesterday = datetime.today() - timedelta(days=1)

def str_to_date(date: str) -> datetime:
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    return date


def date_to_str(date: datetime, format: str = "%Y-%m-%d") -> str:
    if not isinstance(date, str):
        date = date.strftime(format)
    return date


default_args = {
    "depends_on_past": False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'is_paused_upon_creation': False,
}


def mssql_balance_select(dag_run, ti):

    yesterday = dag_run.logical_date - timedelta(days=1)
    yesterday_str = date_to_str(yesterday)
    yesterday_nodash = date_to_str(yesterday).replace('-', '')
    count_rows = ti.xcom_pull(key=f"count_staging_balance_{yesterday_str}")
    if int(count_rows) == 0:
        select_mssql_balance_query = f"""
                SELECT
                    CAST([Date] AS Date) AS date_balance,
                    IDAccount AS id_account,
                    Nat AS sum_national,
                    Cur AS sum_currency
                FROM tla_Bal_Ex 
                WHERE [Date] = '{yesterday_str}';
                """
        yesterday_str.replace("-", "")
        hook = MsSqlHook()
        df = hook.get_hook(conn_id='mssql_conn').get_pandas_df(select_mssql_balance_query)
        df.to_csv(f"tmp/balance_{yesterday_nodash}.csv", index=False)




def pg_select_count(schema, table, date_field, dag_run, ti):
    yesterday = dag_run.logical_date - timedelta(days=1)
    yesterday_str = date_to_str(yesterday)

    sql = f"""
        SELECT COUNT(*) FROM {schema}.{table} WHERE {date_field}='{yesterday_str}';
    """
    pg_hook = PostgresHook(postgres_conn_id="pg_conn", schema="pgdata")
    pg_conn = pg_hook.get_conn()
    pg_curr = pg_conn.cursor()
    pg_curr.execute(sql)
    count = pg_curr.fetchone()[0]
    ti.xcom_push(key=f"count_{schema}_{table}_{yesterday_str}", value=count)



def pg_balance_insert(schema, table, dag_run, ti):

    yesterday = dag_run.logical_date - timedelta(days=1)
    yesterday_str = date_to_str(yesterday)
    count_rows = ti.xcom_pull(key=f"count_staging_balance_{yesterday_str}")
    if int(count_rows) == 0:
        yesterday_nodash = date_to_str(yesterday).replace('-', '')
        csv_file_name = f"tmp/balance_{yesterday_nodash}.csv"
        pg_hook = PostgresHook(postgres_conn_id="pg_conn", schema="pgdata")
        pg_conn = pg_hook.get_conn()
        pg_curr = pg_conn.cursor()
        sql = f"COPY {schema}.{table} FROM STDIN DELIMITER ',' CSV HEADER;"
        pg_curr.copy_expert(sql, open(csv_file_name, "r"))
        pg_conn.commit()
        pg_curr.close()
        pg_conn.close()


with DAG(
    "etl_balance",
    default_args=default_args,
    start_date=datetime(2022, 1, 2),
    end_date=datetime.today() - timedelta(days=-1),
    schedule_interval="45 3 * * *",
    catchup=True,
    template_searchpath="/tmp"
) as dag:

    count_rows_balance = PythonOperator(
        task_id="count_rows_balance",
        python_callable=pg_select_count,
        op_kwargs={
            "schema": "staging",
            "table": "balance",
            "date_field": "date_balance"
        }
    )


    mssql_balance_select = PythonOperator(
        task_id="mssql_balance_select",
        python_callable=mssql_balance_select,

    )

    pg_balance_insert = PythonOperator(
        task_id="pg_balance_insert",
        python_callable=pg_balance_insert,
        op_kwargs={
            "schema": "staging",
            "table": "balance",
        }
    )


count_rows_balance >> mssql_balance_select >> pg_balance_insert