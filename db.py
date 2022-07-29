import datetime
import inspect
import urllib
from typing import Dict

import psycopg2
import psycopg2.extras as extras



import pyodbc
import json
from pathlib import Path
from clickhouse_driver import Client
from colors import Colors
from sqlalchemy import create_engine
# ssh admair@uralprom.local@192.168.0.23
# Q2XBo>.,4fR8

def read_sql_file(file_name: str, folder: str = "sql", file_type: str = "sql") -> str:
    '''
    Считывает текст SQL запрос из файла

    :param file_name: Имя SQL файла без расширения
    :param folder: Папка расположения SQL файла
    :param file_type: Расширение файла
    :return: Текст SQL запроса
    '''
    with open(Path(Path.cwd(), folder, file_name + "." + file_type), 'r', encoding='utf8') as f:
        result = f.read()
    return result


class MSSQL():
    def __init__(self, config) -> None:
        self._params = config[self.__class__.__name__.lower()]
        self._con_string = f"DRIVER={self._params['driver']};SERVER={self._params['server']};DATABASE={self._params['db']};uid={self._params['uid']};Trusted_Connection=yes"
        self._pyodbc = pyodbc.connect(self._con_string)  # pyodbc без sqlalchemy
        self.mssql_connection_params = urllib.parse.quote_plus(self._con_string)
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.mssql_connection_params)


    def read_raw(self, table: str, param: str, pg: bool = False, fetch_size: int = 5000) -> list:
        """
        :param table: Наименование таблицы
        :param param: Дата для выборки
        :param fetch_size: Количество строк в одном батче
        :return: Генератор запроса в БД
        """
        if pg:
            sql_query = read_sql_file(table+'_pg')
        else:
            sql_query = read_sql_file(table)
        with self._pyodbc.cursor().execute(sql_query, param) as conn:
            while True:
                res = conn.fetchmany(fetch_size)
                if res:
                    yield res
                else:
                    break

    def read_df(self, table: str, param: str, pd, pg: bool = False, chunksize: int = 5000):
        if pg:
            sql_query = read_sql_file(table+'_pg')
        else:
            sql_query = read_sql_file(table)
        while True:
            res = pd.read_sql(sql_query, con=self.engine, params = [param], chunksize=chunksize)
            if res:
                yield res
            else:
                break



class Clickhouse:
    def __init__(self, config, db_name="main") -> None:
        self._config = config[self.__class__.__name__.lower()]
        self._user = self._config['user']
        self._password = self._config['password']
        self._db_name = db_name
        self.client = Client(
            host=self._config['host'],
            user=self._config['user'],
            password=self._config['password'],
            database=self._db_name
        )

    def _select_table_column_types(self, table_name) -> dict:
        query = f"""SELECT column_name, data_type FROM information_schema.columns c 
                 WHERE table_name = '{table_name}' FORMAT JSONCompact"""
        data = json.loads(self.select(query))['data']
        return dict(data)

    def _select(self, query):
        """
        :param query: SQL запрос
        :return: Данные
        """
        return self.client.execute(query)

    def select_last_date(self, table: str, date_column_name: str = "date_balance") -> datetime:
        """
        :param table: Наименование таблицы (без наименования БД)
        :param date_column_name: Наименование колонки для проверки максимальной даты
        :return: Последняя (максимальная дата) в таблице
        """
        query = f"SELECT MAX({date_column_name}) FROM {self._db_name}.{table}"
        return self._select(query)[0][0]

    def insert(self, table, data) -> int:
        """
        :param table: Наименование таблицы (без наименования БД)
        :param data: Даттые для вставки
        :return: Возвращает количество строк вставленных в таблицу
        """
        query = f"INSERT INTO {self._db_name}.{table} VALUES "
        rows = None
        try:
            rows = self.client.execute(query, data)
        except AttributeError as e:
            print(Colors.red,
                  f"Ошибка: Class: {self.__class__.__name__}, функия: {inspect.currentframe().f_code.co_name}", e.name,
                  e.obj, e.args)
        return rows

    def check_moex_report(self, doc_num: str):
        """
        Проверка. Внесены ли в базу отчеты биржи с номером отчета 'doc_num'
        :param doc_num: Номер отчета биржи
        :return: True/False
        """
        query = f"SELECT COUNT(*) FROM {self._db_name}.moex_deals WHERE doc_num = '{doc_num}'"
        if self.client.execute(query)[0][0]:
            return True
        return False

class Postgree:
    def __init__(self, config, schema: str):
        self.conn = psycopg2.connect(
            database=config['pg']['db'],
            user=config['pg']['user'],
            password=config['pg']['password'],
            host=config['pg']['host'],
            port=5432
        )
        self.cur = self.conn.cursor()
        self.schema = schema

    q_balance = """
        INERT INTO staging.balance VALUES (%s,%s,%s,%s)
    """
    def execute(self, query):
        self.cur.execute(query)

    def select_max_date(self, table, date_column):
        self.cur.execute(f"SELECT MAX({date_column}) FROM {self.schema}.{table}")
        return self.cur.fetchone()[0]

    def insert(self, data, table):
        query = f"INSERT INTO {self.schema}.{table} VALUES %s"
        #extras.execute_values(self.cur, query, data)
        self.cur.executemany(query, data)
        count = self.cur.rowcount
        self.conn.commit()
        return count

    def delete(self, table, params):
        query = f"DELETE FROM {self.schema}.{table} WHERE %s"

    def truncate_table(self, table: str):
        self.execute(f"TRUNCATE {self.schema}.{table};")
        self.conn.commit()

    def delete_rows_by_date(self, table, date_column, date):
        self.execute(f"DELETE FROM {self.schema}.{table} WHERE {date_column} = '{date}';")



    def close(self):
        self.cur.close()
        self.conn.close()









