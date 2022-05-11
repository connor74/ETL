import datetime

import pyodbc
import requests
import json
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pathlib import Path
from clickhouse_driver import Client


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
        self._con_url = URL.create("mssql+pyodbc", query={"odbc_connect": self._con_string})
        self._engine = create_engine(self._con_url)
        self._pyodbc = pyodbc.connect(self._con_string)  # pyodbc без sqlalchemy

    def read_query_to_df(self, table: str, params: str) -> DataFrame:
        """
        param table: Наименование таблицы
        param params: Параметры запроса
        return: Возвращает DataFrame
        """
        sql = read_sql_file(table)
        for chunk in pd.read_sql_query(sql, self._engine, params=[params], chunksize=5000, coerce_float=False):
            yield chunk

    def read_raw(self, table: str, param: str):
        """
        :param table: Наименование таблицы
        :param param: Дата для выборки
        :return: Генератор запроса в БД
        """
        sql_query = read_sql_file(table)
        with self._pyodbc.cursor().execute(sql_query, param) as conn:
            while True:
                res = conn.fetchmany(5000)
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

    def _select_table_column_types(self, table_name):
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

    def __select(self, query):
        res = requests.post(self._host, auth=(self._user, self._password), data=query)
        if res.status_code == 200:
            return res.text
        else:
            raise ValueError(res.text)

    def select_last_date(self, table: str, date_column_name: str = "date_balance") -> datetime:
        """
        :param table: Наименование таблицы (без наименования БД)
        :param date_column_name: Наименование колонки для проверки максимальной даты
        :return: Последняя (максимальная дата) в таблице
        """
        query = f"SELECT MAX({date_column_name}) FROM {self._db_name}.{table}"
        return self._select(query)[0][0]

    def insert(self, table, data) -> int:
        query = f"INSERT INTO main.{table} VALUES "
        return self.client.execute(query, data)
