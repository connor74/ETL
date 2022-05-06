import requests
import json
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pathlib import Path


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

    def read_query_to_df(self, sql: str, params: str) -> DataFrame:
        """
        param sql: Текст SQL запроса с параметрами
        param params: Параметры запроса
        return: Возвращает DataFrame
        """
        return pd.read_sql_query(sql, self._engine, params=[params])

    def read_raw(self, table: str, param: str):
        """
        :param table: Наименование таблицы
        :param param: Дата для выборки
        :return: Генератор запроса в БД
        """
        with self._engine.connect() as conn:
            sql_query = read_sql_file(table)
            for item in conn.execute(sql_query, param).fetchall():
                yield item


class Clickhouse:
    def __init__(self, config, db_name="main"):
        self._config = config[self.__class__.__name__.lower()]
        self.db_name = db_name
        self._host = f"http://{self._config['host']}:8123/"
        self._user = self._config['user']
        self._password = self._config['password']
        print(self._user, self._password)

    def select(self, query):
        res = requests.post(self._host, auth=(self._user, self._password), data=query)
        if res.status_code == 200:
            return res.text
        else:
            raise ValueError(res.text)

    def insert(self, data):
        query = f"INSERT INTO main.balance VALUES {data}"
        print(query)
        res = requests.post(self._host, auth=(self._user, self._password), data=query)
