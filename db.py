import datetime
import inspect

import pyodbc
import json
from pathlib import Path
from clickhouse_driver import Client
from colors import Colors


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

    def read_raw(self, table: str, param: str, fetch_size: int = 5000) -> list:
        """
        :param table: Наименование таблицы
        :param param: Дата для выборки
        :param fetch_size: Количество строк в одном батче
        :return: Генератор запроса в БД
        """
        sql_query = read_sql_file(table)
        with self._pyodbc.cursor().execute(sql_query, param) as conn:
            while True:
                res = conn.fetchmany(fetch_size)
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