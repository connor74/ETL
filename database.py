import urllib
import datetime
import pandas as pd
import json
import numpy as np

from sqlalchemy import create_engine
from clickhouse_driver import Client


class CH:
    def __init__(self, db_name: str = "main"):
        with open('config.json', 'r') as config:
            config_params = json.load(config)["CLICKHOUSE"]
        self.db_name = db_name
        self.client = Client(
            host=config_params['HOST'],
            user=config_params['USER'],
            password=config_params['PASSWORD'],
            database=self.db_name
        )
        self.yesterday = datetime.date.today() - datetime.timedelta(1)

    def max_date(self, table: str, fileld: str = "date_balance", filter=None):
        '''
        :param table: Наименование таблицы в ClickHouse
        :param fileld: Поле, по которуому вычисляется максимальное значение
        :param filter: Словарь с параметрами в WHERE (пока только через and)
        :return: Возвращает дату "YYYY-MM-DD"
        '''
        if not filter:
            return self.client.execute(f"SELECT MAX({fileld}) FROM {self.db_name}.{table}")[0][0]
        else:
            return self.client.execute(f"SELECT MAX({fileld}) FROM {self.db_name}.{table} WHERE {filter}")[0][0]

    def execute(self, query, data=None, columnar=False, types_check=False):
        return self.client.execute(query, params=data, columnar=columnar, types_check=types_check)

    def insert(self, table, data_dict, types_check=True):
        return self.client.execute(f"INSERT INTO {self.db_name}.{table}  VALUES", data_dict, types_check=types_check)


    def delete_rows_by_date(self, table, date):
        """
        :param table: Таблица для удаления данных без указания наименования базы
        :param date: Поле даты для определения вида {'date_balance': '2000-01-01'}
        :return: Количество удаленных строк
        """
        self.client.execute(
            f"ALTER TABLE {self.db_name}.{table} DELETE WHERE {list(date.keys())[0]}=toDate('{list(date.values())[0]}')")

    def _column_types(self, table):
        return dict(self.client.execute(
                    f"SELECT name, type FROM system.columns WHERE and(database='{self.db_name}', table = '{table}')"
                    )
                )

    def insert_from_db(self, table, data, types_check=True):
        types = self._column_types(table)
        data_types = data.dtypes
        for key, value in types.items():
            if value == 'UInt64' and data_types[key] == 'float64':
                data[key] = data[key].astype('int64')
        return self.client.execute(f'INSERT INTO {self.db_name}.{table} VALUES', data.to_dict(orient='records'))


class MSSQL:
    def __init__(self):
        self.mssql_connection_params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SQL;DATABASE=ComplexBank;UID=URALPROM\\Kurzanovav;Trusted_Connection=yes"
        )

        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.mssql_connection_params)

    def select_to_ch(self, query, params, date_columns, table: str, ins_object, chunksize=None):
        if not isinstance(params, list):
            params = [params]
        if not chunksize:
            data = pd.read_sql(query, con=self.engine, parse_dates=date_columns, params=params)
            print(f"Дата: {params[0].strftime('%d.%m.%Y')} | Добавлено {ins_object.insert_from_db(table, data)} строк")
        else:
            for chunk in pd.read_sql(query, con=self.engine, parse_dates=date_columns, params=params,
                                     chunksize=chunksize):
                print("Прочитано с MSSQL")
                print(
                    f"Дата: {params[0].strftime('%d.%m.%Y')} | Таблица: {table} | Добавлено {ins_object.insert_from_db(table, chunk)} строк")
