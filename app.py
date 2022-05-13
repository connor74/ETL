import time

import pandas as pd
import configparser
import datetime
from db import Clickhouse, MSSQL
from moex_api_ import moex_get
from colors import Colors
from typing import List

config = configparser.ConfigParser()
config.read('config.ini')
yesterday = datetime.date.today() - datetime.timedelta(1)

mssql = MSSQL(config)
ch = Clickhouse(config)

START_DATE = "2019-"


def check_last_date(table: str, date_column_name: str) -> datetime:
    return ch.select_last_date(table, date_column_name)


def migrate_by_date(table: str, date: str) -> int:
    """
    :param table: Наименование таблицы в Clickhouse
    :param date: Дата для выборки
    :return: Количество добавленных строк
    """
    rows_all = 0
    for chunk in mssql.read_raw(table, date):
        rows = ch.insert(table, chunk)
        rows_all += rows
    return rows_all


def str_to_date(date: str) -> datetime:
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    return date


def date_to_str(date: datetime, format: str = "%Y-%m-%d") -> str:
    if not isinstance(date, str):
        date = date.strftime(format)
    return date


def migrate_table(table: str, date_column_name: str, end_date: str, start_date: str = None) -> None:
    if end_date:
        end_date = str_to_date(end_date)
    else:
        end_date = yesterday

    if start_date:
        start_date = str_to_date(start_date) - datetime.timedelta(-1)
    else:
        last_date = check_last_date(table, date_column_name)
        if last_date < end_date:
            start_date = last_date - datetime.timedelta(-1)
        else:
            print("-" * 100)
            print(Colors.green,
                  f" {date_to_str(datetime.datetime.now('%H:%M:%S'))} - Обновление таблицы {table} не требуется! ")
            print("-" * 100)
            return None
    print(start_date)
    print(end_date)
    date_range = pd.date_range(start_date, end_date)
    for day in date_range:
        rows = migrate_by_date(table, date_to_str(day))
        print("-" * 100)
        print(Colors.yellow,
              f" {date_to_str(datetime.datetime.now())} - За {date_to_str(day)} | Таблица {table} | Добавлено: {rows} строк.")
    print("-" * 100)


def get_moex_api_data(date_begin: str = None, date_end: str = None) -> None:
    if not date_end:
        date_end = date_to_str(yesterday)
    else:
        pass



def main() -> None:
    # start_date = datetime.datetime.strptime("2022-05-06", "%Y-%m-%d")
    # migrate_table("balance")
    begin = time.time()
    migrate_table("balance", "date_balance", end_date="2022-04-29", start_date="2022-01-01")
    #data = moex_get(yesterday.strftime('%Y-%m-%d'))
    # moex_bonds_history
    #ch.insert("moex_bonds_history", data)
    print(time.time() - begin)


main()
