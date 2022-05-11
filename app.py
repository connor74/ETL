import pandas as pd

from db import Clickhouse, MSSQL
import configparser
import time
import datetime

config = configparser.ConfigParser()
config.read('config.ini')
yesterday = datetime.date.today() - datetime.timedelta(1)

mssql = MSSQL(config)
ch = Clickhouse(config)


def check_last_date(table):
    return ch.select_last_date(table)


def migrate_by_date(table, date):
    rows_all = 0
    for chunk in mssql.read_raw(table, date):
        rows = ch.insert(table, chunk)
        rows_all += rows
    return rows_all


def migrate_table(table: str, start_date=None, end_date=yesterday):
    if start_date is None:
        last_date = check_last_date(table)
        if end_date > last_date:
            datetime.datetime.strptime("01-12-2021", "%d-%m-%Y")
            date_range = pd.date_range(last_date, end_date)
        else:
            print(f"|-----------------------------------------------------------------------")
            print(f"| {datetime.datetime.now().strftime('%H:%M:%S')} - Обновление таблицы {table} не требуется! ")
            print(f"|-----------------------------------------------------------------------")
            return None
    else:
        date_range = pd.date_range(start_date, end_date)
    for day in date_range:
        rows = migrate_by_date(table, day.strftime("%Y-%m-%d"))
        print(f"|-------------------------------------------------------------------------------------------------")
        print(f"| {datetime.datetime.now().strftime('%H:%M:%S')} - За {day.strftime('%Y-%m-%d')} | Таблица {table} | Добавлено: {rows} строк.")
    print(f"|-------------------------------------------------------------------------------------------------")


def main():
    start_date = datetime.datetime.strptime("2022-05-06", "%Y-%m-%d")
    migrate_table("balance")


main()
