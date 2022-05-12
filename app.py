import time

import pandas as pd
import configparser
import datetime
from db import Clickhouse, MSSQL
from moex_api_ import moex_get


config = configparser.ConfigParser()
config.read('config.ini')
yesterday = datetime.date.today() - datetime.timedelta(1)

mssql = MSSQL(config)
ch = Clickhouse(config)


def check_last_date(table, date_column_name="date_balance"):
    return ch.select_last_date(table, date_column_name)

def migrate_by_date(table, date):
    rows_all = 0
    for chunk in mssql.read_raw(table, date):
        rows = ch.insert(table, chunk)
        rows_all += rows
    return rows_all

def migrate_table(table: str, date_column_name="date_balance", start_date=None, end_date=yesterday):
    if start_date is None:
        last_date = check_last_date(table, date_column_name)
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
    #start_date = datetime.datetime.strptime("2022-05-06", "%Y-%m-%d")
    #migrate_table("balance")
    begin = time.time()
    print(moex_get("2022-05-06"))
    print(time.time() - begin)





main()
