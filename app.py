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


def migrate_plan(start: str, end):
    pass




def main():
    print(check_last_date("balance"))

main()
