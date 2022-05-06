from db import Clickhouse, MSSQL
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


a = MSSQL(config)
result = a.read_raw("balance", "2022-04-29")
for i in result:
    print(i)


def main():
    pass
