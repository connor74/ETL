from db import Clickhouse, MSSQL
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


a = MSSQL(config)
result = a.read_raw("balance", "2022-04-29")
d = []
for i in result:
    d.append(i)

ch = Clickhouse(config)
ch.insert(d)



def main():
    pass
