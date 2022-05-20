import datetime
import pandas as pd
from moex_xls import MOEXxls
from database import CH, MSSQL
import time

tables = [
    {
        "table": "transfer",
        "table_param": ["date_transfer", ],
        "dates_columns": {
            'date_transfer': {'format': "%Y-%m-%d %H:%M:%S"},
        },
    },
    {
        "table": "terminals",
        "table_param": ["date_balance", ],
        "dates_columns": {
            'date_balance': {'format': "%Y-%m-%d"},
            'date_open': {'format': "%Y-%m-%d"},
            'date_close': {'format': "%Y-%m-%d"},
            'time_create': {'format': "%Y-%m-%d %H:%M:%S"},
        },
    },
    {
        "table": "balance_1",
        "table_param": ["date_balance", ],
        "dates_columns": {
            'date_balance': {'format': "%Y-%m-%d"},
            'date_open': {'format': "%Y-%m-%d"},
            'date_close': {'format': "%Y-%m-%d"},
            'date_dep_end': {'format': "%Y-%m-%d"},
            'ac_time_change': {'format': "%Y-%m-%d %H:%M:%S"},
            'time_create': {'format': "%Y-%m-%d %H:%M:%S"},
        },
    },

]


def check_last_update() -> bool:
    now = datetime.datetime.today().replace(microsecond=0)
    check_time = now.replace(hour=11, minute=1)
    if now > check_time:
        print("----------------------------------------------------")
        print("Обновление после 11:30")
        print("----------------------------------------------------")
        return True
    return False


def update_from_db(table, field, dates_columns, ch, date_begin=None):
    mssql = MSSQL()
    yesterday = datetime.date.today() - datetime.timedelta(1)
    max_date = ch.max_date(table, field)
    if max_date < yesterday:
        with open("sql\\" + table + ".sql", 'r', encoding='utf8') as f:
            query = f.read()
            if not date_begin:
                last_date = max_date - datetime.timedelta(-1)
            else:
                last_date = datetime.datetime.strptime(date_begin, '%Y-%m-%d')
            date_range = pd.date_range(start=last_date, end=yesterday)
            for date in date_range:
                mssql.select_to_ch(query, date, dates_columns, table, ch, chunksize=10000)
    else:
        print(f"Таблица {table} с актуальными данными")


if __name__ == '__main__':
    ch = CH()
    if check_last_update():
        print("----------------------------------------------------")
        print("Удаление более ранних данных, закачанных до 11:30")
        tb = ['balance_1', 'terminals']
        today_str = datetime.datetime.today().strftime("%Y-%m-%d")

        for item in tb:
            sql = f"""
                SELECT 
                    DISTINCT formatDateTime(date_balance, '%Y%m%d')
                FROM
                    main.{item}
                WHERE
                    toDate(time_create) = '{today_str}'
            """
            dates = ch.execute(sql)
            print("----------------------------------------------------")
            print(f"Удаление данных из таблицы - {item}")
            for date in dates:
                ch.execute(f"ALTER TABLE main.{item} DROP PARTITION '{date[0]}'")
            print("\n\n")
        time.sleep(20)
    for item in tables:
        update_from_db(item['table'], item['table_param'][0], item['dates_columns'], ch)


    moex_xls = MOEXxls()
    moex_xls.read_xlsx(ch)

