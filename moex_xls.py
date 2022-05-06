import datetime
import pandas as pd

class MOEXxls:
    def __init__(self):
        self.path = "N:\\KurzanovAV\\Общее\\moex\\"


    def read_xlsx(self, ch, file_name="expiration"):
        file = {
            "expiration": "expiration.xlsx",
            "portfolio": "portfolio.xlsx"
        }
        last_date = ch.max_date("moex_deals", "trade_date_time", "BuySell IN ('A', 'E')")
        if last_date is None:
            last_date = datetime.date(1970, 1, 1)
        data = pd.read_excel(
            self.path + file[file_name],
            dtype={
                'Price': float,
                'Quantity': int,
                'Value': float,
                'AccInt': float,
                'Amount': float,
                'Price2': float,
                'RepoPart': int,
                'RepoRate': float,
                'ExchComm': float,
                'ClrComm': float,
                'ClientCode': str,
                'FaceAmount': float
            }
        )
        data['ClientCode'].fillna("", inplace=True)
        df = data[data["trade_date_time"].dt.date > last_date.date()]
        if df.empty:
            print(f"Данные по погашению и оферте в акуальном состоянии")
        else:
            rows = ch.insert("moex_deals", df.to_dict(orient='records'))
            print(f"Добавлено {rows} строк из XLSX файла (погашения и оферты)")



