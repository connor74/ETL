import os
import datetime
import requests
import petl as etl
import openpyxl
import pandas as pd
import numpy as np

class MOEXapi:
    def __init__(self, ch):
        self.ch_client = ch
        self.sec_id_list = self.ch_client.execute(
            f"""SELECT DISTINCT SecurityId 
                FROM main.moex_deals 
                WHERE PriceType = 'PERC' AND BoardId != 'BCS'
                ORDER BY SecurityId FORMAT JSONAsString""",
            columnar=True
        )[0]
        self.ch_client.execute("""
        CREATE TABLE IF NOT EXISTS  main.bonds_history (
            trade_date Date,
            sec_id String,
            short_name String,
            mat_date Nullable(Date),
            offer_date Nullable(Date),
            yield_close Nullable(Decimal64(2)),
            market_price_3  Nullable(Decimal64(2)),
            face_value  Nullable(Decimal64(2)),
            face_unit String,
            coupon_value Nullable(Decimal64(2)),
            accint Nullable(Decimal64(2)),
            coupon_rate Nullable(Decimal64(2)),
            num_trades UInt64,
            yield_to_ofer Nullable(Decimal64(4)),
            volume  UInt64
            )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(trade_date)
        ORDER BY (trade_date)
        """
        )

        self.keys = [
            'trade_date',
            'sec_id',
            'short_name',
            'mat_date',
            'offer_date',
            'yield_close',
            'market_price_3',
            'face_value',
            'face_unit',
            'coupon_value',
            'accint',
            'coupon_rate',
            'num_trades',
            'yield_to_ofer',
            'volume'
        ]
        self.column_dates = [
            'trade_date',
            'mat_date',
            'offer_date'
        ]

    def get_bonds_data(self):
        last_date = self.ch_client.max_date("bonds_history", "trade_date")
        yesterday = datetime.date.today() - datetime.timedelta(1)
        if last_date >= yesterday:
            print("Данные с биржи акутальны")
        else:
            date_range_all = pd.date_range(start=last_date - datetime.timedelta(-1), end=yesterday)
            for date in date_range_all:
                data = []
                for sec_id in self.sec_id_list:
                    params = {
                        "iss.only": "history",
                        "iss.meta": "off",
                        "history.columns": "TRADEDATE,SECID,SHORTNAME,MATDATE,OFFERDATE,YIELDCLOSE,MARKETPRICE3,FACEVALUE,FACEUNIT,COUPONVALUE,ACCINT,COUPONPERCENT,NUMTRADES,YIELDTOOFFER,VOLUME",
                        "from": date.strftime('%Y-%m-%d'),
                        "till": date.strftime('%Y-%m-%d'),
                    }
                    link = f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{sec_id}.json"
                    resp = requests.get(link, params)
                    if resp.json()['history']['data']:
                        values = resp.json()['history']['data'][0]
                        data_dict = dict(zip(self.keys, values))
                        for item in self.column_dates:
                            if data_dict[item]:
                                data_dict[item] = datetime.datetime.strptime(data_dict[item], '%Y-%m-%d')
                        data.append(data_dict)
                if data:
                    rows = self.ch_client.insert('bonds_history', data)
                    print(f"Добавлено {rows} строк")
        






