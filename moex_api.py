from datetime import datetime

import requests

url = "http://iss.moex.com/iss/"
path_history = "history/engines/stock/markets/bonds/"


# http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json?date=2022-03-25&iss.meta=off&history.columns=BOARDID,TRADEDATE,SECID,SHORTNAME,MATDATE,OFFERDATE,YIELDCLOSE,MARKETPRICE3,FACEVALUE,FACEUNIT,COUPONVALUE,ACCINT,COUPONPERCENT,NUMTRADES,YIELDTOOFFER,VOLUME&from=2022-03-25&till=2022-03-25


class MOEX_api:
    def __init__(self, date: str) -> None:
        self._pages = None
        self._data = None
        self._columns = None
        self._status_code = None
        self._resp = None
        self._date = date
        self._req_columns = [
            'BOARDID',
            'TRADEDATE',
            'SECID',
            'SHORTNAME',
            'MATDATE',
            'OFFERDATE',
            'MARKETPRICE3',
            'FACEVALUE',
            'FACEUNIT',
            'COUPONVALUE',
            'ACCINT',
            'COUPONPERCENT',
            'NUMTRADES',
            'YIELDATWAP',
            'YIELDCLOSE',
            'YIELDTOOFFER',
            'VOLUME',
        ]
        self._date_columns = [
            'TRADEDATE',
            'MATDATE',
            'OFFERDATE'
        ]

        self._link = url + path_history
        self._params = {
            'date': self._date,
            'iss.meta': 'off',
            'history.columns': ','.join(self._req_columns),
        }

    def get_data(self, num: int) -> None:
        self._params['start'] = num
        try:
            self._resp = requests.get(f'{self._link}securities.json', params=self._params)
            self._status_code = self._resp.status_code
        except requests.exceptions.HTTPError as err:
            print(f"Статус: {self._status_code} | {err}")
        except requests.exceptions.Timeout as err:
            print(err)
        if self._status_code == 200:
            self._columns = self._resp.json()['history']['columns']
            self._data = self._resp.json()['history']['data']
        else:
            return None

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def columns(self) -> list:
        return self._columns

    @property
    def data(self):
        return self._data

    @property
    def date_columns(self) -> list:
        return self._date_columns

def moex_get(date: str) -> list:
    moex = MOEX_api(date)
    # API выдает по 100 записей, необходимо пройти в цикле по всем страницам
    # Отсчет идет от номера первой записи на странице
    num = 1
    all_data = []
    while True:
        moex.get_data(num)
        if moex.status_code:
            if moex.data:
                if num == 1:
                    columns = [cols.lower() for cols in moex.columns]
                flat_list = []
                for item in moex.data:
                    data_dict = dict(zip(columns, item))
                    for date in moex.date_columns:
                        if data_dict[date.lower()] or data_dict[date.lower()] == 'None':
                            data_dict[date.lower()] = datetime.strptime(data_dict[date.lower()], '%Y-%m-%d')
                            print(data_dict[date.lower()])
                    flat_list.append(data_dict)
                all_data.extend(flat_list)
                num += 100
            else:
                return all_data
        else:
            return None
