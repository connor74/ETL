from datetime import datetime

import requests

url = "http://iss.moex.com/iss/"
path_history = "history/engines/stock/markets/bonds/"


# http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json?date=2022-03-25&iss.meta=off&history.columns=BOARDID,TRADEDATE,SECID,SHORTNAME,MATDATE,OFFERDATE,YIELDCLOSE,MARKETPRICE3,FACEVALUE,FACEUNIT,COUPONVALUE,ACCINT,COUPONPERCENT,NUMTRADES,YIELDTOOFFER,VOLUME&from=2022-03-25&till=2022-03-25


class MOEX_api:
    def __init__(self) -> None:
        self._pages = None
        self._data = None
        self._columns = None
        self._status_code = None
        self._resp = None
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
            'iss.meta': 'off',
            'history.columns': ','.join(self._req_columns),
        }

    def _get_data_by_page(self, date: str, num: int = None):
        """

        :param date:
        :param num:
        :return:
        """
        self._params['date'] = date
        if num:
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

    def get_data_by_date(self, date: str) -> list[list]:
        # API выдает по 100 записей, необходимо пройти в цикле по всем страницам
        # Отсчет идет от номера первой записи на странице
        num = 1
        all_data = []
        while True:
            self._get_data_by_page(date, num)
            if self._status_code:
                if self._data:
                    if num == 1:
                        columns = [cols.lower() for cols in self._columns]
                    flat_list = []
                    for item in self._data:
                        data_dict = dict(zip(columns, item))
                        for date in self._date_columns:
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
