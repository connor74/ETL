import requests

url = "http://iss.moex.com/iss/"
path_history = "history/engines/stock/markets/bonds/"


# http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json?date=2022-03-25&iss.meta=off&history.columns=BOARDID,TRADEDATE,SECID,SHORTNAME,MATDATE,OFFERDATE,YIELDCLOSE,MARKETPRICE3,FACEVALUE,FACEUNIT,COUPONVALUE,ACCINT,COUPONPERCENT,NUMTRADES,YIELDTOOFFER,VOLUME&from=2022-03-25&till=2022-03-25


class MOEX_api:
    def __init__(self, date: str, start=1):
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
            'YIELDCLOSE',
            'MARKETPRICE3',
            'FACEVALUE',
            'FACEUNIT',
            'COUPONVALUE',
            'ACCINT',
            'COUPONPERCENT',
            'NUMTRADES',
            'YIELDTOOFFER',
            'VOLUME',
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
    def status_code(self):
        return self._status_code

    @property
    def columns(self):
        return self._columns

    @property
    def data(self):
        return self._data


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
                    columns = moex.columns
                flat_list = []
                for item in moex.data:
                    flat_list.append(dict(zip(moex.columns, item)))
                all_data.extend(flat_list)
                num += 100
            else:
                return all_data
        else:
            return None
