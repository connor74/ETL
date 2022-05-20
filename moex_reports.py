import xml.etree.ElementTree as ET
import os
import datetime
from typing import Any, Generator

import requests
import pandas as pd

DIR = "N:\\KurzanovAV\\Общее\\moex\\"

float_cols = ["Price", "Value", "AccInt", "Amount", "Price2", "FaceAmount", "RepoRate", "ExchComm", "ClrComm"]
date_cols = ["DueDate"]
int_cols = ["Quantity", "RepoPart"]
time_cols = ["TradeTime"]


def str_to_date(str_date: str) -> datetime:
    return datetime.datetime.strptime(str_date, '%Y-%m-%d')


def convert_types(key: str, value: Any) -> Any:
    if key in float_cols:
        ret_value = float(value)
    elif key in date_cols:
        ret_value = str_to_date(value)
    elif key in int_cols:
        ret_value = int(value)
    elif key in time_cols:
        pass
    else:
        ret_value = value
    return ret_value


class MOEX_reports:
    def __init__(self, report_type: str = "stock") -> None:
        self._is_stock = True if report_type == "stock" else False
        self._path = "moex_reports\\" if self._is_stock else "currency_reports\\"
        self._list_files = os.listdir(DIR + self._path)

    def _encode_xml(self, file):
        tree = ET.parse(DIR + self._path + file)
        root = tree.getroot()
        doc_num = root.find("DOC_REQUISITES").attrib['DOC_NO']
        data = []
        data_dict = {
            "doc_num": doc_num,
            "report_date": str_to_date(root.find("EQM06").attrib['ReportDate']),
            "trade_datetime": None,
            "CurrencyId": "",
            "BoardId": "",
            "SecurityId": "",
            "ISIN": "",
            "SecShortName": "",
            "PriceType": "",
            "TradeNo": "",
            "BuySell": "",
            "Price": 0.0,
            "Quantity": 0,
            "Value": 0.0,
            "AccInt": 0.0,
            "Amount": 0.0,
            "Price2": 0.0,
            "RepoPart": 0,
            "DueDate": "1970-01-01",
            "FaceAmount": 0.0,
            "RepoRate": 0.0,
            "ExchComm": 0.0,
            "ClrComm": 0.0,
            "ClientCode": "",
        }
        for item in root.iter():
            for key, value in item.items():
                if key == 'TradeDate':
                    trade_date = value
                if key == 'TradeTime':
                    trade_time = value
                if key in data_dict.keys():
                    data_dict[key] = convert_types(key, value)
            if item.tag == "RECORDS":
                data_dict["trade_datetime"] = datetime.datetime.fromisoformat(trade_date + " " + trade_time)
                data.append(data_dict.copy())
        return doc_num, data

    @property
    def list_files(self) -> list[str]:
        return self._list_files

    @property
    def doc_num(self) -> str:
        return self._doc_num

    def read_files(self):
        for file in self._list_files:
            doc_num, data = self._encode_xml(file)
            yield doc_num, data, file
