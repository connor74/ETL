# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import os
import datetime
import requests
import pandas as pd


class MOEXreports:
    def __init__(self):
        dir = "N:\\KurzanovAV\\Общее\\moex\\"
        self.path= "N:\\KurzanovAV\\Общее\\moex\\moex_reports\\"

        self.list_files = os.listdir(self.path)

    @staticmethod
    def delete_item(keys, dict_):
        for i in keys:
            if i in dict_:
                del dict_[i]

    def _get_xml_root(self, file):
        tree = ET.parse(self.path + file)
        root = tree.getroot()
        doc_num = tree.getroot()[0].attrib['DOC_NO']
        return doc_num, root

    def _parse_moex_xml(self, root):
        doc_num = root[0].attrib['DOC_NO']
        try:
            report_date = root[1].attrib['ReportDate']

        except KeyError:
            print(f"File: {doc_num}. Отсутствует ReportDate")
        tags = [
            "CURRENCY",
            "BOARD",
            "SECURITY",
        ]
        records = []
        attrs = {
            "CurrencyId": None,
            "BoardId": None,
            "BoardName": None,
            "SecurityId": None,
            "ISIN": None,
            "SecShortName": None,
            "PriceType": None,
        }
        record_keys = [
            "TradeNo",
            "TradeDate",
            "TradeTime",
            "BuySell",
            "Price",
            "Quantity",
            "Value",
            "AccInt",
            "Amount",
            "Price2",
            "RepoPart",
            "DueDate",
            "FaceAmount",
            "RepoRate",
            "ExchComm",
            "ClrComm",
            "ClientDetails",
            "ClientCode",
        ]
        for item in root.iter():
            if item.tag in tags:
                for key, value in item.attrib.items():
                    attrs['report_date'] = report_date
                    attrs['doc_num'] = doc_num
                    attrs[key] = value
            if item.tag == "RECORDS":
                record = dict.fromkeys(record_keys)
                for key, value in item.attrib.items():
                    if key in record_keys:
                        record[key] = value
                records.append(attrs | record)

        for item in records:
            for key, value in item.items():
                if key == 'TradeDate':
                    trade_date = value
                if key == 'TradeTime':
                    trade_time = value
                if key in ['Price', 'Price2', 'Value', 'AccInt', 'Amount', 'FaceAmount', 'ExchComm', 'ClrComm', 'RepoRate']:
                    if not value:
                        value = 0.0
                    item[key] = float(value)
                if key in ['Quantity', 'RepoPart']:
                    if not value:
                        value = 0
                    item[key] = int(value)
                if key in ['ClientCode']:
                    if not value:
                        value = str("")
            item['trade_date_time'] = datetime.datetime.fromisoformat(trade_date + " " + trade_time)
            MOEXreports.delete_item(
                [
                    'TradeDate',
                    'TradeTime',
                    'BoardName',
                    'CurrencyName',
                    'ClientDetails',
                ], item)
            item['report_date'] = datetime.datetime.fromisoformat(item['report_date'])
            item['DueDate'] = datetime.datetime.fromisoformat(item['DueDate'])
        return records

    def moex_xmlreports_to_ch(self, ch):
        files_count = len(self.list_files)
        for i, item in enumerate(self.list_files):
            doc_num, root = self._get_xml_root(item)
            count = ch.execute(f"SELECT COUNT(*) FROM main.moex_deals WHERE doc_num = '{doc_num}'")
            if count[0][0] == 0:
                data = self._parse_moex_xml(root)
                rows = ch.insert("moex_deals", data)
                print(f"Файл {i+1} из {files_count} ||| Добавлено {str(rows)} строк из  {str(item)}")
            else:
                print(f"Файл {i+1} из {files_count} |||  {str(item)}- уже внесен в базу")

class MOEXcurency:
    def __init__(self, file):
        self.path = "N:\\KurzanovAV\\Общее\\moex\\currency_reports\\"
        self.file = file



        self.list_files = os.listdir(self.path)


    def _get_xml_root(self):
        tree = ET.parse(self.path + self.file)
        root = tree.getroot()
        doc_num = tree.getroot()[0].attrib['DOC_NO']
        return doc_num, root



#MOEXcurency("MB02568_CUX23_D77_040322_003854088.xml")

class CBRcurrency:
    def __init__(self, ch):
        self.today = datetime.date.today()
        self.curr_list = ["USD", "EUR", "GBP", "KZT", "CNY"]
        self.url = "https://cbr.ru/scripts/XML_daily.asp?date_req="
        self.ch = ch

    def _parse_data(self, date):
        data = []
        # Добавить обработку исключений
        resp = requests.get(self.url + date.strftime('%d/%m/%Y'))
        root = ET.fromstring(resp.text)
        for cur in root.findall('Valute'):
            if cur.find('CharCode').text in self.curr_list:
                items = dict()
                for item in cur:
                    items['date'] = date
                    items['cur_code'] = cur.find('CharCode').text
                    items['nominal'] = int(cur.find('Nominal').text)
                    items['value'] = float(cur.find('Value').text.replace(',', '.'))
                data.append(items)

        num = self.ch.insert('cbr_currency', data)
        if num:
            print("Добавлены курсы валют за ", date.strftime("%Y-%m-%d"))
            return True
        return False

    def get_data(self):
        last_date = self.ch.max_date("cbr_currency", "date")
        print(type(last_date))
        print(type(self.today))
        if not last_date:
            date_range_all = pd.date_range(start=last_date - datetime.timedelta(-1), end=self.today)
            for date in date_range_all:
                self._parse_data(date)
        elif last_date < self.today:
            self._parse_data(self.today)
        else:
            return None






