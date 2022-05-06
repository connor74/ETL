import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class db_clickhouse:
    def __init__(self):
        with open('config.json', 'r') as config:
            config_params = json.load(config)["CLICKHOUSE"]
        self._host = "http://" + config_params['HOST'] + ":8123/"
        self._user = config_params['USER']
        self._password = config_params['PASSWORD']
        self._read_format = "JSON"

    def get_data(self, query):
        res = requests.post(self._host, auth=(self._user, self._password), data=query)
        if res.status_code == 200:
            return (res.headers, res.text)
        else:
            raise ValueError(res.text)

class MSSQL:
    def __init__(self):
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SQL;DATABASE=ComplexBank;UID=URALPROM\\Kurzanovav;Trusted_Connection=yes"
        con_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        self._engine = create_engine(con_url)
    def query(self, sql):
        pd.read_sql_query(sql, self._engine)




db = db_clickhouse()
#data = db.get_data("SELECT * FROM main.balance_1 LIMIT 10 FORMAT Values")
query = """
INSERT INTO main.Test_data VALUES\n 
(1, , 'None', 'Test message')
"""
db.get_data(query)





