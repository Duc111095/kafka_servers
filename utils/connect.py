import os
from pathlib import Path

import pyodbc
import configparser

class CustomConfigParser(configparser.RawConfigParser):
    def get(self, section, option):
        try:
            return configparser.RawConfigParser.get(self, section, option)
        except configparser.NoOptionError:
            return None

def connect_db(db_name: str) -> any:
    config = CustomConfigParser(allow_no_value=True)
    config.read(os.path.dirname(__file__)+"/config.ini")

    server = config.get(db_name, 'server')
    database = config.get(db_name, 'database')
    username = config.get(db_name, 'username')
    password = config.get(db_name, 'password')

    if server == None: return None

    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=YES;TrustServerCertificate=YES'
    conn = pyodbc.connect(connectionString)
    print(f"Connected to {db_name}")
    return conn
