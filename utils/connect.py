import base64
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

def connect_db() -> any:
    config = CustomConfigParser(allow_no_value=True)
    config.read(os.path.dirname(__file__)+"/server.ini")
    result = {}
    for section in config.sections():
        server = config.get(section, 'server')
        database = config.get(section, 'database')
        username = 'sa'
        password = base64.b64decode(b'MTIzNDU2YUBA').decode("utf-8")
        if server == None: return None
        connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=YES;TrustServerCertificate=YES'
        conn = pyodbc.connect(connectionString)
        result[database] = conn
    return result
