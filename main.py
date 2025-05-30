from datetime import datetime
import os
from services import kafka_services
from utils import connect
import pyodbc
from utils.logger import get_app_logger
from services.excel_services import create_excel_file_from_sql_data
import utils.zullip_code as zc

if __name__ == "__main__":
    # Init Connection of Multiples DB
    server_pool = connect.connect_db()
   
    # Kafka consumer
    kafka_services.kafka_consumer(server_pool)
