import base64
from datetime import datetime
import json
import os
import pyodbc

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from entity.tbmt import Tbmt
from services.excel_services import create_excel_file_from_sql_data
from services.format_tbmt import task_to_send
from utils.logger import get_app_logger
from utils import zullip_code as zc


def getdecode(obj):
    if obj is None:
        return None
    else:
        return json.loads(obj.decode('utf-8'))
    
def kafka_consumer(connect_pool):
    logger = get_app_logger()
    logger.info("Starting Kafka Consumer")
    # TODO
    # Consumer multiples topics
    bootstrap_server = 'localhost:9092'
    # topic = 'notify.SKMT_App.dbo.notify_zullip, '
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(
                            client_id='zullip-server-51',
                            group_id='zullip-servers-consumer-51',
                            bootstrap_servers=bootstrap_server,
                            auto_offset_reset='earliest',
                            value_deserializer=lambda m: getdecode(m),
                            enable_auto_commit=False
                        )
    consumer.subscribe(pattern='^notify.*.dbo.notify_zulip')    

    for message in consumer:
        try:
            tp = TopicPartition(message.topic, message.partition)
            om = OffsetAndMetadata(message.offset+1, message.timestamp, 1)
            db_name = message.topic.split('.')[1]
            connectString = connect_pool.get(db_name.lower())
            conn = pyodbc.connect(connectString)
            cursor = conn.cursor()
            msg_before = message.value['payload']['before'] 
            msg = message.value['payload']['after']
            file_name = msg['gc_td2']
            file_name_dest = msg['gc_td3']
            if msg['s4'] != None and msg['s4'] != '': 
                start_row = int.from_bytes(base64.b64decode(msg['s4']), byteorder='big', signed=True)
            content = msg['content']
            to_person = msg['to_person']
            topic = msg['subtitle']
            status = msg['status']
            group_yn = msg['group_yn']
            operation = message.value['payload']['op']
            sql_query = msg['gc_td1']

            
            if file_name != None and file_name != '' and status != '1' and operation != 'r':
                dest_file = create_excel_file_from_sql_data(cursor, sql_query, file_name, start_row, file_name_dest)

            # Consumer by config + runtime method
            if status != '1' and operation != 'r':
                if sql_query != None and sql_query != '':
                    if file_name == None or file_name == '':
                        cursor.execute(sql_query)
                        try:
                            tbmts: list[Tbmt] = cursor.fetchall()
                            msg_task = task_to_send(tbmts)
                        except Exception as e:
                            msg_task = f'Không có Thông báo thầu đến hạn - Ngày {datetime.strftime(datetime.now().date(), "%d-%m-%Y")}'
                            logger.info("Skipping non rs message: {}".format(e))

                else:
                    msg_task = content

                if group_yn != '1':
                    if file_name == None or file_name == '':
                        result = zc.send_msg_private(msg_task, int(to_person))
                    else:
                        try:
                            result = zc.upload_file_private(dest_file, int(to_person), content.strip() + " - Ngày: "+ datetime.now().strftime('%d-%m-%Y'))
                            logger.info(f"Send file successfully: {dest_file}")
                            os.remove(dest_file)
                        except Exception as e:
                            logger.error(f"Error: {e} - SQL query: {sql_query}")
                else:
                    if file_name == None or file_name == '':
                        result = zc.send_msg_group(msg_task, int(msg['to_person']), topic)
                    else:
                        try:
                            result = zc.upload_file_group(dest_file, int(to_person), content.strip() + " " + datetime.now().strftime('%d-%m-%Y'), topic)
                            logger.info(f"Send file successfully: {dest_file}")
                            os.remove(dest_file)
                        except Exception as e:
                            logger.error(f"Error: {e} - SQL query: {sql_query}")
                logger.info(f"Result: {result}")
                if result['result'] == 'success':
                    sql_query = 'update notify_zulip set datetime2 = getdate(), status = 1 where id = ' + str(msg['id'])
                    cursor.execute(sql_query)
                    conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"{db_name} - {e}")
        finally:
            consumer.commit({tp:om})
            conn.close()
    for conn in connect_pool.values:
        conn.close()
