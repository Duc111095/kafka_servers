import json

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from entity.tbmt import Tbmt
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
    # TODO
    # Consumer multiples topics
    bootstrap_server = 'localhost:9092'
    # topic = 'notify.SKMT_App.dbo.notify_zullip, '
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(
                            client_id='zullip-server',
                            group_id='zullip-servers-consumer',
                            bootstrap_servers=bootstrap_server,
                            auto_offset_reset='latest',
                            value_deserializer=lambda m: getdecode(m),
                            enable_auto_commit=False
                        )
    consumer.subscribe(pattern='^notify.*.dbo.notify_zulip')    

    for message in consumer:
        try:
            tp = TopicPartition(message.topic, message.partition)
            om = OffsetAndMetadata(message.offset+1, message.timestamp)
            # Get connection from map
            db_name = message.topic.split('.')[1]
            conn = connect_pool.get(db_name.lower())
            cursor = conn.cursor()
            msg_before = message.value['payload']['before'] 
            msg = message.value['payload']['after']
            logger.info(f"------------------------------------------")
            logger.info(f"Before: {msg_before}")
            logger.info(f"After: {msg}")
            # TODO
            # Consumer by config + runtime method
            if msg['status'] != '1':
                if msg['gc_td1'] != None and msg['gc_td1'] != '':
                    sql_query = msg['gc_td1']
                    cursor.execute(sql_query)
                    tbmts: list[Tbmt] = cursor.fetchall()
                    if len(tbmts) > 0:
                        msg_task = task_to_send(tbmts)
                else:
                    msg_task = msg['content']
                if msg['group_yn'] != '1':
                    result = zc.send_msg_private(msg_task, int(msg['to_person']))
                else:
                    result = zc.send_msg_group(msg_task, int(msg['to_person']))
                logger.info(f"Result: {result}")
                if result['result'] == 'success':
                    sql_query = 'update notify_zulip set datetime2 = getdate(), status = 1 where id = ' + str(msg['id'])
                    cursor.execute(sql_query)
                    logger.info(sql_query)
                    conn.commit()
            consumer.commit({tp:om})
        except Exception as e:
            conn.rollback()
            logger.error(f"{e}")
            consumer.commit({tp:om})
    for conn in connect_pool.values:
        conn.close()