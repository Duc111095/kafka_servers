from services import kafka_services
from utils import connect

if __name__ == "__main__":
    # Init Connection of Multiples DB
    server_pool = connect.connect_db()
    # Kafka consumer
    kafka_services.kafka_consumer(server_pool)
