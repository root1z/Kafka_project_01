from confluent_kafka import Consumer
from config.kafka_config import CONSUMER_CONFIG, TOPIC_NAME
from utils.logger import setup_logger
import json


# Setup logger
consumer_logger = setup_logger("kafka_consumer")

# Create Consumer instance
def create_consumer():
    try:
        consumer = Consumer(CONSUMER_CONFIG)
        consumer.subscribe([TOPIC_NAME['data_input']])
        consumer_logger.info(f"Subscribed to topic {TOPIC_NAME['data_input']}")
        return consumer
    except Exception as e:
        consumer_logger.error(f"Failed to create Kafka consumer: {e}")
        raise e

# Process message
def process_message(msg):
    try:
        #parse message value from bytes to json
        if msg.value() is None:
            consumer_logger.error(f"Message value is None: {msg}")
            return None
        try:    
            value = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            consumer_logger.error(f"Error decoding JSON message: {e}")
            return None
        consumer_logger.debug(f"Received message: {value}")
        return value
    except Exception as e:
        consumer_logger.error(f"Error processing message: {e}")
        return None

# Commit offset
def commit_offset(consumer, msg):
    try:
        consumer.commit(message=msg, asynchronous=False)
        consumer_logger.info(f"Committed offset: {msg.topic()}[{msg.partition()}]@{msg.offset()+1}")
    except Exception as e:
        consumer_logger.error(f"Error committing offset: {e}")

# Close consumer
def close_consumer(consumer):
    try:
        consumer.close()
        consumer_logger.info("Kafka consumer closed")
    except Exception as e:
        consumer_logger.error(f"Error closing Kafka consumer: {e}")