from confluent_kafka import Producer, KafkaException
from config.kafka_config import PRODUCER_CONFIG
from utils.logger import setup_logger
import json

producer_logger = setup_logger('kafka_producer')


def deliver_callback(err, msg):
    if err is not None:
        producer_logger.error(f"Message delivery failed: {err}")
    else:
        producer_logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_producer():
    """Tạo Kafka producer instance với config được định nghĩa trong kafka_config.py"""
    try:
        producer = Producer(PRODUCER_CONFIG)
        producer_logger.info("Kafka producer initialized successfully")
        return producer
    except KafkaException as e:
        producer_logger.error(f"Kafka producer initialization failed: {e}")
        raise

def send_message(producer, topic, messages):
    """Gửi message tới Kafka topic"""
    try:
        for key, value in messages.items():
            value_json = json.dumps(value).encode("utf-8")
            producer.produce(
                topic, 
                key=key, 
                value=value_json, 
                callback=deliver_callback)
        producer.flush()
        producer_logger.info(f"Messages sent to topic {topic}: {messages}")
    except KafkaException as e:
        producer_logger.error(f"Failed to send messages to topic {topic}: {e}")
        raise

def close_producer(producer, timeout=30):
    """Đóng Kafka producer"""
    try:
        remaining_messages = producer.flush(timeout)
        if remaining_messages > 0:
            producer_logger.warning(f"{remaining_messages} messages were not delivered")
        else:
            producer_logger.info("All messages were successfully delivered")
    except KafkaException as e:
        producer_logger.error(f"Error while closing Kafka producer: {e}")
        raise

def batch_send_messages(producer, topic, messages, timeout=30):
    """Gửi nhiều message tới Kafka topic trong một batch"""
    success_count = 0
    fail_count = 0
    try:
        for key, value in messages.items():
            try:
                send_message(producer, topic, {key: value})
                success_count += 1
            except KafkaException as e:
                producer_logger.error(f"Failed to send message to topic {topic}: {e}")
                fail_count += 1
        close_producer(producer, timeout)
    except KafkaException as e:
        producer_logger.error(f"Failed to send batch messages to topic {topic}: {e}")
        raise
    finally:
        producer_logger.info(f"Batch send messages to topic {topic}: {success_count} successes, {fail_count} failures")
        return success_count, fail_count
