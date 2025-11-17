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
        if PRODUCER_CONFIG.get('transactional.id'):
            producer.init_transactions()
        producer_logger.info("Kafka producer initialized successfully")
        return producer
    except KafkaException as e:
        producer_logger.error(f"Kafka producer initialization failed: {e}")
        raise

def send_message(producer, topic, messages):
    try:
        for key, value in messages.items():
            if isinstance(value, (bytes, bytearray)):
                payload = value
            elif isinstance(value, str):
                payload = value.encode("utf-8")
            else:
                payload = json.dumps(value).encode("utf-8")
            producer.produce(topic, key=key, value=payload, callback=deliver_callback)
        producer.flush()
        producer_logger.info(f"Messages sent to topic {topic}: {len(messages)} items")
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
    success_count = 0
    fail_count = 0
    try:
        for key, value in messages.items():
            try:
                if isinstance(value, (bytes, bytearray)):
                    payload = value
                elif isinstance(value, str):
                    payload = value.encode("utf-8")
                else:
                    payload = json.dumps(value).encode("utf-8")
                for _ in range(3):
                    try:
                        producer.produce(topic, key=key, value=payload, callback=deliver_callback)
                        success_count += 1
                        break
                    except BufferError:
                        producer_logger.warning("Local queue full, draining callbacks...")
                        producer.poll(0)
                else:
                    fail_count += 1
            except KafkaException as e:
                producer_logger.error(f"Failed to send message to topic {topic}: {e}")
                fail_count += 1
        producer.flush(timeout)
    except KafkaException as e:
        producer_logger.error(f"Failed to send batch messages to topic {topic}: {e}")
        raise
    finally:
        producer_logger.info(f"Batch send: {success_count} successes, {fail_count} failures to {topic}")
        return success_count, fail_count
