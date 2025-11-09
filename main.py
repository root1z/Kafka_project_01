from consumer.kafka_consumer import create_consumer,process_message,commit_offset,close_consumer
from config.mongo_config import get_mongo_uri, MONGO_CONFIG, COLLECTIONS
from config.kafka_config import APPLICATION_CONFIG, CONSUMER_CONFIG, TOPIC_NAME, KAFKA_CONFIG
from utils.logger import setup_logger
from pymongo import MongoClient
from confluent_kafka.admin import AdminClient, NewTopic
from producer.kafka_producer import create_producer, batch_send_messages, close_producer
import json
import signal
import sys

logger = setup_logger("main")


def ensure_topic():
    topic_name = TOPIC_NAME.get("data_input")
    try:
        admin = AdminClient(KAFKA_CONFIG)
        md = admin.list_topics(timeout=10)
        if topic_name not in md.topics:
            logger.info(f"Topic '{topic_name}' not found. Creating...")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            fs = admin.create_topics([new_topic])
            for t, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic '{t}' created successfully")
                except Exception as e:
                    logger.warning(f"Failed to create topic '{t}': {e}")
    except Exception as e:
        logger.warning(f"Failed to check/create topic '{topic_name}': {e}")


def load_messages_from_file(file_path: str = "sample_data.json") -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {f"item-{i}": item for i, item in enumerate(data)}
    raise ValueError("JSON input must be a dict or list.")


def produce_from_file(file_path: str = "sample_data.json"):
    ensure_topic()
    messages = load_messages_from_file(file_path)
    producer = None
    try:
        producer = create_producer()
        topic = TOPIC_NAME.get("data_input")
        batch_send_messages(producer, topic, messages)
        logger.info(f"Sent {len(messages)} messages to topic '{topic}' from '{file_path}'")
    except Exception as e:
        logger.error(f"Error when sending messages: {e}")
        sys.exit(1)
    finally:
        if producer is not None:
            try:
                close_producer(producer)
            except Exception:
                pass


def create_mongo_client():
    uri = get_mongo_uri()
    client = MongoClient(
        uri,
        maxPoolSize=MONGO_CONFIG["max_pool_size"],
        minPoolSize=MONGO_CONFIG["min_pool_size"],
        maxIdleTimeMS=MONGO_CONFIG["max_idle_time_ms"],
    )
    db = client[MONGO_CONFIG["database"]]
    cols = {
        "data_messages": db[COLLECTIONS["data_messages"]],
        "error_messages": db[COLLECTIONS["error_messages"]],
        "processed_offsets": db[COLLECTIONS["processed_offsets"]],
    }
    logger.info(
        f"Connected to MongoDB {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']} database {MONGO_CONFIG['database']}"
    )
    return client, cols


def run_consumer():
    running = True

    def _handle_signal(signum):
        nonlocal running
        logger.info(f"Received signal {signum}, stopping...")
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer = None
    mongo_client = None
    try:
        ensure_topic()
        consumer = create_consumer()
        mongo_client, cols = create_mongo_client()

        poll_timeout_ms = APPLICATION_CONFIG.get("poll.timeout.ms", 1000)
        poll_timeout_sec = poll_timeout_ms / 1000.0

        while running:
            msg = consumer.poll(poll_timeout_sec)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            parsed = process_message(msg)
            if parsed is None:
                try:
                    cols["error_messages"].insert_one(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "key": (
                                msg.key().decode("utf-8")
                                if isinstance(msg.key(), (bytes, bytearray))
                                else msg.key()
                            ),
                            "value_raw": (
                                msg.value().decode("utf-8")
                                if isinstance(msg.value(), (bytes, bytearray))
                                else msg.value()
                            ),
                            "error": "parse_failed",
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to write error message to MongoDB: {e}")
                continue

            try:
                cols["data_messages"].insert_one(
                    {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "group_id": CONSUMER_CONFIG.get("group.id"),
                        "key": (
                            msg.key().decode("utf-8")
                            if isinstance(msg.key(), (bytes, bytearray))
                            else msg.key()
                        ),
                        "value": parsed,
                        "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
                    }
                )

                try:
                    cols["processed_offsets"].update_one(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "group_id": CONSUMER_CONFIG.get("group.id"),
                        },
                        {
                            "$set": {
                                "offset": msg.offset(),
                                "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
                            }
                        },
                        upsert=True,
                    )
                except Exception as e:
                    logger.warning(f"Failed to update processed_offsets: {e}")

                commit_offset(consumer, msg)
            except Exception as e:
                logger.error(f"Failed to write message to MongoDB: {e}")
                try:
                    cols["error_messages"].insert_one(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "key": (
                                msg.key().decode("utf-8")
                                if isinstance(msg.key(), (bytes, bytearray))
                                else msg.key()
                            ),
                            "value": parsed,
                            "error": str(e),
                        }
                    )
                except Exception as ie:
                    logger.error(f"Failed to write error message to MongoDB: {ie}")

    except Exception as e:
        logger.error(f"Critical error in consumer: {e}")
        sys.exit(1)
    finally:
        if consumer is not None:
            try:
                close_consumer(consumer)
            except Exception:
                pass
        if mongo_client is not None:
            try:
                mongo_client.close()
                logger.info("Closed MongoDB client")
            except Exception:
                pass


if __name__ == "__main__":
    produce_from_file("sample_data.json")
    run_consumer()