from confluent_kafka import Consumer, TopicPartition
from config.kafka_config import APPLICATION_CONFIG, TOPIC_NAME, SOURCE_CONSUMER_CONFIG
from producer.kafka_producer import create_producer, close_producer, deliver_callback
from utils.logger import setup_logger


logger = setup_logger("bridge")


def _flush_batch_tx(producer, ext_consumer, batch_messages, batch_to_commit):
    try:
        if not batch_messages:
            return
        # Bắt đầu transaction nếu có transactional.id (an toàn khi không có)
        try:
            producer.begin_transaction()
        except Exception:
            pass

        for k, v in batch_messages.items():
            for _ in range(3):
                try:
                    producer.produce(TOPIC_NAME.get("data_input"), key=k, value=v, callback=deliver_callback)
                    break
                except BufferError:
                    producer.poll(0)

        producer.flush()

        # Tính offset mới nhất theo (topic, partition)
        latest = {}
        for m in batch_to_commit:
            tp = (m.topic(), m.partition())
            off = m.offset() + 1
            if tp not in latest or off > latest[tp]:
                latest[tp] = off
        offsets = [TopicPartition(t, p, o) for (t, p), o in latest.items()]

        # Commit offsets trên source consumer (KHÔNG gửi vào transaction của producer)
        try:
            ext_consumer.commit(offsets=offsets, asynchronous=False)
            logger.info(f"Committed source offsets: {offsets}")
        except Exception as ce:
            logger.error(f"Failed to commit source offsets: {ce}")

        # Kết thúc transaction nếu có
        try:
            producer.commit_transaction()
        except Exception:
            try:
                producer.abort_transaction()
            except Exception:
                pass
    except Exception as e:
        try:
            producer.abort_transaction()
        except Exception:
            pass
        logger.error(f"Failed to transactional flush batch: {e}")
    finally:
        batch_messages.clear()
        batch_to_commit.clear()


def _add_to_batch(batch_messages, batch_to_commit, msg):
    try:
        batch_messages[msg.key()] = msg.value()
        batch_to_commit.append(msg)
    except Exception as e:
        logger.warning(f"Failed to buffer message into batch: {e}")


def bridge_external_to_internal(stop_event):
    ext_consumer = None
    producer = None
    try:
        ext_consumer = Consumer(SOURCE_CONSUMER_CONFIG)
        ext_consumer.subscribe([TOPIC_NAME.get("source_input")])
        producer = create_producer()

        poll_timeout_ms = APPLICATION_CONFIG.get("poll.timeout.ms", 1000)
        poll_timeout_sec = poll_timeout_ms / 1000.0
        batch_size = APPLICATION_CONFIG.get("batch.size", 100)
        batch_messages = {}
        batch_to_commit = []

        while not stop_event.is_set():
            msg = ext_consumer.poll(poll_timeout_sec)
            if msg is None:
                _flush_batch_tx(producer, ext_consumer, batch_messages, batch_to_commit)
                continue
            if msg.error():
                logger.error(f"Kafka source error: {msg.error()}")
                continue

            try:
                _add_to_batch(batch_messages, batch_to_commit, msg)
                if len(batch_messages) >= batch_size:
                    _flush_batch_tx(producer, ext_consumer, batch_messages, batch_to_commit)
            except Exception as e:
                logger.error(f"Failed to bridge message: {e}")
    except Exception as e:
        logger.error(f"Critical error in bridge: {e}")
    finally:
        if producer is not None and batch_messages:
            _flush_batch_tx(producer, ext_consumer, batch_messages, batch_to_commit)
        if producer is not None:
            try:
                close_producer(producer)
            except Exception:
                pass
        if ext_consumer is not None:
            try:
                ext_consumer.close()
            except Exception:
                pass