import os
from dotenv import load_dotenv

load_dotenv()

# kafka config
KAFKA_CONFIG = {
    # ========================================================= #
    # Cấu hình chung cho Kafka, bao gồm các thông tin cơ bản    #
    # - bootstrap.servers: địa chỉ broker Kafka                 #
    # - security.protocol: 'SASL_SSL' cho kết nối bảo mật      #
    # - sasl.mechanisms: 'PLAIN' cơ chế xác thực                #
    # - sasl.username/sasl.password: thông tin xác thực SASL    #
    # ========================================================= #
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS"),
    "sasl.username": os.getenv("KAFKA_USERNAME",''),
    "sasl.password": os.getenv("KAFKA_PASSWORD",''),
    'session.timeout.ms': int(os.getenv('SESSION_TIMEOUT_MS', '10000')),
    'socket.timeout.ms': int(os.getenv('SOCKET_TIMEOUT_MS', '30000')),
}

# Producer config
PRODUCER_CONFIG = {
    # ========================================================= #
    # Cấu hình riêng cho Producer, kế thừa từ KAFKA_CONFIG      #
    # - acks: đảm bảo độ bền (0/1/all) – 'all' chờ toàn bộ ISR  #
    # - retries: số lần gửi lại khi gặp lỗi tạm thời            #
    # - enable.idempotence: bật chính xác-một-lần theo thứ tự   #
    # - compression.type: 'snappy' nén nhanh, mức vừa phải      #
    # ========================================================= #
    **KAFKA_CONFIG,
    'acks': os.getenv("KAFKA_PRODUCER_ACK", 'all'), # default 'all'
    'retries': os.getenv("MAX_RETRY_ATTEMPTS", 3), # default 3 retries
    'enable.idempotence': os.getenv("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", 'true'), # default 'true'
    'compression.type': os.getenv("KAFKA_PRODUCER_COMPRESSION_TYPE", 'snappy'), # default 'snappy'
}

# Consumer config
CONSUMER_CONFIG = {
    # ========================================================= #
    # Cấu hình riêng cho Consumer, kế thừa từ KAFKA_CONFIG      #
    # - group.id: nhóm ID để phân phối các consumer             #
    # - auto.offset.reset: 'earliest' bắt đầu từ đầu, 'latest' #
    #                     bắt đầu từ cuối                       #
    # - enable.auto.commit: 'true' tự động commit offset       #
    # - max.poll.interval.ms: giới hạn thời gian giữa các poll #
    # ========================================================= #
    **KAFKA_CONFIG,
    'group.id': os.getenv("KAFKA_CONSUMER_GROUP", 'data-processing-group'),
    'auto.offset.reset': os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET"),
    'enable.auto.commit': os.getenv("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT").lower() == 'true',
    'max.poll.interval.ms': os.getenv("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS"),
}

#topic
TOPIC_NAME = {
    # ========================================================= #
    # Tên topic để publish/subscribe dữ liệu                     #
    # - data_input: topic nhận dữ liệu đầu vào                  #
    # - data_output: topic phát dữ liệu ra ngoài                #
    # ========================================================= #
    'data_input': os.getenv('KAFKA_INPUT_TOPIC', 'data-input-topic'),
    'data_output': os.getenv('KAFKA_OUTPUT_TOPIC', 'data-output-topic'),
}

# Application config
APPLICATION_CONFIG = {
    # ========================================================= #
    # Cấu hình ứng dụng                                        #
    # - batch.size: số lượng record tối đa trong mỗi lô        #
    # - poll.timeout.ms: thời gian chờ giữa các poll           #
    # - max.poll.records: số lượng record tối đa trong mỗi poll#
    # ========================================================= #
    'batch.size': int(os.getenv('BATCH_SIZE', 100)),
    'poll.timeout.ms': int(os.getenv('POLL_TIMEOUT_MS', 1000)),
    'max.poll.records': int(os.getenv('MAX_POLL_RECORDS', 100)),
}

# Monitoring
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
STATISTICS_INTERVAL_MS = int(os.getenv("STATISTICS_INTERVAL_MS", 10000))

# External Services
AKHQ_CONFIG = {
    'url': os.getenv('AKHQ_URL', 'http://localhost:8180'),
    'username': os.getenv('AKHQ_USERNAME', ''),
    'password': os.getenv('AKHQ_PASSWORD', '')
}