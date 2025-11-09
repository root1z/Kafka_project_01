import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configuration
MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST'),
    'port': int(os.getenv('MONGO_PORT')),
    'username': os.getenv('MONGO_USERNAME', ''),
    'password': os.getenv('MONGO_PASSWORD', ''),
    'database': os.getenv('MONGO_DATABASE', 'kafka_data'),
    'auth_source': os.getenv('MONGO_AUTH_SOURCE'),
    'max_pool_size': int(os.getenv('MONGO_MAX_POOL_SIZE', '100')),
    'min_pool_size': int(os.getenv('MONGO_MIN_POOL_SIZE', '10')),
    'max_idle_time_ms': int(os.getenv('MONGO_MAX_IDLE_TIME_MS', '30000')),
}

# Collection names
COLLECTIONS = {
    'data_messages': os.getenv('MONGO_COLLECTION_DATA'),
    'error_messages': os.getenv('MONGO_COLLECTION_ERRORS'),
    'processed_offsets': os.getenv('MONGO_COLLECTION_OFFSETS')
}

def get_mongo_uri():
    """Build MongoDB connection URI"""
    if MONGO_CONFIG['username'] and MONGO_CONFIG['password']:
        return f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['database']}?authSource={MONGO_CONFIG['auth_source']}"
    else:
        return f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['database']}"