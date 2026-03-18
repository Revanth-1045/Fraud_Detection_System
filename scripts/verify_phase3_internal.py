import json
import time
import uuid
import redis
import os
import sys
import logging
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration (Internal Docker Network)
REDIS_HOST = 'redis-ram-cache'
REDIS_PORT = 6379
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
METADATA_TOPIC = 'transactions-metadata'
SHARED_DASHBOARD_PATH = '/shared/dashboard/recent.json'     # Path INSIDE spark-inference container

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def verify_ram_processing():
    tx_id = str(uuid.uuid4())
    user_id = 88888
    amount = 8888.0
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")

    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": timestamp,
        "merchant_id": "INTERNAL_TEST",
        "device_id": "INTERNAL_DEVICE"
    }

    metadata = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "timestamp": timestamp
    }

    logger.info(f"Starting Phase 3 Internal RAM Verification for TX ID: {tx_id}")

    # 1. Write Payload to Redis (RAM)
    try:
        r = get_redis_client()
        r.set(f"tx:{tx_id}", json.dumps(payload), ex=300)
        logger.info("✅ Payload written to Redis (RAM Cache)")
    except Exception as e:
        logger.error(f"Failed to write to Redis: {e}")
        return False

    # 2. Send Metadata to Kafka
    try:
        producer = get_kafka_producer()
        # producer usually takes a bit to connect
        producer.send(METADATA_TOPIC, metadata)
        producer.flush()
        logger.info(f"✅ Metadata sent to Kafka topic '{METADATA_TOPIC}'")
    except Exception as e:
        logger.error(f"Failed to send to Kafka: {e}")
        return False

    # 3. Wait for Spark Processing (Check Redis Purge)
    logger.info("⏳ Waiting for Spark to process and purge Redis key...")
    max_retries = 30
    for i in range(max_retries):
        if not r.exists(f"tx:{tx_id}"):
            logger.info("✅ Redis key purged! Spark processing confirmed.")
            break
        time.sleep(2)
    else:
        logger.error("❌ Redis key was NOT purged after 60 seconds. Spark job may be failing or slow.")
        return False

    # 4. Verify recent.json in Shared Volume (Direct File Check)
    logger.info("⏳ Verifying recent.json update in shared volume...")
    try:
        found = False
        for _ in range(10):
            if os.path.exists(SHARED_DASHBOARD_PATH):
                with open(SHARED_DASHBOARD_PATH, 'r') as f:
                    content = f.read()
                    if tx_id in content:
                        logger.info(f"✅ Transaction found in {SHARED_DASHBOARD_PATH}")
                        found = True
                        break
            else:
                 logger.info(f"⚠️ {SHARED_DASHBOARD_PATH} does not exist yet for this check.")
            time.sleep(1)
        
        if not found:
             logger.error("❌ Transaction NOT found in recent.json.")
             return False

        return True
            
    except Exception as e:
        logger.error(f"Failed to check shared volume file: {e}")
        return False

if __name__ == "__main__":
    if verify_ram_processing():
        logger.info("\n🎉 PASSED: Phase 3 Internal RAM-Only Processing Verified!")
        sys.exit(0)
    else:
        logger.error("\nFAILED: Verification steps failed.")
        sys.exit(1)
