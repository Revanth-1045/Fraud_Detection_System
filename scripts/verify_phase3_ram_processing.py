import json
import time
import uuid
import redis
from kafka import KafkaProducer
import logging
import subprocess
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9094'
METADATA_TOPIC = 'transactions-metadata'

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def verify_ram_processing():
    tx_id = str(uuid.uuid4())
    user_id = 99999
    amount = 5000.0
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")

    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": timestamp,
        "merchant_id": "TEST_MERCHANT",
        "device_id": "TEST_DEVICE"
    }

    metadata = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "timestamp": timestamp
    }

    logger.info(f"Starting Phase 3 RAM Verification for TX ID: {tx_id}")

    # 1. Write Payload to Redis (RAM)
    try:
        r = get_redis_client()
        r.set(f"tx:{tx_id}", json.dumps(payload), ex=300) # 5 min TTL
        logger.info("✅ Payload written to Redis (RAM Cache)")
    except Exception as e:
        logger.error(f"Failed to write to Redis: {e}")
        return False

    # 2. Send Metadata to Kafka
    try:
        producer = get_kafka_producer()
        producer.send(METADATA_TOPIC, metadata)
        producer.flush()
        logger.info(f"✅ Metadata sent to Kafka topic '{METADATA_TOPIC}'")
    except Exception as e:
        logger.error(f"Failed to send to Kafka: {e}")
        return False

    # 3. Wait for Spark Processing (Check Redis Purge)
    logger.info("⏳ Waiting for Spark to process and purge Redis key...")
    max_retries = 20
    for i in range(max_retries):
        if not r.exists(f"tx:{tx_id}"):
            logger.info("✅ Redis key purged! Spark processing confirmed.")
            break
        time.sleep(2)
    else:
        logger.error("❌ Redis key was NOT purged after 40 seconds. Spark job may be failing or slow.")
        return False

    # 4. Verify recent.json in Shared Volume (via Docker Exec)
    logger.info("⏳ Verifying recent.json update in shared volume...")
    try:
        # Check if the transaction ID appears in the recent.json file inside the web-app container
        # We use web-app because it mounts the same shared volume
        cmd = f"docker exec project-copy-web-app-1 grep {tx_id} /shared/dashboard/recent.json"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ Transaction found in /shared/dashboard/recent.json: {result.stdout.strip()}")
            return True
        else:
            logger.error("❌ Transaction NOT found in recent.json. Shared volume write failed.")
            return False
            
    except Exception as e:
        logger.error(f"Failed to check shared volume: {e}")
        return False

if __name__ == "__main__":
    if verify_ram_processing():
        logger.info("\n🎉 PASSED: Phase 3 RAM-Only Processing Verified Successfully!")
        sys.exit(0)
    else:
        logger.error("\nFAILED: Verification steps failed.")
        sys.exit(1)
