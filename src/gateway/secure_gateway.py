import os
import json
import logging
import redis
from kafka import KafkaConsumer, KafkaProducer
from cryptography.fernet import Fernet

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SecureGateway:
    def __init__(self):
        # Load encryption key
        key = os.environ.get('ENCRYPTION_KEY')
        if not key:
            raise ValueError("ENCRYPTION_KEY environment variable not set")
        self.cipher = Fernet(key.encode())
        
        # Redis Connection (RAM Cache)
        self.redis_client = redis.Redis(
            host=os.environ.get('REDIS_HOST', 'redis'),
            port=6379,
            decode_responses=False  # Binary for safety, though json is text
        )

        # Kafka Consumer (Encrypted Input)
        self.consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092').split(','),
            security_protocol='PLAINTEXT',
            # ssl_cafile='/etc/kafka/secrets/ca-cert',
            # ssl_certfile='/etc/kafka/secrets/client-cert.pem',
            # ssl_keyfile='/etc/kafka/secrets/client-key.pem',
            group_id='secure-gateway-v2',
            auto_offset_reset='earliest'
        )

        # Kafka Producer (Metadata Output for Spark)
        # We produce to a NEW topic: 'transactions-metadata'
        self.producer = KafkaProducer(
            bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092').split(','),
            security_protocol='PLAINTEXT',
            # ssl_cafile='/etc/kafka/secrets/ca-cert',
            # ssl_certfile='/etc/kafka/secrets/client-cert.pem',
            # ssl_keyfile='/etc/kafka/secrets/client-key.pem',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        logger.info("✅ Secure Gateway Initialized")

    def run(self):
        logger.info("🚀 Gateway started. Listening for encrypted messages...")
        
        for message in self.consumer:
            try:
                # 1. Decrypt
                encrypted_data = message.value
                try:
                    plaintext_bytes = self.cipher.decrypt(encrypted_data)
                    transaction = json.loads(plaintext_bytes.decode('utf-8'))
                except Exception as e:
                    logger.error(f"❌ Decryption failed: {e}")
                    continue

                # 2. Cache in Redis (RAM) with TTL
                tx_id = transaction.get('transaction_id')
                if not tx_id:
                    logger.warning("⚠️ Transaction missing ID")
                    continue

                # Store with 5-minute TTL
                self.redis_client.setex(
                    name=f"tx:{tx_id}",
                    time=300, # 5 minutes
                    value=json.dumps(transaction)
                )
                
                # 3. Emit Metadata to Spark
                # Spark needs to know there's a new transaction to fetch
                metadata = {
                    'transaction_id': tx_id,
                    'timestamp': transaction.get('timestamp'),
                    'user_id': transaction.get('user_id'),
                    'ready': True
                }
                self.producer.send('transactions-metadata', metadata)
                self.producer.flush()
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")

if __name__ == '__main__':
    gateway = SecureGateway()
    gateway.run()
