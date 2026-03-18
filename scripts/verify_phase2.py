
import os
import json
import time
import redis
from kafka import KafkaConsumer
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load env
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BROKER_URL', 'localhost:9093')
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6380))

print(f"--- Phase 2 Verification ---")
print(f"Kafka: {KAFKA_BOOTSTRAP}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
print(f"Key: {ENCRYPTION_KEY[:5]}...")

# 1. Verify Encryption (Kafka)
try:
    print("\n[1/3] Checking Kafka Encryption...")
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=KAFKA_BOOTSTRAP.split(','),
        security_protocol='SSL',
        ssl_cafile='/etc/kafka/secrets/ca-cert',
        ssl_certfile='/etc/kafka/secrets/client-cert.pem',
        ssl_keyfile='/etc/kafka/secrets/client-key.pem',
        auto_offset_reset='latest',
        consumer_timeout_ms=5000
    )
    
    msg_count = 0
    for message in consumer:
        payload = message.value
        try:
            # Try to load as JSON (Should FAIL if encrypted)
            json.loads(payload)
            print("❌ FAILURE: Payload is PLAINTEXT JSON!")
        except:
            print("✅ SUCCESS: Payload is NOT JSON (likely encrypted)")
            
        # Try to Decrypt
        cipher = Fernet(ENCRYPTION_KEY.encode())
        try:
            plaintext = cipher.decrypt(payload)
            data = json.loads(plaintext)
            print(f"✅ SUCCESS: Decrypted payload! ID: {data.get('transaction_id')}")
            msg_count += 1
            if msg_count >= 1: break
        except Exception as e:
            print(f"❌ FAILURE: Decryption failed: {e}")
            break
            
except Exception as e:
    print(f"⚠️ Kafka Check Error: {e}")

# 2. Verify Redis Cache
try:
    print("\n[2/3] Checking Redis Cache...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    keys = r.keys('tx:*')
    print(f"Found {len(keys)} keys in Redis.")
    
    if len(keys) > 0:
        key = keys[0]
        ttl = r.ttl(key)
        val = r.get(key)
        print(f"Key: {key}, TTL: {ttl}s")
        print(f"Value sample: {val[:50]}...")
        if ttl > 0:
             print("✅ SUCCESS: Data cached with TTL")
        else:
             print("⚠️ WARNING: No TTL found")
    else:
        print("⚠️ WARNING: Redis is empty (Gateway might not be processing?)")

except Exception as e:
    print(f"⚠️ Redis Check Error: {e}")

print("\n--- End Verification ---")
