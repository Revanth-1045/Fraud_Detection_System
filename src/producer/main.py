import datetime
import time
import json
import os
import logging
from confluent_kafka import Producer
from faker import Faker
from cryptography.fernet import Fernet

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER_URL", "kafka:29092")

# Encryption
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
cipher = None
if ENCRYPTION_KEY:
    try:
        cipher = Fernet(ENCRYPTION_KEY)
        logger.info("🔐 Payload Encryption ENABLED")
    except Exception as e:
        logger.error(f"Failed to initialize encryption: {e}")

def wait_for_kafka():
    logger.info(f"Checking Kafka connection at {KAFKA_BOOTSTRAP_SERVERS}...")
    pass

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def check_status():
    status_file = "/app/data/producer_status"
    if os.path.exists(status_file):
        try:
            with open(status_file, "r") as f:
                return f.read().strip() == "RUNNING"
        except:
            return True
    return True

def run_replay_mode(producer, cipher, fake):
    logger.info("🔁 Starting Producer in REPLAY MODE (Reading from CSV)...")
    try:
        import pandas as pd
        csv_path = "/app/data/mock_creditcard.csv"
        if not os.path.exists(csv_path):
             logger.error(f"CSV file not found at {csv_path}. Defaulting to generator.")
             return False # Signal to fall back
        
        df = pd.read_csv(csv_path)
        if 'Amount' in df.columns:
            df.rename(columns={'Amount': 'amount'}, inplace=True)
        logger.info(f"Loaded {len(df)} records from {csv_path}")
        
        while True:
            for index, row in df.iterrows():
                while not check_status():
                    time.sleep(2)
                # Synthesize Metadata
                user_id = fake.random_int(min=1, max=100)
                payload = row.to_dict()
                amount_val = payload.pop('Amount', None)
                if amount_val is None:
                    amount_val = payload.pop('amount', None)
                if amount_val is not None:
                    payload['amount'] = float(amount_val)
                else:
                    payload['amount'] = 0.0
                payload['transaction_id'] = fake.uuid4()
                payload['user_id'] = user_id
                payload['merchant_id'] = f"M_{fake.random_int(min=0, max=49):03d}"
                payload['ip_address'] = fake.ipv4()
                payload['lat'] = float(fake.latitude())
                payload['lon'] = float(fake.longitude())
                payload['timestamp'] = datetime.datetime.now().isoformat()
                
                # Encrypt
                payload_str = json.dumps(payload)
                final_payload = cipher.encrypt(payload_str.encode('utf-8')) if cipher else payload_str

                producer.produce(KAFKA_TOPIC, key=str(user_id), value=final_payload, callback=delivery_report)
                producer.poll(0)
                time.sleep(0.1) 
                
            logger.info("🔁 Replay Loop Complete. Restarting...")
            
    except Exception as e:
        logger.error(f"Replay failed: {e}. Switching to Generator.")
        return False

def run_generator_mode(producer, cipher, fake):
    logger.info("Starting producer in GENERATOR mode for topic %s", KAFKA_TOPIC)
    import random
    
    user_profiles = {}
    RING_IPS = [fake.ipv4() for _ in range(5)]
    RING_DEVICES = [fake.uuid4() for _ in range(5)]
    
    while True:
        while not check_status():
            time.sleep(2)
        user_id = fake.random_int(min=1, max=100)
        
        if user_id not in user_profiles:
            is_ring = random.random() < 0.1
            user_profiles[user_id] = {
                "avg_amount": random.uniform(20, 100) if is_ring else random.uniform(50, 500),
                "home_lat": float(fake.latitude()),
                "home_lon": float(fake.longitude()),
                "current_lat": float(fake.latitude()),
                "current_lon": float(fake.longitude()),
                "home_ip": random.choice(RING_IPS) if is_ring else fake.ipv4(),
                "device_id": random.choice(RING_DEVICES) if is_ring else fake.uuid4(),
                "is_ring": is_ring
            }
        
        profile = user_profiles[user_id]
        is_anomaly = random.random() < 0.05
        
        amount = 0.0
        fraud_type = "normal"
        
        if is_anomaly:
            rand = random.random()
            if rand < 0.2: amount = random.uniform(10000, 50000)
            elif rand < 0.4: amount = random.uniform(9000, 9999)
            elif rand < 0.6: amount = random.uniform(0.01, 1.0)
            elif rand < 0.8: amount = random.uniform(500, 3000)
            else: amount = random.uniform(5000, 10000)
        else:
            amount = max(1.0, random.normalvariate(profile["avg_amount"], 50))

        payload = {
            "transaction_id": fake.uuid4(),
            "user_id": user_id,
            "amount": round(amount, 2),
            "timestamp": datetime.datetime.now().isoformat(),
            "lat": profile["current_lat"],
            "lon": profile["current_lon"],
            "ip_address": profile["home_ip"],
            "device_id": profile["device_id"],
            "merchant_id": f"M_{fake.random_int(min=0, max=49):03d}"
        }
        
        payload_str = json.dumps(payload)
        final_payload = cipher.encrypt(payload_str.encode('utf-8')) if cipher else payload_str

        producer.produce(KAFKA_TOPIC, key=str(user_id), value=final_payload, callback=delivery_report)
        producer.poll(0)
        time.sleep(1)

def main():
    wait_for_kafka()
    
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'client.id': 'producer-1'}
    if "9093" in KAFKA_BOOTSTRAP_SERVERS and os.path.exists('/etc/kafka/secrets/kafka.server.truststore.jks'):
        conf.update({
            'security.protocol': 'SSL',
            'ssl.truststore.location': '/etc/kafka/secrets/kafka.server.truststore.jks',
            'ssl.truststore.password': 'kafka-secret',
            'ssl.keystore.location': '/etc/kafka/secrets/kafka.server.keystore.jks',
            'ssl.keystore.password': 'kafka-secret'
        })

    producer = Producer(conf)
    fake = Faker()
    
    if os.getenv("REPLAY_MODE", "false").lower() == "true":
        success = run_replay_mode(producer, cipher, fake)
        if not success:
            run_generator_mode(producer, cipher, fake)
    else:
        run_generator_mode(producer, cipher, fake)

if __name__ == "__main__":
    main()
