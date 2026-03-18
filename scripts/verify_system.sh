#!/bin/bash
echo "=== Fraud Detection System Verification ==="

# Check all services are running
echo -e "\n[1] Checking Docker services..."
docker-compose ps

# Verify Kafka is receiving data
echo -e "\n[2] Checking Kafka messages..."
timeout 10 docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic transactions \
  --max-messages 3

# Check Spark processing
echo -e "\n[3] Verifying Spark processing..."
docker-compose logs spark-inference | grep "Processing batch" | tail -5

# Verify recent.json exists
echo -e "\n[4] Checking output file..."
docker exec spark-inference ls -lh /app/data/recent.json

# Test API endpoints
echo -e "\n[5] Testing API endpoints..."
echo "Live data:"
curl -s http://localhost:3000/api/live | jq '. | length'

echo "Fraud data:"
curl -s http://localhost:3000/api/fraud | jq '. | length'

echo "Producer status:"
curl -s http://localhost:3000/api/producer/status | jq .

echo "High Priority Alerts:"
curl -s http://localhost:3000/api/alerts/high_priority | jq '. | length'

echo -e "\n=== Verification Complete ==="
