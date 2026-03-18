#!/bin/bash
set -e

echo "Submitting Spark Job with explicitly low memory configuration..."

echo "Runtime patching: Installing boto3..."
pip install boto3

echo "Submitting Spark Job..."
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --total-executor-cores 2 \
  --conf spark.driver.host=spark-inference \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/streaming_job.py
