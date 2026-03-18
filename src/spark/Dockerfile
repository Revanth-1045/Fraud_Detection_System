FROM python:3.9-slim-bullseye

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install PySpark and depencencies
# Install dependencies first (better caching)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=1000 \
    mlflow==2.7.1 \
    xgboost==2.0.0 \
    pandas \
    openpyxl \
    "numpy<2.0.0" \
    redis \
    boto3 \
    psycopg2-binary

# Install PySpark separately (large file, prone to timeouts)
RUN pip install --no-cache-dir --default-timeout=2000 pyspark==3.5.0

# Copy the python script
COPY streaming_job.py .

CMD ["python", "streaming_job.py"]
