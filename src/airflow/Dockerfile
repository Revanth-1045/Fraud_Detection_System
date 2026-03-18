FROM apache/airflow:2.7.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --default-timeout=1000 \
    apache-airflow-providers-cncf-kubernetes \
    apache-airflow-providers-celery \
    apache-airflow-providers-postgres \
    mlflow==2.7.1 \
    xgboost==2.0.0 \
    scikit-learn==1.3.1 \
    pandas \
    faker==19.6.2 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-amazon \
    boto3 \
    optuna==3.4.0 \
    imbalanced-learn==0.11.0 \
    lightgbm==4.1.0 \
    redis==5.0.1 \
    cryptography==41.0.3
