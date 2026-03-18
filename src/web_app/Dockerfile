FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install flask pandas openpyxl redis shap xgboost scikit-learn matplotlib

COPY . .


# Create a non-root user and switch to it
RUN useradd -m appuser
USER appuser

CMD ["python", "app.py"]
