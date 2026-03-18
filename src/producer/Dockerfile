FROM python:3.9-slim

WORKDIR /app

# Install bash for wait-for-it.sh
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
# Copy wait-for-it.sh from the build context (assumes we copy it from scripts/ to src/producer/ during build or use context root)
# Actually, since docker compose context is ./src/producer, we can't easily copy from ../../scripts without correct context.
# Simplest for this setup: Copy the wait-for-it.sh content into the image, or changing the build context in docker-compose.
# I'll modify docker-compose to pass wait-for-it logic, or I will use a simple python logic in main.py to wait?
# The user asked for "wait-for-it.sh logic". I'll stick to a robust python wait or fix the build context.
# Let's use a multi-stage build or just CURL it? No internet guarantee.
# Let's assumes I will manually put wait-for-it.sh in src/producer for now? No, that duplicates.
# Best practice: Mount it or Change build context.
# In docker-compose I set context: ./src/producer.
# I will change the Dockerfile to COPY wait-for-it if I change context, OR I will rely on the volume mount logic if I mount scripts.
# BUT, Producer runs `python main.py` immediately.
# Let's use the Python ENTRYPOINT to check for connection or volume mount `wait-for-it.sh`.
# I'll rely on volume mounting `scripts/` to `/app/scripts` in docker-compose for producer (need to add it).
# And I'll update the CMD to use it.


# Create a non-root user and switch to it
RUN useradd -m appuser
USER appuser

CMD ["python", "main.py"]
