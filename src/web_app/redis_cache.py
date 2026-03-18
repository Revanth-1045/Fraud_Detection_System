import redis
import json
import os

# Connect to Redis
# Service name in docker-compose is 'redis', port 6379
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
except Exception as e:
    print(f"Warning: Could not connect to Redis: {e}")
    redis_client = None

def cache_user_stats(user_id, stats):
    """Cache user transaction statistics"""
    if not redis_client:
        return
        
    try:
        redis_client.setex(
            f"user:{user_id}:stats",
            3600,  # 1 hour TTL
            json.dumps(stats)
        )
    except Exception as e:
        print(f"Error caching user stats: {e}")

def get_user_stats(user_id):
    """Retrieve cached user stats"""
    if not redis_client:
        return None
        
    try:
        data = redis_client.get(f"user:{user_id}:stats")
        return json.loads(data) if data else None
    except Exception as e:
        print(f"Error reading from Redis: {e}")
        return None
