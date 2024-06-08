from src.config.config import Config

redis_client= Config.connect_redis()

def save_data(key,value):
    redis_client.set(f"plan:{key}", value)

def get_data(key):
    return redis_client.get(f"plan:{key}")

def delete_data(key):
    redis_client.delete(f"plan:{key}")
