from app.config import get_redis_connection

redis_client= get_redis_connection()

def save_data(key,value):
    redis_client.set(key, value)

def get_data(key):
    return redis_client.get(key)

def delete_data(key):
    redis_client.delete(key)
