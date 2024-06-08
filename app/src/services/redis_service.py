from src.config.config import Config
import json
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

redis_client= Config.connect_redis()



def save_data(key, value, etag):
    logger.debug(f"Saving data with key: plan:{key}, value: {value}, etag: {etag}")
    redis_client.set(f"plan:{key}", json.dumps({'data': value, 'etag': etag}))

def get_data(key):
    raw_data = redis_client.get(f"plan:{key}")
    logger.debug(f"Retrieved raw data for key: plan:{key}: {raw_data}")
    if raw_data:
        data = json.loads(raw_data)
        if 'data' in data and 'etag' in data:
            return data['data'], data['etag']
        else:
            logger.error(f"Data format error for key: plan:{key}, data: {data}")
            return None, None
    return None, None

def delete_data(key):
    logger.debug(f"Deleting data with key: plan:{key}")
    redis_client.delete(f"plan:{key}")

