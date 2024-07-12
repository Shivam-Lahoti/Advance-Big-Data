from src.config.config import Config
import json
import os
import logging
import hashlib

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

def get_all_data():
    keys = redis_client.keys("plan:*")
    logger.debug(f"Retrieved keys: {keys}")
    all_data = []
    for key in keys:
        raw_data = redis_client.get(key)
        if raw_data:
            data = json.loads(raw_data)
            if 'data' in data:
                all_data.append(json.loads(data['data']))
    return all_data


def delete_data(key):
    logger.debug(f"Deleting data with key: plan:{key}")
    redis_client.delete(f"plan:{key}")

def patch_data(key, updates):
    data, etag = get_data(key)
    if data:
        updated_data = json.loads(data)
        
        
        if 'linkedPlanServices' in updates:
            if 'linkedPlanServices' not in updated_data:
                updated_data['linkedPlanServices'] = []
            for new_service in updates['linkedPlanServices']:
                updated_data['linkedPlanServices'].append(new_service)
            del updates['linkedPlanServices']  
        
       
        updated_data.update(updates)
        
        new_etag = hashlib.sha1(json.dumps(updated_data).encode()).hexdigest()
        
    
        if updated_data == json.loads(data):
            logger.debug(f"No changes made for key: {key}")
            return None, None
        
        save_data(key, json.dumps(updated_data), new_etag)
        return updated_data, new_etag
    return None, None

