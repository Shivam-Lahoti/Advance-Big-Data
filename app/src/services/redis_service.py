from src.config.config import Config
import json
import logging
import hashlib

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

redis_client = Config.connect_redis()

class DataService:
    def __init__(self, redis_client, es):
        self.redis_client = redis_client
        self.es = es
        self.INDEX_NAME = "your_index_name"  # Replace with your Elasticsearch index name

    def save_data(self, data, update=False):
        def save_object(obj, object_type, parent_key=None):
            if "objectId" in obj:
                object_id = obj["objectId"]
                redis_key = f"{object_type}:{object_id}"
                redis_value = json.dumps(obj)
                self.redis_client.set(redis_key, redis_value)
                logger.debug(f"Saved object with key: {redis_key}, value: {redis_value}")
                if parent_key:
                    # Save the reference to this object in the parent
                    self.redis_client.sadd(parent_key, redis_key)
                return redis_key
            return None

        def save_to_elasticsearch(obj, object_type, parent_id=None):
            if "objectId" in obj:
                object_id = obj["objectId"]
                es_doc = obj.copy()
                es_doc["join_field"] = {"name": object_type}
                if parent_id:
                    es_doc["join_field"]["parent"] = parent_id

                self.es.index(index=self.INDEX_NAME, id=object_id, body=es_doc, routing=parent_id, doc_type="_doc")
                logger.debug(f"Indexed {object_type} with ID: {object_id} in Elasticsearch")

        if isinstance(data, dict):
            # Handle the root object
            root_object_type = "plan"
            root_object_key = save_object(data, root_object_type)
            save_to_elasticsearch(data, root_object_type)

            # Process nested objects
            for key, value in data.items():
                if key == "objectId":
                    continue
                elif isinstance(value, dict):
                    nested_key = save_object(value, key, root_object_key)
                    save_to_elasticsearch(value, key, data["objectId"])
                    if nested_key:
                        self.save_data(value, update)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            nested_key = save_object(item, key, root_object_key)
                            save_to_elasticsearch(item, key, data["objectId"])
                            if nested_key:
                                self.save_data(item, update)
        
        return root_object_key




def get_data(key):
    # Retrieve the set members (references)
    object_ids = redis_client.smembers(key)
    logger.debug(f"Retrieved object references for key: {key}: {object_ids}")

    if object_ids:
        # Return only the object IDs (references)
        return list(object_ids)
    else:
        # If no references found, return the actual data
        raw_data = redis_client.get(f"plan:{key}")
        if raw_data:
            data = json.loads(raw_data)
            logger.debug(f"Retrieved full data for key: {key}: {data}")
            return data
        else:
            logger.error(f"No data found for key: {key}")
            return None




def get_all_data():
    keys = redis_client.keys("plan:*")
    logger.debug(f"Retrieved keys: {keys}")
    all_data = []
    for key in keys:
        raw_data = redis_client.get(key)
        if raw_data:
            data = json.loads(raw_data)
            all_data.append(data)
    return all_data

def delete_data(key):
    logger.debug(f"Deleting data with key: {key}")
    redis_client.delete(f"{key}")

def patch_data(key, updates):
    data, etag = get_data(key)
    if not data:
        return None, None

    existing_data = data
    updated_data = merge_dicts(existing_data, updates)

    new_etag = hashlib.sha1(json.dumps(updated_data, sort_keys=True).encode()).hexdigest()

    if json.dumps(updated_data, sort_keys=True) == json.dumps(existing_data, sort_keys=True):
        return None, None

    save_data(updated_data, key)
    return updated_data, new_etag

def merge_dicts(original, updates):
    for key, value in updates.items():
        if key in original:
            if isinstance(value, dict) and isinstance(original[key], dict):
                merge_dicts(original[key], value)
            elif isinstance(value, list) and isinstance(original[key], list):
                original[key].extend(value)
            else:
                original[key] = value
        else:
            original[key] = value
    return original
