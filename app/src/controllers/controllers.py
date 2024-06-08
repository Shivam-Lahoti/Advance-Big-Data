from flask import request, jsonify, Response
from jsonschema import validate, ValidationError
import json
from src.services.redis_service import save_data,get_data,delete_data
import logging
import os
import hashlib

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

schema_path = os.path.join('src','config','schema_data.json')
with open(schema_path,'r') as schema_file:
    schema_data=json.load(schema_file)


def create_data():
    try:
        data = request.get_json()
        logger.debug(f"Received data to save: {data}")
        validate(instance=data, schema=schema_data)

    
        existing_data, existing_etag = get_data(data['objectId'])
        if existing_data:
            logger.debug(f"Data with key {data['objectId']} already exists.")
            return jsonify({"error": "Data with this ID already exists."}), 409

        etag = hashlib.sha1(json.dumps(data).encode()).hexdigest()
        save_data(data['objectId'], json.dumps(data), etag)
        return jsonify({"message": "Data saved!"}), 201
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({"error": str(e)}), 400
    except KeyError as e:
        logger.error(f"Key error: {str(e)}")
        return jsonify({"error": f"Missing required property: {e}"}), 400

def read_data(key):
    logger.debug(f"Received request to get data with key: {key}")
    data, etag = get_data(key)
    if data:
        logger.debug(f"Data found for key: {key}, data: {data}, etag: {etag}")
        if request.headers.get('If-None-Match') == etag:
            return jsonify({"message": "No changes made"}), 304
        response = jsonify(json.loads(data))
        response.set_etag(etag)
        return response
    else:
        logger.debug(f"Data not found for key: {key}")
        return jsonify({"error": "Data not found"}), 404

def delete_data_by_key(key):
    logger.debug(f"Received request to delete data with key: {key}")
    data, etag = get_data(key)
    if data:
        delete_data(key)
        return jsonify({"message": "Data deleted"}), 200
    else:
        return jsonify({"message": "Data already deleted or does not exist"}), 404