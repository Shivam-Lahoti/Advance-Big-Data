from flask import request, jsonify, Response
from jsonschema import validate, ValidationError, Draft7Validator
import json
from src.services.redis_service import save_data,get_data,get_all_data, delete_data, patch_data
from src.middleware.middleware import require_auth
import logging
import os
import hashlib

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

schema_path = os.path.join('src','config','schema_data.json')
with open(schema_path,'r') as schema_file:
    schema_data=json.load(schema_file)

@require_auth
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
        return jsonify({"error": "Invalid"}), 400
    except KeyError as e:
        logger.error(f"Key error: {str(e)}")
        return jsonify({"error": f"Missing required property: {e}"}), 400

@require_auth
def read_data(key):
    logger.debug(f"Received request to get data with key: {key}")
    data, etag = get_data(key)
    if data:
        logger.debug(f"Data found for key: {key}, data: {data}, etag: {etag}")
        if_none_match = request.headers.get('If-None-Match')
        if if_none_match and if_none_match == etag:
            logger.debug("ETag matches, returning 304 Not Modified")
            return Response(status=304)
        else:
            response = jsonify(json.loads(data))
            response.set_etag(etag)
            return response
    else:
        logger.debug(f"Data not found for key: {key}")
        return jsonify({"error": "Data not found"}), 404
@require_auth    
def read_all_data():
    logger.debug(f"Received request to get all data")
    all_data = get_all_data()
    return jsonify(all_data), 200

@require_auth
def delete_data_by_key(key):
    logger.debug(f"Received request to delete data with key: {key}")
    data, etag = get_data(key)
    if data:
        delete_data(key)
        return jsonify({"message": "Data deleted"}), 200
    else:
        return jsonify({"message": "Data already deleted or does not exist"}), 404
    
@require_auth
def patch_data_by_key(key):
    updates = request.get_json()
    errors = validate_patch(updates, schema_data)
    if errors:
        error_messages = [error.message for error in errors]
        logger.error(f"Validation errors: {error_messages}")
        return jsonify({"error": error_messages}), 400

    data, etag = get_data(key)
    if not data:
        logger.debug(f"No data found to patch with key: {key}")
        return jsonify({"error": "No data found to patch"}), 404

    updated_data, new_etag = patch_data(key, updates)
    if updated_data is None and new_etag is None:
        return jsonify({"message": "No changes made"}), 304
    elif updated_data:
        response = jsonify(updated_data)
        response.set_etag(new_etag)
        return response
    else:
        return jsonify({"error": "Data not found"}), 404

def validate_patch(instance, schema):

    validator = Draft7Validator(schema)
    errors = []
    for field, value in instance.items():
        field_schema = schema['properties'].get(field)
        if field_schema:
            try:
                validate(instance={field: value}, schema={'type': 'object', 'properties': {field: field_schema}})
            except ValidationError as e:
                errors.append(e)
    return errors