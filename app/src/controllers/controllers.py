from flask import request, jsonify, Response
from jsonschema import validate, ValidationError, Draft7Validator
from elasticsearch import Elasticsearch
import json
from src.services.redis_service import  get_data,get_all_data, delete_data, patch_data, save_data
from src.middleware.middleware import require_auth
import logging
import os
import hashlib
from src.queue_service import publish_message

#es = Elasticsearch(hosts={"http://localhost:9200/"})

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

schema_path = os.path.join('src','config','schema_data.json')
with open(schema_path,'r') as schema_file:
    schema_data=json.load(schema_file)

@require_auth
def create_data():
    try:
        # Get the JSON payload from the request
        data = request.get_json()
        logger.debug(f"Received data to save: {data}")

        # Validate the JSON data against the schema
        validate(instance=data, schema=schema_data)

        try:
            # Publish message to RabbitMQ with the actual data
            publish_message(json.dumps({'action': 'create', 'data': data}))
            logger.info("Message published to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to publish message to RabbitMQ: {str(e)}")
            return jsonify({"error": "Failed to publish message to RabbitMQ"}), 500

        try:
            # Recursively save all objectIds and their associated data
            save_data(data)
        except Exception as e:
            logger.error(f"Failed to save data: {str(e)}")
            return jsonify({"error": "Failed to save data"}), 500

        # Prepare the response without the ETag
        response = jsonify({"message": "Data saved!"})
        response.status_code = 201
        logger.info(f"Data saved successfully for key {data.get('objectId')}")

        return response

    except ValidationError as e:
        # Handle validation errors
        logger.error(f"Validation error: {str(e)}")
        return jsonify({"error": "Invalid data format."}), 400
    except KeyError as e:
        # Handle missing required property errors
        logger.error(f"Key error: {str(e)}")
        return jsonify({"error": f"Missing required property: {str(e)}"}), 400
    except Exception as e:
        # Catch any other exceptions and log them
        logger.error(f"An unexpected error occurred: {str(e)}")
        return jsonify({"error": "An unexpected error occurred."}), 500
    

@require_auth
def read_data(key):
    logger.info("Fetching plan data")

    # Check If-None-Match header
    if_none_match = request.headers.get('If-None-Match')
    logger.debug(f"If-None-Match header: {if_none_match}")

    # Fetch the latest data from the data store
    data, _ = get_data(key)
    if not data:
        logger.info("No plan found")
        return Response(
            response=json.dumps({
                "status": "failed",
                "message": "No plan found"
            }),
            status=404,
            mimetype="application/json"
        )

    # Parse the data
    current_data = json.loads(data)
    logger.debug(f"Current data: {current_data}")

    # Calculate a new ETag based on the latest data
    serialized_data = json.dumps(current_data, sort_keys=True).encode()
    new_etag = hashlib.sha1(serialized_data).hexdigest()
    logger.debug(f"Serialized data for ETag: {serialized_data}")
    logger.debug(f"Generated new ETag for key {key}: {new_etag}")

    # Check if the ETag matches the If-None-Match header
    if if_none_match and if_none_match == new_etag:
        logger.warning("Content not modified")
        return Response(status=304)

    # Prepare the response with the data and set the new ETag
    response = Response(
        response=json.dumps(current_data),
        status=200,
        mimetype="application/json"
    )
    response.set_etag(new_etag)
    etag_value = response.headers.get("Etag").strip('\"')
    logger.info(f"Saved ETag value: {etag_value}")

    return response


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

    # Validate updates against schema
    errors = validate_patch(updates, schema_data)
    if errors:
        error_messages = [error.message for error in errors]
        logger.error(f"Validation errors: {error_messages}")
        return jsonify({"error": error_messages}), 400

    # Get existing data and ETag
    data, etag = get_data(key)
    if not data:
        logger.debug(f"No data found to patch with key: {key}")
        return jsonify({"error": "No data found to patch"}), 404

    # Check If-Match header before applying updates
    if_match = request.headers.get('If-Match')
    logger.debug(f"If-Match header: {if_match}, Current ETag: {etag}")
    if if_match and if_match != etag:
        logger.debug(f"If-Match header matches the current ETag for key: {key}, returning 412 Precondition Failed")
        return jsonify({"error": "Precondition Failed"}), 412

    # Patch the data
    updated_data, new_etag = patch_data(key, updates)
    if updated_data is None and new_etag is None:
        return jsonify({"message": "No changes made"}), 304
    elif updated_data:
        response = jsonify(updated_data)
        response.set_etag(new_etag)
        logger.debug(f"Updated ETag set in response for key {key}: {response.get_etag()}")


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


