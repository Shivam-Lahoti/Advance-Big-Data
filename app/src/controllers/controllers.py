from flask import request, jsonify
from jsonschema import validate, ValidationError
import json
from src.services.redis_service import save_data,get_data,delete_data

import os

schema_path = os.path.join('src','config','schema_data.json')
with open(schema_path,'r') as schema_file:
    schema_data=json.load(schema_file)

def create_data():
    try:
        data = request.get_json()
        validate(instance=data, schema=schema_data)
        save_data(data['objectId'], json.dumps(data))
        return jsonify({"message": "Data saved!"}), 201
    except ValidationError as e:
        return jsonify({"error": str(e)}), 400
    except KeyError as e:
        return jsonify({"error": f"Missing required property: {e}"}), 400

def read_data(key):
    data = get_data(key)
    if data:
        return jsonify({"key": key, "value": json.loads(data)}), 200
    else:
        return jsonify({"error": "Data not found"}), 404

def delete_data_by_key(key):
    delete_data(key)
    return jsonify({"message": "Data deleted"}), 200