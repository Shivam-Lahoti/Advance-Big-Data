from flask import request, jsonify
from jsonschema import validate, ValidationError
import json
from app.services.redis_service import save_data,get_data,delete_data
from app.models import DataModel
import os

schema_path = os.path.join(os.path.dirname('app\schemas\use case.txt'),'schemas','schema_data.json')
with open(schema_path,'r') as schema_file:
    schema_data=json.load(schema_file)

def create_data():
    try:
        data=request.get_json()
        validate(instance=data, schema=save_data)
        data_model= DataModel(**data)
        save_data(data_model.key , json.dumps(data_model.value))
        return jsonify({"message: data saved!!"}),201
    except ValidationError as e:
        return jsonify({"error",str(e)}),400
    
def read_data(key):
    data= get_data(key)
    if data:
        return jsonify({"key": key, "value": json.loads(data)}), 200
    else:
        return jsonify({"error": "data not found"}),404
    
def delete_data_by_key(key):
    delete_data(key)
    return jsonify({"message":"Data deleted"}), 200