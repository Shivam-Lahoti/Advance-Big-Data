from flask import Flask
from src.controllers.controllers import create_data,read_data,delete_data_by_key,read_all_data

def register_routes(app: Flask):
    app.add_url_rule('/plan','create_data',create_data, methods=['POST'])
    app.add_url_rule('/plan/<string:key>','read_data',read_data, methods=['GET'])
    app.add_url_rule('/plan/','read_all_data',read_all_data,methods=['GET'])
    app.add_url_rule('/plan/<string:key>','delete_data',delete_data_by_key, methods=['DELETE'])
