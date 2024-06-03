from flask import Flask
from app.controllers import create_data,read_data,delete_data_by_key

def routes(app: Flask):
    app.add_url_rule('/data','create_data',create_data, methods=['POST'])
    app.add_url_rule('/data/<string:key>','read_data',read_data, methods=['GET'])
    app.add_url_rule('/data/<string:key>','delete_data',delete_data_by_key, methods=['DELETE'])