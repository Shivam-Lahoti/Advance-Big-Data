from flask import Flask
from src.routes import register_routes
from src.middleware.middleware import etag_middleware, require_auth

def create_app():
    app = Flask(__name__)
    register_routes(app) 
    etag_middleware(app)
    require_auth(app)

    return app
