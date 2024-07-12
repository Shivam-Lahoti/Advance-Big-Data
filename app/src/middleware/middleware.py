import hashlib
from flask import request, Response, jsonify
from jose import jwt, JWTError
from functools import wraps
import requests
import logging
from src.config.config import Config

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def generate_etag(data):
    return hashlib.sha1(data).hexdigest()

def etag_middleware(app):
    @app.after_request
    def add_etag(response):
        if response.status_code == 200 and response.response:
            data = b''.join(response.response)
            etag = generate_etag(data)
            response.set_etag(etag)

            if request.headers.get('If-None-Match') == etag:
                response = Response(status=304)
            
        return response
    


def get_google_public_keys():
    response = requests.get(Config.GOOGLE_DISCOVERY_URL)
    jwks_uri = response.json()["jwks_uri"]
    return requests.get(jwks_uri).json()

def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]
        if not token:
            logger.error("Token is missing")
            return jsonify({'message': 'Token is missing!'}), 401
        
        try:
            logger.debug(f"Received token: {token}")
            jwks = get_google_public_keys()
            logger.debug(f"JWKS: {jwks}")

            unverified_header = jwt.get_unverified_header(token)
            logger.debug(f"Unverified JWT header: {unverified_header}")
            rsa_key = {}
            for key in jwks["keys"]:
                if key["kid"] == unverified_header["kid"]:
                    rsa_key = {
                        "kty": key["kty"],
                        "kid": key["kid"],
                        "use": key["use"],
                        "n": key["n"],
                        "e": key["e"]
                    }
            if rsa_key:
                payload = jwt.decode(token, rsa_key, algorithms=['RS256'], audience=Config.GOOGLE_CLIENT_ID, issuer='https://accounts.google.com', options={"verify_at_hash": False})
                logger.debug(f"Token payload: {payload}")
            else:
                logger.error("RSA key not found")
                return jsonify({'message': 'RSA key not found'}), 401
            
        except JWTError as e:
            logger.error(f"Token is invalid: {str(e)}")
            return jsonify({'message': 'Token is invalid'}), 401
        
        return f(*args, **kwargs)
    
    return decorated_function