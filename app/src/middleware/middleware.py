import hashlib
from flask import request, Response


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
        