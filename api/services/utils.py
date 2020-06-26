from jwt import decode, encode
from functools import wraps
from flask import request, jsonify, current_app


def token_required(f):
    """
    Wrapped for endpoints which need to auth
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        token = request.headers.get('Authorization', None)
        if token is None:
            return jsonify({'status': 'error', 'message': 'A valid token is missing'}), 401
        try:
            data = decode(jwt=token[7:],
                          key=current_app.config['SECRET_KEY'],
                          algorithms='HS256')
        except:
            return jsonify({'status': 'error', 'message': 'Token is invalid'}), 401

        return f(data, *args,  **kwargs)
    return wrapped


def create_token(payload: dict) -> bytes:
    """
    Function for create token
    """
    return encode(payload=payload,
                  key=current_app.config['SECRET_KEY'],
                  algorithm='HS256')
