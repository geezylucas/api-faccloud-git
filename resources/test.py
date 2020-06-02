from flask import Blueprint, request, jsonify
import json

bp = Blueprint('test', __name__)

@bp.route('/')
def test_connection():
    return jsonify({'greeting': 'Hello world!'}), 200
