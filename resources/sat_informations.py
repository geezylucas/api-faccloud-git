from flask import Blueprint, request, jsonify
from database.db import db
from bson.json_util import dumps
from pymongo.errors import DuplicateKeyError

bp = Blueprint('satinformations', __name__)


@bp.route('', methods=['GET', 'POST'])
def satinformations():
    """
    Function to send all satInformations data or insert new record
    """
    if request.method == 'POST':
        body = request.get_json()

        try:
            info = {"rfc": body["rfc"]}
            db.satInformations.create_index('rfc', unique=True)
            result = dumps(db.satInformations.insert_one(info).inserted_id)

            return jsonify({'status': 'success', 'data': {'_id': result}}), 200
        except KeyError:
            return jsonify({'status': 'error', 'message': "rfc isn't in body"}), 400
        except DuplicateKeyError:
            return jsonify({'status': 'error', 'message': "Duplicate key error collection"}), 400

    else:
        return jsonify({'status': 'success', 'data': dumps(db.satInformations.find({}))}), 200
