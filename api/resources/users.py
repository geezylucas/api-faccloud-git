import bcrypt
from datetime import datetime
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from api.services.users import convert_pwd
from api.db import get_db
from werkzeug.local import LocalProxy

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
bp = Blueprint('users', __name__)


@bp.route('/login', methods=['POST'])
def login():
    """
    Endpoint for login only email
    """
    body = request.get_json()

    user = db.users.find_one(filter={'email': body['email']},
                             projection={'_id': 0, 'password': 1})

    if user is not None:
        if bcrypt.checkpw(bytes(body['password'].encode('utf-8')), user['password']):
            return jsonify({'status': 'success'}), 200
        else:
            print("does not match")
            return jsonify({'status': 'error'}), 401


@bp.route('', methods=['POST'])
def insert_user():
    """
    Endpoint for insert user
    """
    body = request.get_json()

    path_files = '/Users/geezylucas/Documents/Python37/datasensible/FIEL/'
    cer = path_files + 'SUSANA GUZMAN - 00001000000504205545.cer'
    key = path_files + 'SUSANA GUZMAN - Claveprivada_FIEL_CAJF760331FU1_20200615_142206.key'
    passkeyprivate = 'Cz1GvqqWR3'

    cer_der = open(cer, 'rb').read()
    key_der = open(key, 'rb').read()

    db.users.create_index('email', unique=True)
    db.users.create_index('phonenumber', unique=True)
    db.satInformations.create_index('rfc', unique=True)

    pwd_hashed = convert_pwd(bytes(body['password'].encode('utf-8')))

    user_info = {
        'name': body['name'],
        'lastname': body['lastname'],
        'email': body['email'],
        'phonenumber': body['phonenumber'],
        'password': pwd_hashed,
        'status': True,
        'creationdate': datetime.now()
    }

    user_inserted_id = db.users.insert_one(user_info).inserted_id

    sat_info = {
        'user_id': user_inserted_id,
        'rfc': body["rfc"],
        'settingsrfc': {
            'timerautomatic': False,
            'timerequest': 0,
            'usocfdis': {}
        }
    }

    info_inserted_id = db.satInformations.insert_one(sat_info).inserted_id

    efirma = {
        "info_id": info_inserted_id,
        "cer": cer_der,
        "key": key_der,
        "passkeyprivate": passkeyprivate
    }

    db.efirmas.insert_one(efirma)

    return jsonify({
        'status': 'success',
        'data': {
            '_id': 'json.loads(dumps({}))'
        }}), 201
