import bcrypt
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from api.services.users import convert_pwd
from api.db import get_db
from werkzeug.local import LocalProxy
from api.services.utils import create_token, token_required

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
bp = Blueprint('users', __name__)


@bp.route('/', methods=['GET'])
@token_required
def get_user(data):
    """
    Endpoint for get user by payload from token
    """
    # TODO: Validate if user isn't type of None
    user = list(db.users.aggregate([
        {
            '$match': {
                '_id': ObjectId(data['userId'])
            }
        }, {
            '$project': {
                'password': 0,
                'status': 0
            }
        }, {
            '$lookup': {
                'from': 'satInformations',
                'localField': '_id',
                'foreignField': 'user_id',
                'as': 'satInfo'
            }
        }, {
            '$unwind': {
                'path': '$satInfo'
            }
        }, {
            '$project': {
                '_id': 0,
                'satInfo._id': 0,
                'satInfo.user_id': 0,
                'satInfo.settingsrfc.usocfdis': 0
            }
        }
    ]))

    return jsonify({'status': 'success', 'data': {'user': json.loads(dumps(user[0]))}}), 200


@bp.route('/login', methods=['POST'])
def login():
    """
    Endpoint for login only email
    """
    body = request.get_json()

    user = db.users.find_one(filter={'email': body['email']},
                             projection={'password': 1, 'name': 1})

    sat_info = None
    if not user is None:
        sat_info = db.satInformations.find_one(filter={'user_id': user['_id']},
                                               projection={'user_id': 0})
    else:
        return jsonify({'status': 'error', 'message': 'El usuario no existe'}), 200

    if user is not None:
        if bcrypt.checkpw(bytes(body['password'].encode('utf-8')), user['password']):
            token = create_token({'userId': str(user['_id']),
                                  'satInfo': str(sat_info['_id']),
                                  'rfc': sat_info['rfc'],
                                  'exp': datetime.utcnow() + timedelta(minutes=5)}
                                 ).decode('utf-8')
            return jsonify({'status': 'success', 'data': {'token': token}}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Usuario o contraseña incorrectos'}), 200


@bp.route('', methods=['POST'])
def insert_user():
    """
    Endpoint for insert user
    """
    body = request.get_json()

    path_files = '/Users/geezylucas/Documents/Python37/datasensible/FIEL/'
    cer = path_files + 'HERMILA GUZMAN - 00001000000504205831.cer'
    key = path_files + 'HERMILA GUZMAN - Claveprivada_FIEL_PTI121203SZ0_20200615_144223.key'
    passkeyprivate = 'BEAUGENCY1964'

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
