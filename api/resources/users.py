import bcrypt
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
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
    ]))[0]

    return jsonify({'status': 'success', 'data': json.loads(dumps(user))}), 200


@bp.route('/login', methods=['POST'])
def login():
    """
    Endpoint for login only email
    """
    body = request.get_json()

    user = list(db.users.aggregate([
        {
            '$match': {
                'email': body['email']
            }
        }, {
            '$project': {
                'name': 0,
                'lastname': 0,
                'phonenumber': 0,
                'status': 0,
                'creationdate': 0
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
                'satInfo.rfc': 0,
                'satInfo.user_id': 0,
                'satInfo.settingsrfc': 0
            }
        }
    ]))

    if len(user):
        if bcrypt.checkpw(bytes(body['password'].encode('utf-8')), user[0]['password']):
            token = create_token({'userId': str(user[0]['_id']),
                                  'infoId': str(user[0]['satInfo']['_id']),
                                  'exp': datetime.utcnow() + timedelta(minutes=5)}
                                 ).decode('utf-8')
            return jsonify({'status': 'success', 'data': token}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Usuario o contraseña incorrectos'}), 401
    else:
        return jsonify({'status': 'error', 'message': 'El usuario no existe'}), 400


@bp.route('', methods=['POST'])
def singup():
    """
    Endpoint for insert user
    """
    body = request.get_json()

    db.users.create_index('email', unique=True)
    db.users.create_index('phonenumber', unique=True)
    db.satInformations.create_index('rfc', unique=True)

    pwd_hashed = convert_pwd(bytes(body['password'].encode('utf-8')))

    user_info = {
        'name': body['name'],
        'lastname': body['lastname'],
        'email': body['email'],
        'password': pwd_hashed,
        'status': True,
        'creationdate': datetime.now()
    }

    user_inserted_id = None
    info_inserted_id = None
    try:
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
    except DuplicateKeyError:
        db.users.delete_one(user_inserted_id)
        return jsonify({'status': 'error', 'message': 'Email o RFC ya existen'}), 400

    token = create_token({'userId': str(user_inserted_id),
                          'infoId': str(info_inserted_id),
                          'exp': datetime.utcnow() + timedelta(minutes=5)}
                         ).decode('utf-8')
    return jsonify({'status': 'success', 'data': token}), 201


# path_files = '/Users/geezylucas/Documents/Python37/datasensible/FIEL/'
# cer = path_files + 'HERMILA GUZMAN - 00001000000504205831.cer'
# key = path_files + 'HERMILA GUZMAN - Claveprivada_FIEL_PTI121203SZ0_20200615_144223.key'
# passkeyprivate = 'BEAUGENCY1964'

# cer_der = open(cer, 'rb').read()
# key_der = open(key, 'rb').read()
