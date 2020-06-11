from flask import Blueprint, request, jsonify
from database.db import db
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError

bp = Blueprint('satinformations', __name__)


# TODO: Cambiar info_id tengamos users y el user_id este en la collection satInformations
@bp.route('/<info_id>', methods=['GET'])
def get_sat_info(info_id):
    """
    Endpoint for get info by app movil
    """
    sat_info = db.satInformations.find_one({'_id': ObjectId(info_id)})
    return jsonify({'status': 'success', 'data': json.loads(dumps(sat_info))}), 200


@bp.route('', methods=['POST'])
def insert_sat_info():
    """
    Function for insert new record
    """
    body = request.get_json()

    info = {
        'rfc': body["rfc"],
        'settingsrfc': {
            'timerautomatic': False,
            'timerequest': 0,
            'usocfdis': {}
        }
    }

    try:
        db.satInformations.create_index('rfc', unique=True)
        inserted_id = db.satInformations.insert_one(info).inserted_id
        return jsonify({
            'status': 'success',
            'data': {
                '_id': json.loads(dumps(inserted_id))
            }}), 200
    except DuplicateKeyError:
        return jsonify({'status': 'error', 'message': "Duplicate key error collection"}), 400


@bp.route('/usocfdis/<info_id>', methods=['PATCH'])
def update_uso_cfdis_in_rfc(info_id):
    """
    Endpoint to update settingsrfc.usocfdis by id
    """
    body = request.get_json()

    list_uso_cfdis = list(body['usocfdis'])

    projection = {'_id': 0}

    for uso in list_uso_cfdis:
        projection.update({'usocfdi.' + uso: 1})

    uso_cfdis = db.catalogs.find_one(filter={'type': 'cfdis'},
                                     projection=projection)

    update_uso_cfdis = db.satInformations.update_one({'_id': ObjectId(info_id)},
                                                     {'$set': {
                                                         'settingsrfc.usocfdis': uso_cfdis['usocfdi']
                                                     }}
                                                     ).modified_count

    if update_uso_cfdis != 0:
        return jsonify({'status': 'success'}), 204
    else:
        return jsonify({'status': 'error'}), 500
