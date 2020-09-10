from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from werkzeug.local import LocalProxy
from api.db import get_db
from api.services.utils import token_required

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
bp = Blueprint('satinformations', __name__)


@bp.route('', methods=['GET'])
@token_required
def get_sat_info(data):
    """
    Endpoint for get info by app movil
    """
    sat_info = db.satInformations.find_one(filter={'_id': ObjectId(data["infoId"])}, projection={"user_id": 0, "id": 0})

    return jsonify({'status': 'success', 'data': json.loads(dumps(sat_info))}), 200


@bp.route('/updatesettings', methods=['PATCH'])
@token_required
def update_settings(data):
    """
    Endpoint to update settingsrfc.usocfdis by id
    """
    body = request.get_json()

    projec_usos_cfdi = {'_id': 0}

    # create dict to projection in catalogs Collection
    projec_usos_cfdi.update({'usocfdi.' + uso: 1 for uso in list(body['usocfdis'])})

    uso_cfdis = db.catalogs.find_one(filter={'type': 'cfdis'}, projection=projec_usos_cfdi)

    db.satInformations.update_one({'_id': ObjectId(data['infoId'])},
                                  {'$set': {
                                      'settingsrfc.usocfdis': uso_cfdis['usocfdi'],
                                      'settingsrfc.timerautomatic': body['timerautomatic'],
                                      'settingsrfc.timerequest': body['timerequest']
                                  }})

    # TODO: agregar un 500 por si pasa un problema
    return jsonify({'status': 'success'}), 204
