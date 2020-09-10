from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from werkzeug.local import LocalProxy
from api.db import get_db
from api.services.utils import token_required

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
bp = Blueprint('requestscfdis', __name__)


@bp.route('/getrequests', methods=['GET', 'POST'])
@token_required
def get_requests(data):
    """
    Endpoint for search with filters (dateIni, dateFin) records in requests Cfdis 
    and return data and pagination
    """
    from api.services.requests_cfdis import pagination_requests
    body = None

    if request.method == 'POST':
        body = request.get_json()

    page_size = int(request.args.get('pagesize'))
    page_num = int(request.args.get('pagenum'))

    requests_cfdis, data_pagination = pagination_requests(page_size=page_size,
                                                          page_num=page_num,
                                                          info_id=data['infoId'],
                                                          filters=body)
    return jsonify({'status': 'success', 'data': {
        'dataPagination': json.loads(data_pagination),
        'requests': json.loads(requests_cfdis)
    }}), 200


@bp.route('/<request_id>', methods=['GET'])
@token_required
def get_request(data, request_id):
    """
    Endpoint for search only record in requests Cfdis 
    """
    request_cfdi = db.requestsCfdis.find_one(filter={'_id': request_id}, projection={'info_id': 0})

    return jsonify({'status': 'success', 'data': json.loads(dumps(request_cfdi))}), 200


@bp.route('/insertmanually', methods=['POST'])
@token_required
def insert_request_manually(data):
    """
    Endpoint for request and insert in requests Cfdis 
    """
    from api.services.requests_cfdis import insert_request_func
    body = request.get_json()

    applicant = db.satInformations.find_one(filter={'_id': ObjectId(data['infoId'])}, projection={'rfc': 1})

    result_insert_request = insert_request_func(info_id=applicant['_id'],
                                                request_id='',
                                                date_ini=body['dateIni'],
                                                date_fin=body['dateFin'],
                                                type_request=body['typeRequest'],
                                                auto_or_man='m')

    return jsonify({'status': 'success', 'data': result_insert_request}), 201
