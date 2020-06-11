from run import app
from database.db import db
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from services.requests_cfdis import get_limit_requests, insert_request_func, insert_request_automatically, create_jobs_download


bp = Blueprint('requestscfdis', __name__)


@bp.route('/getrequests/<info_id>', methods=['GET', 'POST'])
def get_requests(info_id):
    """
    Endpoint for search with filters (dateIni, dateFin) records in requests Cfdis 
    and return data and pagination
    """
    body = None
    if request.method == 'POST':
        body = request.get_json()

    page_size = int(request.args.get('pagesize'))
    page_num = int(request.args.get('pagenum'))

    requests_cfdis, data_pagination = get_limit_requests(page_size=page_size,
                                                         page_num=page_num,
                                                         info_id=info_id,
                                                         filters=body
                                                         )
    return jsonify({'status': 'success', 'data': {
        'dataPagination': json.loads(data_pagination),
        'requests': json.loads(requests_cfdis)
    }}), 200


@bp.route('/<request_id>', methods=['GET'])
def get_request(request_id):
    """
    Endpoint for search only record in requests Cfdis 
    """
    request_cfdi = db.requestsCfdis.find_one(filter={'_id': request_id},
                                             projection={'downloads': 0, 'info_id': 0})
    return jsonify({'status': 'success', 'data': json.loads(dumps(request_cfdi))}), 200


@bp.route('', methods=['POST'])
def insert_request_manually():
    """
    Endpoint for request and insert in requests Cfdis 
    """
    body = request.get_json()

    date_ini = datetime.strptime(
        body['dateIni'], '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
    date_fin = datetime.strptime(
        body['dateFin'], '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)

    applicant = db.satInformations.find_one(filter={'_id': ObjectId(body['infoId'])},
                                            projection={'rfc': 1})

    result_insert_request = insert_request_func(info_id=applicant['_id'],
                                                date_ini=date_ini,
                                                date_fin=date_fin,
                                                type_request=body['typeRequest'],
                                                auto_or_man='m',
                                                rfc_applicant=applicant['rfc'])

    if result_insert_request is not None:
        return jsonify({'status': 'success', 'data': {'_id': result_insert_request, 'message': 'OK'}}), 201
    else:
        return jsonify({'status': 'error', 'data': {'_id': None, 'message': 'Error'}}), 200


@bp.route('/requestsauto/<info_id>', methods=['PATCH'])
def update_request_automatically(info_id):
    """
    Endpoint for activate downloads automatically
    """
    body = request.get_json()

    db.satInformations.update_one({'_id': ObjectId(info_id)},
                                  {'$set': {
                                      'settingsrfc.timerautomatic': body['timerautomatic']
                                  }})

    pending_req_receptor = list(db.requestsCfdis.find(filter={'info_id': ObjectId(info_id),
                                                              'typerequest': 'r',
                                                              'request': 'a',
                                                              'status': False},
                                                      sort=list({
                                                          'daterequest': -1
                                                      }.items())))

    pending_req_emisor = list(db.requestsCfdis.find(filter={'info_id': ObjectId(info_id),
                                                            'typerequest': 'e',
                                                            'request': 'a',
                                                            'status': False},
                                                    sort=list({
                                                        'daterequest': -1
                                                    }.items())))

    if len(pending_req_receptor) != 0 or len(pending_req_emisor) != 0:
        applicant = db.satInformations.find_one(filter={'_id': ObjectId(info_id)},
                                                projection={'rfc': 1})
        for pending in pending_req_receptor:
            create_jobs_download(request_id=pending['_id'],
                                 info_id=applicant['_id'],
                                 rfc_applicant=applicant['rfc'])

        for pending in pending_req_emisor:
            create_jobs_download(request_id=pending['_id'],
                                 info_id=applicant['_id'],
                                 rfc_applicant=applicant['rfc'])

    if body['timerautomatic']:
        # Cambiar a dias
        app.apscheduler.add_job(func=insert_request_automatically,
                                trigger='interval',
                                args=[info_id],
                                minutes=5,
                                id=info_id)
    else:
        app.apscheduler.remove_job(info_id)

    return jsonify({'status': 'success'}), 200
