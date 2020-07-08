from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from werkzeug.local import LocalProxy
from api.db import get_db
from api.services.utils import token_required

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
bp = Blueprint('satinformations', __name__)


@bp.route('/<info_id>', methods=['GET'])
@token_required
def get_sat_info(data, info_id):
    """
    Endpoint for get info by app movil
    """
    # TODO: Cambiar info_id tengamos users y el user_id este en la collection satInformations
    sat_info = db.satInformations.find_one({'_id': ObjectId(info_id)})
    return jsonify({'status': 'success', 'data': json.loads(dumps(sat_info))}), 200


@bp.route('/updatesettings/<info_id>', methods=['PATCH'])
def update_settings(info_id):
    """
    Endpoint to update settingsrfc.usocfdis by id
    """
    from api.services.requests_cfdis import create_jobs_requests, create_jobs_download, \
        get_job, delete_job

    body = request.get_json()
    applicant = db.satInformations.find_one(filter={'_id': ObjectId(info_id)},
                                            projection={'rfc': 1, 'settingsrfc.timerequest': 1})

    job_request = get_job(info_id)
    # BEGIN downdload automatically
    if bool(body['timerautomatic']):
        pending_req_receptor = list(db.requestsCfdis.find(filter={'info_id': ObjectId(applicant['_id']),
                                                                  'typerequest': 'r',
                                                                  'status': False},
                                                          sort=list({
                                                              'daterequest': -1
                                                          }.items())))

        pending_req_emisor = list(db.requestsCfdis.find(filter={'info_id': ObjectId(applicant['_id']),
                                                                'typerequest': 'e',
                                                                'status': False},
                                                        sort=list({
                                                            'daterequest': -1
                                                        }.items())))

        if len(pending_req_receptor) != 0 or len(pending_req_emisor) != 0:
            for pending in pending_req_receptor:
                create_jobs_download(request_id=str(pending['_id']),
                                     info_id=ObjectId(applicant['_id']),
                                     rfc_applicant=str(applicant['rfc']))

            for pending in pending_req_emisor:
                create_jobs_download(request_id=str(pending['_id']),
                                     info_id=ObjectId(applicant['_id']),
                                     rfc_applicant=str(applicant['rfc']))

        if not job_request is None:
            if int(body['timerequest']) != int(applicant['settingsrfc']['timerequest']):
                delete_job(info_id)
                # Cambiar a dias
                create_jobs_requests(info_id=info_id,
                                     time=int(body['timerequest']),
                                     args=[info_id])
        else:
            # Cambiar a dias
            create_jobs_requests(info_id=info_id,
                                 time=int(body['timerequest']),
                                 args=[info_id])
    else:
        if not job_request is None:
            delete_job(info_id)

    # END downdload automatically
    # BEGIN uso cfdis
    projec_usos_cfdi = {'_id': 0}

    projec_usos_cfdi.update({'usocfdi.' + uso: 1
                             for uso in list(body['usocfdis'])})

    uso_cfdis = db.catalogs.find_one(filter={'type': 'cfdis'},
                                     projection=projec_usos_cfdi)
    # END uso cfdis
    update_uso_cfdis = db.satInformations.update_one({'_id': ObjectId(info_id)},
                                                     {'$set': {
                                                         'settingsrfc.usocfdis': uso_cfdis['usocfdi'],
                                                         'settingsrfc.timerautomatic': body['timerautomatic'],
                                                         'settingsrfc.timerequest': body['timerequest']
                                                     }}
                                                     ).modified_count

    # TODO: agregar un 500 por si pasa un problema
    return jsonify({'status': 'success'}), 204
