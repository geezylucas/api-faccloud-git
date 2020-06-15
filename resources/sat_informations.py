from flask import Blueprint, request, jsonify
from database.db import db
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from run import app
from services.requests_cfdis import create_jobs_download, insert_request_automatically

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


@bp.route('/updatesettings/<info_id>', methods=['PATCH'])
def update_settings(info_id):
    """
    Endpoint to update settingsrfc.usocfdis by id
    """
    body = request.get_json()
    applicant = db.satInformations.find_one(filter={'_id': ObjectId(info_id)},
                                            projection={'rfc': 1, 'settingsrfc.timerequest': 1})

    # BEGIN uso cfdis
    projection = {'_id': 0}

    projection.update({'usocfdi.' + uso: 1 for uso in list(body['usocfdis'])})

    uso_cfdis = db.catalogs.find_one(filter={'type': 'cfdis'},
                                     projection=projection)
    # END uso cfdis
    # BEGIN downdload automatically
    if body['timerautomatic']:
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
            for pending in pending_req_receptor:
                create_jobs_download(request_id=pending['_id'],
                                     info_id=applicant['_id'],
                                     rfc_applicant=applicant['rfc'])

            for pending in pending_req_emisor:
                create_jobs_download(request_id=pending['_id'],
                                     info_id=applicant['_id'],
                                     rfc_applicant=applicant['rfc'])

        if app.apscheduler.get_job(info_id) != None:
            if int(body['timerequest']) != int(applicant['settingsrfc']['timerequest']):
                app.apscheduler.remove_job(info_id)
                # Cambiar a dias
                app.apscheduler.add_job(func=insert_request_automatically,
                                        trigger='interval',
                                        args=[info_id],
                                        minutes=int(body['timerequest']),
                                        id=info_id)
        else:
            # Cambiar a dias
            app.apscheduler.add_job(func=insert_request_automatically,
                                    trigger='interval',
                                    args=[info_id],
                                    minutes=int(body['timerequest']),
                                    id=info_id)

    else:
        if app.apscheduler.get_job(info_id) != None:
            app.apscheduler.remove_job(info_id)

    update_uso_cfdis = db.satInformations.update_one({'_id': ObjectId(info_id)},
                                                     {'$set': {
                                                         'settingsrfc.usocfdis': uso_cfdis['usocfdi'],
                                                         'settingsrfc.timerautomatic': body['timerautomatic'],
                                                         'settingsrfc.timerequest': body['timerequest']
                                                     }}
                                                     ).modified_count

    if update_uso_cfdis != 0:
        return jsonify({'status': 'success'}), 204
    else:
        return jsonify({'status': 'error'}), 500
