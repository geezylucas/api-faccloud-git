from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from database.db import db
from cfdiclient import Autenticacion
from cfdiclient import SolicitaDescarga
from cfdiclient import Fiel

bp = Blueprint('requestscfdis', __name__)


@bp.route('', methods=['GET'])
def get_requests():
    return jsonify({'status': 'success', 'data': json.loads(dumps(db.requestsCfdis.find({})))}), 200


@bp.route('', methods=['POST'])
def insert_request():
    body = request.get_json()

    fecha_inicial = datetime.strptime(
        body['datestart'], '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
    fecha_final = datetime.strptime(
        body['dateend'], '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)

    solicitante = db.satInformations.find_one({
        '_id': ObjectId(body["id"])
    })

    rfc_solicitante = solicitante["rfc"]

    type_request = body['typerequest']

    # esta parte de hará cuando este lista la tabla
    path_files = '/Users/geezylucas/Documents/Python/datasensible/'
    cer = path_files + '00001000000404800833.cer'
    key = path_files + 'Claveprivada_FIEL_PTI121203SZ0_20170111_190425.key'
    passkeyprivate = 'BEAUGENCY1964'

    cer_der = open(cer, 'rb').read()
    key_der = open(key, 'rb').read()

    fiel = Fiel(cer_der, key_der, passkeyprivate)
    # FIN example

    # 1. Token
    auth = Autenticacion(fiel)
    token = auth.obtener_token()
    # 2. Solicitud
    descarga = SolicitaDescarga(fiel)

    new_request = {}
    result_solicitud = dict()

    if type_request == 'e':
        # Emitidos
        result_solicitud = descarga.solicitar_descarga(token, rfc_solicitante,
                                                       fecha_inicial, fecha_final,
                                                       rfc_emisor=rfc_solicitante)
    elif type_request == 'r':
        # Recibidos
        result_solicitud = descarga.solicitar_descarga(token, rfc_solicitante,
                                                       fecha_inicial, fecha_final,
                                                       rfc_receptor=rfc_solicitante)

    if result_solicitud["cod_estatus"] == '5000':
        result = db.requestsCfdis.insert_one({
            "_id": result_solicitud["id_solicitud"],
            "info_id": ObjectId(solicitante["_id"]),
            "typerequest": type_request,
            "daterequest": datetime.datetime.now(),
            "status": False,
            "datestart": fecha_inicial,
            "dateend": fecha_final
        }).inserted_id
        return jsonify({'status': 'success', 'data': {'_id': result}}), 201
    else:
        return jsonify({'status': 'error', 'message': result_solicitud["cod_estatus"]}), 500


@bp.route('/<info_id>', methods=['GET', 'POST'])
def get_request(info_id):
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


def get_limit_requests(page_size, page_num, info_id, filters):
    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)

    filter = {'info_id': ObjectId(info_id)}

    if not filters is None:
        fecha_inicial = datetime.strptime(
            filters['dateIni'], '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
        fecha_final = datetime.strptime(
            filters['dateFin'], '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
        filter.update({
            'daterequest': {
                '$gte': fecha_inicial,
                '$lte': fecha_final
            }})

    requests_cfdi = list(db.requestsCfdis.find(filter=filter,
                                               projection={
                                                   'typerequest': 1,
                                                   'status': 1,
                                                   'numcfdis': 1,
                                                   'daterequest': 1
                                               },
                                               sort=list({
                                                   'daterequest': -1
                                               }.items()),
                                               skip=skips,
                                               limit=page_size))

    data_pagination = {}

    if len(requests_cfdi) != 0:
        num = float(len(requests_cfdi)) / float(page_size)
        if num.is_integer():
            data_pagination.update({
                "fieldsmatched": len(requests_cfdi),
                "pages": int(num)
            })
        else:
            data_pagination.update({
                "fieldsmatched": len(requests_cfdi),
                "pages": int(num + 1.0)
            })
    else:
        data_pagination.update({
            "fieldsmatched": 0,
            "pages": 1
        })

    # Return data and pagination
    return dumps(requests_cfdi), dumps(data_pagination)
