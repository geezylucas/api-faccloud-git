import datetime
from flask import Blueprint, request, jsonify
from bson.json_util import dumps
from bson.objectid import ObjectId
from database.db import db
from cfdiclient import Autenticacion
from cfdiclient import SolicitaDescarga
from cfdiclient import Fiel

bp = Blueprint('requestscfdis', __name__)


@bp.route('', methods=['GET'])
def get_requests():
    return jsonify({'status': 'success', 'data': dumps(db.requestsCfdis.find({}))}), 200


@bp.route('', methods=['POST'])
def insert_request():
    body = request.get_json()

    fecha_inicial = datetime.datetime.strptime(body['datestart'], '%Y-%m-%d') + \
        datetime.timedelta(hours=0, minutes=0, seconds=0)
    fecha_final = datetime.datetime.strptime(body['dateend'], '%Y-%m-%d') + \
        datetime.timedelta(hours=23, minutes=59, seconds=59)

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
