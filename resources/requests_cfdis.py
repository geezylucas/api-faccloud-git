from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from database.db import db
from cfdiclient import Autenticacion
from cfdiclient import SolicitaDescarga
from cfdiclient import Fiel

bp = Blueprint('requestscfdis', __name__)


def get_limit_requests(page_size: int, page_num: int, info_id: str, filters: dict or None):
    """
    Function for search records in requestsCfdis
    """
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

    count_requests_cfdi = db.requestsCfdis.count_documents(filter=filter)
    data_pagination = {}
    if count_requests_cfdi != 0:
        num = float(count_requests_cfdi) / float(page_size)
        if num.is_integer():
            data_pagination.update({
                "fieldsmatched": count_requests_cfdi,
                "pages": int(num)
            })
        else:
            data_pagination.update({
                "fieldsmatched": count_requests_cfdi,
                "pages": int(num + 1.0)
            })
    else:
        data_pagination.update({
            "fieldsmatched": 0,
            "pages": 1
        })

    # Return data and pagination
    return dumps(requests_cfdi), dumps(data_pagination)


def insert_request_func(info_id: str, date_ini: str, date_fin: datetime, type_request: datetime, auto_or_man: str) -> str or None:
    """
    Function for search request and insert
    """
    applicant = db.satInformations.find_one({'_id': ObjectId(info_id)})

    rfc_applicant = applicant["rfc"]

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
    request_download = SolicitaDescarga(fiel)

    result_request = {}
    if type_request == 'e':
        # Emitidos
        result_request = request_download.solicitar_descarga(token,
                                                             rfc_applicant,
                                                             date_ini,
                                                             date_fin,
                                                             rfc_emisor=rfc_applicant)
    elif type_request == 'r':
        # Recibidos
        result_request = request_download.solicitar_descarga(token,
                                                             rfc_applicant,
                                                             date_ini,
                                                             date_fin,
                                                             rfc_receptor=rfc_applicant)

    # TODO: almacenar el error cod_estatus
    if "cod_estatus" in result_request.keys():
        if result_request["cod_estatus"] == '5000':
            return db.requestsCfdis.insert_one({
                "_id": result_request["id_solicitud"],
                "info_id": ObjectId(applicant["_id"]),
                "typerequest": type_request,
                "daterequest": datetime.now(),
                "status": False,
                "datestart": date_ini,
                "dateend": date_fin,
                "request": auto_or_man
            }).inserted_id
        else:
            return None

    return None


def insert_request_automatically():
    pass


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
    return jsonify({'status': 'success', 'data': json.loads(dumps(request_cfdi))})


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

    result_insert_request = insert_request_func(info_id=body['infoId'],
                                                date_ini=date_ini,
                                                date_fin=date_fin,
                                                type_request=body['typeRequest'],
                                                auto_or_man='m')

    if result_insert_request is not None:
        return jsonify({'status': 'success', 'data': {'_id': result_insert_request, 'message': 'OK'}}), 201
    else:
        return jsonify({'status': 'error', 'data': {'_id': None, 'message': 'Error'}}), 200
