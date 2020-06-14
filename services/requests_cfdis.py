from run import app
from database.db import db
from datetime import datetime, timedelta
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from cfdiclient import Autenticacion
from cfdiclient import SolicitaDescarga
from cfdiclient import Fiel
from services.cfdis import insert_many_cfdis_func


def get_limit_requests(page_size: int, page_num: int, info_id: str, filters: dict or None):
    """
    Function for search records in requestsCfdis
    """
    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)

    filter = {'info_id': ObjectId(info_id)}

    if filters is not None:
        fecha_inicial = datetime.strptime(
            filters['dateIni'], '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
        fecha_final = datetime.strptime(
            filters['dateFin'], '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
        filter.update({
            'daterequest': {
                '$gte': fecha_inicial,
                '$lte': fecha_final
            }})

        if filters['status'] != '':
            filter.update({'status': filters['status']})

    requests_cfdi = list(db.requestsCfdis.find(filter=filter,
                                               projection={
                                                   'typerequest': 1,
                                                   'status': 1,
                                                   'numcfdis': 1,
                                                   'daterequest': 1,
                                                   'request': 1,
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


def insert_request_func(info_id: ObjectId, date_ini: datetime, date_fin: datetime, type_request: str, auto_or_man: str, rfc_applicant: str) -> str or None:
    """
    Function for search request and insert
    """
    with app.app_context():
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
            result_request = request_download.solicitar_descarga(token=token,
                                                                 rfc_solicitante=rfc_applicant,
                                                                 fecha_inicial=date_ini,
                                                                 fecha_final=date_fin,
                                                                 rfc_emisor=rfc_applicant)
        elif type_request == 'r':
            # Recibidos
            result_request = request_download.solicitar_descarga(token=token,
                                                                 rfc_solicitante=rfc_applicant,
                                                                 fecha_inicial=date_ini,
                                                                 fecha_final=date_fin,
                                                                 rfc_receptor=rfc_applicant)

        # TODO: almacenar el error cod_estatus
        if "cod_estatus" in result_request.keys():
            if result_request["cod_estatus"] == '5000':
                return db.requestsCfdis.insert_one({
                    "_id": result_request["id_solicitud"],
                    "info_id": info_id,
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


def create_jobs_download(request_id: str, info_id: ObjectId, rfc_applicant: str):
    """
    Function for create job to download
    """
    app.apscheduler.add_job(func=insert_many_cfdis_func,
                            trigger='interval',
                            args=[request_id,
                                  info_id,
                                  'a',
                                  rfc_applicant],
                            minutes=3,
                            id=request_id)


def insert_request_automatically(info_id):
    """
    Function for search requests and create downloads automatically
    """
    print('beep request: ' + info_id)
    with app.app_context():
        applicant = db.satInformations.find_one(filter={'_id': ObjectId(info_id)},
                                                projection={'rfc': 1})

        # TODO: Historico suscripciones, preguntar la ultima fecha premium y comenzar desde ahí la descarga
        last_req_receptor = db.requestsCfdis.find_one(filter={'info_id': applicant['_id'],
                                                              'typerequest': 'r',
                                                              'request': 'a'},
                                                      sort=list({
                                                          'daterequest': -1
                                                      }.items()))

        last_req_emisor = db.requestsCfdis.find_one(filter={'info_id': applicant['_id'],
                                                            'typerequest': 'e',
                                                            'request': 'a'},
                                                    sort=list({
                                                        'daterequest': -1
                                                    }.items()))

        # TODO: si es nuevo, preguntar al pratrón desde que día pedimos los cfdis DATE_INI DATE_FIN
        date_ini = last_req_receptor['dateend'] + timedelta(
            seconds=1) if last_req_receptor is not None else datetime(2019, 12, 2, 0, 0, 0)
        date_fin = last_req_receptor['dateend'] + timedelta(
            days=5) if last_req_receptor is not None else datetime(2019, 12, 4, 23, 59, 59)

        id_req_receptor = insert_request_func(info_id=applicant['_id'],
                                              date_ini=date_ini,
                                              date_fin=date_fin,
                                              type_request='r',
                                              auto_or_man='a',
                                              rfc_applicant=applicant['rfc'])

        date_ini = last_req_emisor['dateend'] + timedelta(
            seconds=1) if last_req_emisor is not None else datetime(2020, 1, 3, 0, 0, 0)
        date_fin = last_req_emisor['dateend'] + timedelta(
            days=5) if last_req_emisor is not None else datetime(2020, 1, 5, 23, 59, 59)

        id_req_emisor = insert_request_func(info_id=applicant['_id'],
                                            date_ini=date_ini,
                                            date_fin=date_fin,
                                            type_request='e',
                                            auto_or_man='a',
                                            rfc_applicant=applicant['rfc'])

        # Agregamos los nuevos jobs para solicitar descarga
        if id_req_receptor is not None:
            create_jobs_download(request_id=id_req_receptor,
                                 info_id=applicant['_id'],
                                 rfc_applicant=applicant['rfc'])

        if id_req_emisor is not None:
            create_jobs_download(request_id=id_req_emisor,
                                 info_id=applicant['_id'],
                                 rfc_applicant=applicant['rfc'])
