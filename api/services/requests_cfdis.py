from api import create_app
from datetime import datetime, timedelta
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from typing import Union
from cfdiclient import Autenticacion
from cfdiclient import SolicitaDescarga
from cfdiclient import VerificaSolicitudDescarga
from cfdiclient import DescargaMasiva
from cfdiclient import Fiel
from werkzeug.local import LocalProxy
from api.db import get_db
from pyfcm import FCMNotification
from requests.exceptions import ConnectionError

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
app = create_app()


def pagination_requests(page_size: int, page_num: int, info_id: str, filters: Union[dict, None]):
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


def insert_request_func(info_id: ObjectId, date_ini: datetime, date_fin: datetime, type_request: str, auto_or_man: str, rfc_applicant: str) -> Union[str, None]:
    """
    Function for search request and insert
    """
    with app.app_context():
        efirma = db.efirmas.find_one(filter={'info_id': info_id},
                                     projection={'_id': 0})

        fiel = Fiel(efirma['cer'], efirma['key'], efirma['passkeyprivate'])

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

        # TODO: almacenar el error cod_estatus, checar el -1 para responder con el codigo correcto
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
            return -1


# 0:request_id: str, 1:info_id: ObjectId, 2:request: str, 3:rfc: str
def insert_many_cfdis_func(*args) -> Union[int, None]:
    """
    Function to search package from SAT
    """
    print('beep cfdi: ' + args[0])
    with app.app_context():
        from api.services.cfdis import decode_data64_and_insert
        # Armamos la data para solicitar datos al sat
        request_id = args[0]
        info_id = args[1]
        request_type = args[2]
        rfc_applicant = args[3]

        efirma = db.efirmas.find_one(filter={'info_id': info_id},
                                     projection={'_id': 0})

        fiel = Fiel(efirma['cer'], efirma['key'], efirma['passkeyprivate'])

        # 1. Token
        auth = Autenticacion(fiel)
        token = auth.obtener_token()
        # 2. Solicitud
        verify_download = VerificaSolicitudDescarga(fiel)

        check_download = verify_download.verificar_descarga(token=token,
                                                            rfc_solicitante=rfc_applicant,
                                                            id_solicitud=request_id)

        if 'estado_solicitud' in check_download.keys():
            if check_download['estado_solicitud'] == '3' and check_download['cod_estatus'] == '5000':
                num_cfdis = 0
                packages_result = check_download['paquetes']
                for package in packages_result:
                    download = DescargaMasiva(fiel)
                    result_download = download.descargar_paquete(token=token,
                                                                 rfc_solicitante=rfc_applicant,
                                                                 id_paquete=package)

                    if result_download['paquete_b64'] is None:
                        if request_type == 'a':
                            delete_job(request_id)
                            print('beep cfdi remove: ' + request_id)

                        return None

                    data = result_download['paquete_b64']

                    num_cfdis = num_cfdis + decode_data64_and_insert(data=data,
                                                                     info_head={"request_id": request_id,
                                                                                "info_id": info_id})
                    # END For packages

                if request_type == 'a':
                    delete_job(request_id)
                    print('beep cfdi remove: ' + request_id)
                    send_notification(request_id)
                # Actualizamos la solicitud
                return db.requestsCfdis.update_one(
                    {"_id": request_id},
                    {"$set": {
                        "status": True,
                        "numcfdis": num_cfdis,
                        "datedownload": datetime.now(),
                        "downloads": check_download['paquetes']
                    }}
                ).modified_count
            elif check_download['estado_solicitud'] == '5' and check_download['cod_estatus'] == '5000':
                if request_type == 'a':
                    delete_job(request_id)
                    print('beep cfdi remove: ' + request_id)
                    send_notification(request_id)
                # Actualizamos la solicitud
                return db.requestsCfdis.update_one(
                    {"_id": request_id},
                    {"$set": {
                        "status": True,
                        "numcfdis": 0,
                        "datedownload": datetime.now()
                    }}
                ).modified_count
            elif check_download['estado_solicitud'] == '2' and check_download['cod_estatus'] == '5000':
                return 0
        else:
            # TODO: guardar el error
            if request_type == 'a':
                delete_job(request_id)
                print('beep cfdi remove: ' + request_id)
                send_notification(request_id)
            return None


def insert_request_automatically(info_id: str):
    """
    Function for search requests and create downloads automatically
    """
    print('beep request: ' + info_id)
    applicant = {}
    last_req_receptor = {}
    last_req_emisor = {}
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
    date_ini = last_req_receptor['dateend'] + timedelta(seconds=1) \
        if last_req_receptor is not None else datetime(2020, 12, 0, 0, 0, 0)
    date_fin = last_req_receptor['dateend'] + timedelta(days=5) \
        if last_req_receptor is not None else datetime(2019, 12, 3, 23, 59, 59)

    id_req_receptor = insert_request_func(info_id=applicant['_id'],
                                          date_ini=date_ini,
                                          date_fin=date_fin,
                                          type_request='r',
                                          auto_or_man='a',
                                          rfc_applicant=applicant['rfc'])

    date_ini = last_req_emisor['dateend'] + timedelta(seconds=1) \
        if last_req_emisor is not None else datetime(2020, 12, 0, 0, 0, 0)
    date_fin = last_req_emisor['dateend'] + timedelta(days=5) \
        if last_req_emisor is not None else datetime(2020, 12, 3, 23, 59, 59)

    id_req_emisor = insert_request_func(info_id=applicant['_id'],
                                        date_ini=date_ini,
                                        date_fin=date_fin,
                                        type_request='e',
                                        auto_or_man='a',
                                        rfc_applicant=applicant['rfc'])

    # Agregamos los nuevos jobs para solicitar descarga
    if id_req_receptor is not None:
        create_jobs_download(request_id=id_req_receptor,
                             info_id=ObjectId(applicant['_id']),
                             rfc_applicant=str(applicant['rfc']))

    if id_req_emisor is not None:
        create_jobs_download(request_id=id_req_emisor,
                             info_id=ObjectId(applicant['_id']),
                             rfc_applicant=str(applicant['rfc']))


def get_job(id: str):
    """
    Function to search job
    """
    return app.apscheduler.get_job(id)


def create_jobs_download(request_id: str, info_id: ObjectId, rfc_applicant: str):
    """
    Function for create job to download
    """
    app.apscheduler.add_job(func=insert_many_cfdis_func,
                            trigger='interval',
                            args=[request_id, info_id, 'a', rfc_applicant],
                            minutes=2,
                            id=request_id)


def create_jobs_requests(info_id: str, time: int, args: list):
    """
    Function for create job to request
    """
    app.apscheduler.add_job(func=insert_request_automatically,
                            trigger='interval',
                            args=args,
                            minutes=time,
                            id=info_id)


def delete_job(id: str):
    """
    Function to delete job
    """
    app.apscheduler.remove_job(id)


def send_notification(request_id: str):
    """
    Function for send notification to device
    """
    push_service = FCMNotification(
        api_key="AAAAI4MAUJI:APA91bFvIZ09iWATXfk4lggfaKDCrpG2oDUKm91YlpLzqRoQexYhwjXZaiXKjZ0gvGVSlXL9qVDSBLQz2pM6iirGVBg9_NJdT0W3C3pb7m6F3y6oFAgDz-R0uboFJul0Ay6LcyFFDl4_")
    registration_id = "ejN9LkS1Qh6cGaSJHwI_t2:APA91bGyh9OTppC-io2BYgH8Lquc0aH2kqc6AjCXcCV6Kb1aRanqd34o5ouGthcf1SHo5zgl4YSvhtAEConDF9VHx4XFO-2E1ZnbxBZHykHN1Z7tz8i48QPHcmSOiUUy5hvkKbGgcMXi"
    message_title = "Descarga automática"
    message_body = "Se descargó un paquete {}".format(request_id)
    result = push_service.notify_single_device(
        registration_id=registration_id, message_title=message_title, message_body=message_body)

    print(result)
