from datetime import datetime, timedelta
from bson.json_util import dumps
from bson.objectid import ObjectId
from typing import Union
from werkzeug.local import LocalProxy
from api.db import get_db

# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)


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


def insert_request_func(info_id: ObjectId, request_id: str, date_ini: datetime, date_fin: datetime, type_request: str,
                        auto_or_man: str) -> str:
    """
    Function for insert request
    """
    # TODO: almacenar el error cod_estatus, checar el -1 para responder con el codigo correcto
    return db.requestsCfdis.insert_one({
        "_id": request_id,
        "info_id": info_id,
        "typerequest": type_request,
        "daterequest": datetime.now(),
        "status": False,
        "datestart": date_ini,
        "dateend": date_fin,
        "request": auto_or_man
    }).inserted_id


def insert_many_cfdis_func(request_id: str, info_id: ObjectId, data: str) -> int:
    """
    Function to insert base64 str which contains the XMLs
    """
    from api.services.cfdis import decode_data64_and_insert

    num_cfdis = decode_data64_and_insert(data=data, info_head={"request_id": request_id, "info_id": info_id})

    # Actualizamos la solicitud
    return db.requestsCfdis.update_one(
        {"_id": request_id},
        {"$set": {
            "status": True,
            "numcfdis": num_cfdis,
            "datedownload": datetime.now(),
        }}
    ).modified_count
