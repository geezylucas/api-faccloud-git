import os
import base64
import zipfile
from database.db import db
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from services.cfdis import get_limit_cfdis, insert_many_cfdis_func, decode_data64_and_insert


bp = Blueprint('cfdis', __name__)


@bp.route('/lastcfditotype/<info_rfc>', methods=['GET'])
def last_cdfi_to_type(info_rfc):
    """
    Endpoint for search last cfdi emisor and receptor by RFC
    """
    last_receptor_cfdi = db.cfdis.find_one(filter={'Receptor.Rfc': info_rfc},
                                           projection={'Fecha': 1, '_id': 0},
                                           sort=list({'Fecha': -1}.items()))

    last_emisor_cfdi = db.cfdis.find_one(filter={'Emisor.Rfc': info_rfc},
                                         projection={'Fecha': 1, '_id': 0},
                                         sort=list({'Fecha': -1}.items()))

    return jsonify({'status': 'success', 'data': {
        'lastReceptorCFDI': json.loads(dumps(last_receptor_cfdi)),
        'lastEmisorCFDI': json.loads(dumps(last_emisor_cfdi)),
    }, }), 200


@bp.route('/<cfdi_id>', methods=['GET'])
def get_cfdi(cfdi_id):
    """
    Endpoint for search only record in cfdis
    """
    cfdi_found = db.cfdis.find_one({'_id': ObjectId(cfdi_id)}, {
        '_id': 0,
        'Emisor.Rfc': 1,
        'Emisor.Nombre': 1,
        'Receptor.Rfc': 1,
        'Receptor.Nombre': 1,
        'Receptor.UsoCFDI': 1,
        'Fecha': 1,
        'SubTotal': 1,
        'Total': 1,
        'TipoDeComprobante': 1,
        'Descuento': 1,
        'Conceptos.Cantidad': 1,
        'Conceptos.Descripcion': 1,
        'Conceptos.ValorUnitario': 1,
        'Conceptos.Importe': 1,
        'Impuestos.TotalImpuestosTrasladados': 1,
        'Impuestos.TotalImpuestosRetenidos': 1
    })
    return jsonify({'status': 'success', 'data': json.loads(dumps(cfdi_found))})


@bp.route('/getcfdis/<info_rfc>', methods=['GET', 'POST'])
def get_cfdis(info_rfc):
    """
    Endpoint for search with filters (dateIni, dateFin, rfc, usoCfdi) records in cfdis
    and return data and pagination
    """
    body = None
    if request.method == 'POST':
        body = request.get_json()

    page_size = int(request.args.get('pagesize'))
    page_num = int(request.args.get('pagenum'))
    type_comprobante = request.args.get('typecomprobante')
    type_request = request.args.get('typerequest')

    cfdis, data_total_monto = get_limit_cfdis(page_size=page_size,
                                              page_num=page_num,
                                              info_rfc=info_rfc,
                                              type_comprobante=type_comprobante,
                                              type_request=type_request,
                                              filters=body
                                              )

    return jsonify({'status': 'success', 'data': {
        'dataPagination': json.loads(data_total_monto),
        'cfdis': json.loads(cfdis)
    }}), 200


@bp.route('', methods=['POST'])
def insert_cfdis_manually():
    """
    Endpoint to insert cfdis manually
    """
    body = request.get_json()

    applicant = db.satInformations.find_one(filter={'_id': ObjectId(body['infoId'])},
                                            projection={'rfc': 1})

    result_insert_cfdis = insert_many_cfdis_func(body['requestId'],
                                                 applicant['_id'],
                                                 'm',
                                                 applicant['rfc'])

    if result_insert_cfdis is not None:
        return jsonify({'status': 'success', 'data': {'modifiedCount': result_insert_cfdis}}), 201
    else:
        return jsonify({'status': 'error'}), 500

    # folder_extract = '/Users/geezylucas/Documents/Python/api-general-git/temp/5181BDF8-DB54-49E6-A486-92DC91D1D7EE_01'
    # result = insert_cfdis(list_files=[f for f in os.listdir(folder_extract)], folder_extract=folder_extract, info_head={
    #     "request_id": body['requestid'],
    #     "info_id": body['infoid']
    # })

    # return jsonify({'result': result}), 200


@bp.route('/insertcfdismanually/<info_id>', methods=['POST'])
def insert_cfdis_by_portal(info_id):
    body = request.get_json()

    files_base64 = list(body['files'])

    num_cfdis = 0
    for file in files_base64:
        starter = file['data64'].find(',')
        zip_data = file['data64'][starter+1:]
        num_cfdis = num_cfdis + decode_data64_and_insert(data=zip_data,
                                                         name=file['name'],
                                                         info_head={"info_id": ObjectId(info_id)})

    return jsonify({'status': 'OK', 'data': {'cfdisInsertados': num_cfdis}}), 200
