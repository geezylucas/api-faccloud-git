import os
import base64
import zipfile
import shutil
from datetime import datetime, timedelta
from typing import List, Tuple
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from database.db import db
from cfdiclient import Autenticacion
from cfdiclient import VerificaSolicitudDescarga
from cfdiclient import DescargaMasiva
from cfdiclient import Fiel

bp = Blueprint('cfdis', __name__)

ns = {
    'cfdi': 'http://www.sat.gob.mx/cfd/3',
    'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
}


def get_limit_cfdis(page_size: int, page_num: int, info_rfc: str, type_comprobante: str, type_request: str, filters: dict):
    """
    Function to search records in cfdis
    """
    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)
    comprobante_types = type_comprobante.split("-")

    # PARA MATCH
    # Emisor.Rfc or Receptor.Rfc por type_request
    # Receptor.UsoCFDI
    # Emisor.Rfc or Receptor.Rfc por filter
    # Range convertedFecha
    filter_type_request = {'TipoDeComprobante': {'$in': comprobante_types}}
    project_type_request = {'Total': 1, 'Fecha': 1}

    if type_request == 'r':
        filter_type_request.update({'Receptor.Rfc': info_rfc})
        project_type_request.update({'Rfc': '$Emisor.Rfc'})
    elif type_request == 'e':
        filter_type_request.update({'Emisor.Rfc': info_rfc})
        project_type_request.update({'Rfc': '$Receptor.Rfc'})

    if not filters is None:
        fecha_inicial = datetime.strptime(
            filters['dateIni'], '%Y-%m-%d') + timedelta(hours=0, minutes=0, seconds=0)
        fecha_final = datetime.strptime(
            filters['dateFin'], '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
        filter_type_request.update({
            'Fecha': {
                '$gte': fecha_inicial.isoformat(),
                '$lte': fecha_final.isoformat()
            }})

        if filters['rfc'] != '':
            rfc_filter = '^{}'.format(filters['rfc'])
            if type_request == 'r':
                filter_type_request.update({'Emisor.Rfc': {
                    '$regex': rfc_filter, '$options': 'i'
                }})
            elif type_request == 'e':
                filter_type_request.update({'Receptor.Rfc': {
                    '$regex': rfc_filter, '$options': 'i'
                }})

        if filters['usoCfdi'] != '':
            # CHECAR: esta parte la tenemos que hacer con un verdadero catalogo
            switcher = {
                "Adquisición de mercancias": "G01",
                "Devoluciones, descuentos o bonificaciones": "G02",
                "Gastos en general": "G03"
            }
            filter_type_request.update({
                'Receptor.UsoCFDI': switcher[filters['usoCfdi']]
            })

    # Requires the PyMongo package. POAG760804RP8
    # https://api.mongodb.com/python/current

    list_cfdis = list(db.cfdis.find(filter=filter_type_request,
                                    projection=project_type_request,
                                    sort=list({
                                        'Fecha': -1
                                    }.items()),
                                    skip=skips,
                                    limit=page_size))

    pagination_monto = list(db.cfdis.aggregate([
        {
            '$match': filter_type_request
        }, {
            '$project': project_type_request
        }, {
            '$group': {
                '_id': None,
                'totalMonto': {'$sum': '$Total'},
                'fieldsmatched': {'$sum': 1}
            }
        }
    ]))

    data_pagination_monto = {}

    if len(pagination_monto) != 0:
        num = float(pagination_monto[0]["fieldsmatched"]) / float(page_size)
        if num.is_integer():
            data_pagination_monto.update({
                "totalMonto": pagination_monto[0]["totalMonto"],
                "fieldsmatched": pagination_monto[0]["fieldsmatched"],
                "pages": int(num)
            })
        else:
            data_pagination_monto.update({
                "totalMonto": pagination_monto[0]["totalMonto"],
                "fieldsmatched": pagination_monto[0]["fieldsmatched"],
                "pages": int(num + 1.0)
            })
    else:
        data_pagination_monto.update({
            "totalMonto": {'$numberDecimal': 0},
            "fieldsmatched": 0,
            "pages": 1
        })

    # Return data and pagination
    return dumps(list_cfdis), dumps(data_pagination_monto)


def extract_attrs_without(dirty_attrs: dict) -> dict:
    """
    Function to filter attributes
    """
    attrs = dict(filter(lambda elem: not elem[0].startswith('{http')
                        and not elem[0].startswith('xml')
                        and not elem[0].startswith('xsi'),
                        dirty_attrs.items()))

    return attrs


def impuestos_reten_trasl(nodo: Element) -> Tuple[list, list]:
    """
    Function to extract impuestos traslados or retenciones
    """
    list_traslados = []
    list_retenciones = []

    # Traslados
    n_traslado = nodo.find("cfdi:Traslados", ns)
    if n_traslado != None:
        for child in n_traslado.findall("cfdi:Traslado", ns):
            list_traslados.append(child.attrib)

    # Retenciones
    n_retencion = nodo.find("cfdi:Retenciones", ns)
    if n_retencion != None:
        for child in n_retencion.findall("cfdi:Retencion", ns):
            list_retenciones.append(child.attrib)

    return list_traslados, list_retenciones


def extract_data_nodo_attr(nodo: List[Element], attribute: str) -> List[str]:
    """
    Function to extract data to attribute in a list nodo
    """
    list_data = []

    for child in nodo:
        list_data.append(child.attrib[attribute])

    return list_data


# TODO:"Addenda QUE FUNCION TIENE"
def insert_cfdis(list_files: List[str], folder_extract: str, info_head: dict) -> int:
    """
    Function for insert many in cfdis
    """

    cfdis_to_insert = []
    for f in list_files:
        tree = ET.parse(folder_extract + '/' + f)
        root = tree.getroot()

        new_cfdi = {}
        new_cfdi.update(info_head)

        # Complemento
        n_complemento = root.find("cfdi:Complemento", ns)
        n_timbre = n_complemento.find("tfd:TimbreFiscalDigital", ns)
        timbre_fiscal_attrs = extract_attrs_without(n_timbre.attrib)

        if db.cfdis.count_documents({'Complemento.TimbreFiscalDigital.UUID': timbre_fiscal_attrs['UUID']}, limit=1):
            continue

        new_cfdi.update({"Complemento": {
            'TimbreFiscalDigital': timbre_fiscal_attrs
        }})
        # END Complemento

        attrs_comprobante = extract_attrs_without(root.attrib)
        if "SubTotal" in attrs_comprobante.keys():
            attrs_comprobante.update(
                {'SubTotal': Decimal128(attrs_comprobante['SubTotal'])})

        if "Descuento" in attrs_comprobante.keys():
            attrs_comprobante.update(
                {'Descuento': Decimal128(attrs_comprobante['Descuento'])})

        if "Total" in attrs_comprobante.keys():
            attrs_comprobante.update(
                {'Total': Decimal128(attrs_comprobante['Total'])})

        new_cfdi.update(attrs_comprobante)

        # CfdiRelacionado
        n_cfdirelacionados = root.find("cfdi:CfdiRelacionados", ns)
        if n_cfdirelacionados != None:
            new_cfdi.update({"CfdiRelacionado": {
                "TipoRelacion": n_cfdirelacionados.attrib["TipoRelacion"],
                "CfdiRelacionados": extract_data_nodo_attr(
                    n_cfdirelacionados.findall("cfdi:CfdiRelacionado", ns),
                    "UUID"
                )
            }})
        # FIN CfdiRelacionado
        #
        # Emisor
        emisor = root.find("cfdi:Emisor", ns)
        new_cfdi.update({"Emisor": emisor.attrib})

        # Receptor
        receptor = root.find("cfdi:Receptor", ns)
        new_cfdi.update({"Receptor": receptor.attrib})

        # Conceptos TODO: "ComplementoConcepto QUE FUNCION TIENE"
        list_conceptos = []
        n_concepto = root.find("cfdi:Conceptos", ns)
        for concepto in n_concepto.findall("cfdi:Concepto", ns):
            new_concepto = concepto.attrib

            # Impuestos
            n_impuestos = concepto.find("cfdi:Impuestos", ns)
            if n_impuestos != None:
                concepto_impuestos = {}
                list_tras, list_reten = impuestos_reten_trasl(n_impuestos)
                if list_tras:
                    concepto_impuestos.update({"Traslados": list_tras})

                if list_reten:
                    concepto_impuestos.update({"Retenciones": list_reten})

                new_concepto.update({"Impuestos": concepto_impuestos})
            # END Impuestos
            #
            # InformacionAduanera
            n_info_aduanera = concepto.findall("cfdi:InformacionAduanera", ns)
            if n_info_aduanera:
                new_concepto.update({
                    "InformacionAduanera": extract_data_nodo_attr(
                        n_info_aduanera,
                        "NumeroPedimento"
                    )
                })

            # CuentaPredial
            n_cuenta_predial = concepto.find("cfdi:CuentaPredial", ns)
            if n_cuenta_predial != None:
                new_concepto.update(
                    {"CuentaPredial": n_cuenta_predial.attrib["Numero"]})

            # Parte
            n_parte = concepto.findall("cfdi:Parte", ns)
            if n_parte:
                list_partes = []
                for child in n_parte:
                    obj_parte = child.attrib
                    obj_parte.update({
                        "InformacionAduanera": extract_data_nodo_attr(
                            child.findall("cfdi:InformacionAduanera", ns),
                            "NumeroPedimento"
                        )
                    })
                    list_partes.append(obj_parte)

                new_concepto.update({"Parte": list_partes})

            list_conceptos.append(new_concepto)
        # END Concepto

        new_cfdi.update({"Conceptos": list_conceptos})

        # Impuestos
        n_impuestos = root.find("cfdi:Impuestos", ns)
        if n_impuestos != None:
            new_impuestos = n_impuestos.attrib
            list_tras, list_reten = impuestos_reten_trasl(n_impuestos)
            if list_tras:
                new_impuestos.update({"Traslados": list_tras})

            if list_reten:
                new_impuestos.update({"Retenciones": list_reten})

            new_cfdi.update({"Impuestos": new_impuestos})
        # END Impuestos

        cfdis_to_insert.append(new_cfdi)

    # END For xmls
    db.cfdis.create_index('Emisor.Rfc')
    db.cfdis.create_index('Receptor.Rfc')
    db.cfdis.create_index('Complemento.TimbreFiscalDigital.UUID', unique=True)

    if cfdis_to_insert:
        return len(db.cfdis.insert_many(cfdis_to_insert).inserted_ids)
    else:
        return 0


def insert_cfdis_manually_func(request_id: str, info_id: str) -> int or None:
    """
    Function to search package from SAt
    """
    # obtenemos los datos desde el body de la request
    applicant = db.satInformations.find_one(
        {'_id': ObjectId(info_id)}, {'rfc': 1})
    # Armamos la data para solicitar datos al sat
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
    verify_download = VerificaSolicitudDescarga(fiel)

    check_download = verify_download.verificar_descarga(token=token,
                                                        rfc_solicitante=rfc_applicant,
                                                        id_solicitud=request_id)
    if check_download['estado_solicitud'] == '3':
        numcfdis = 0
        packages_result = check_download['paquetes']
        for package in packages_result:
            binary_file_path = os.getcwd() + '/temp/'
            zip_path = binary_file_path + package + '.zip'
            folder_extract = binary_file_path + package

            download = DescargaMasiva(fiel)
            result_download = download.descargar_paquete(token=token,
                                                         rfc_solicitante=rfc_applicant,
                                                         id_paquete=package)
            data = result_download['paquete_b64']

            decoded = base64.b64decode(data)
            with open(zip_path, 'wb') as f:
                f.write(decoded)

            zf = zipfile.ZipFile(zip_path, 'r')
            zf.extractall(folder_extract)
            zf.close()
            os.remove(zip_path)

            # Debemos de recorrer file por file de la new folder y almacenar en la bd
            list_files = [f for f in os.listdir(folder_extract)]

            numcfdis = numcfdis + insert_cfdis(list_files=list_files,
                                               folder_extract=folder_extract,
                                               info_head={
                                                   "request_id": request_id,
                                                   "info_id": applicant['_id']
                                               })
            shutil.rmtree(folder_extract)
            # END For packages

        # Actualizamos la solicitud
        return db.requestsCfdis.update_one(
            {"_id": request_id},
            {"$set": {
                "status": True,
                "numcfdis": numcfdis,
                "datedownload": datetime.now(),
                "downloads": check_download['paquetes']
            }}
        ).modified_count

    elif check_download['codigo_estado_solicitud'] == '5004':
        return db.requestsCfdis.update_one(
            {"_id": request_id},
            {"$set": {
                "status": True,
                "numcfdis": 0,
                "datedownload": datetime.now()
            }}
        ).modified_count
    else:
        return None


@bp.route('/totalcfdistotype/<info_rfc>', methods=['GET'])
def total_cdfis_to_type(info_rfc):
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


@bp.route('/<id>', methods=['GET'])
def get_cfdi(id):
    """
    Endpoint for search only record in cfdis 
    """
    cfdi_found = db.cfdis.find_one({'_id': ObjectId(id)}, {
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

    result_insert_cfdis = insert_cfdis_manually_func(request_id=body['requestid'],
                                                     info_id=body['infoid'])

    if not result_insert_cfdis is None:
        return jsonify({'status': 'success', 'data': {'modified_count': result_insert_cfdis}}), 201
    else:
        return jsonify({'status': 'error'}), 500

    # folder_extract = '/Users/geezylucas/Documents/Python/api-general-git/temp/5181BDF8-DB54-49E6-A486-92DC91D1D7EE_01'
    # result = insert_cfdis(list_files=[f for f in os.listdir(folder_extract)], folder_extract=folder_extract, info_head={
    #     "request_id": body['requestid'],
    #     "info_id": body['infoid']
    # })

    # return jsonify({'result': result}), 200
