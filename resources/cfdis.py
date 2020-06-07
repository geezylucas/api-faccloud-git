import os
import base64
import zipfile
from datetime import datetime, timedelta
from typing import List, Tuple
from flask import Blueprint, request, jsonify
from bson.json_util import dumps, json
from bson.objectid import ObjectId
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


def extract_attrs_without(dirty_attrs: dict) -> dict:
    attrs = dict(filter(lambda elem: not elem[0].startswith('{http')
                        and not elem[0].startswith('xml')
                        and not elem[0].startswith('xsi'),
                        dirty_attrs.items()))

    return attrs


def impuestos_reten_trasl(nodo: Element) -> Tuple[list, list]:
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
    list_data = []

    for child in nodo:
        list_data.append(child.attrib[attribute])

    return list_data


# INSERT MANY CFDIS CHECAR: "Addenda QUE FUNCION TIENE"
def insert_cfdis(list_files: List[str], folder_extract: str, info_id: ObjectId, request_id: str) -> int:
    cfdis_to_insert = []

    for f in list_files:
        new_cfdi = {
            "info_id": info_id,
            # TODO: Eso se evaluara si lo envian
            "request_id": request_id
        }

        tree = ET.parse(folder_extract + '/' + f)
        root = tree.getroot()

        attrs_comprobante = extract_attrs_without(root.attrib)

        new_cfdi.update(attrs_comprobante)

        # CfdiRelacionado
        n_cfdirelacionados = root.find("cfdi:CfdiRelacionados", ns)
        if n_cfdirelacionados != None:
            list_relacionado = extract_data_nodo_attr(
                n_cfdirelacionados.findall("cfdi:CfdiRelacionado", ns),
                "UUID"
            )

            new_cfdirelacionados = {
                "TipoRelacion": n_cfdirelacionados.attrib["TipoRelacion"],
                "CfdiRelacionados": list_relacionado
            }

            new_cfdi.update({"CfdiRelacionado": new_cfdirelacionados})
        # FIN CfdiRelacionado
        #
        # Emisor
        emisor = root.find("cfdi:Emisor", ns)
        new_cfdi.update({"Emisor": emisor.attrib})

        # Receptor
        receptor = root.find("cfdi:Receptor", ns)
        new_cfdi.update({"Receptor": receptor.attrib})

        # Conceptos CHECAR: "ComplementoConcepto QUE FUNCION TIENE"
        list_conceptos = []
        n_concepto = root.find("cfdi:Conceptos", ns)
        for concepto in n_concepto.findall("cfdi:Concepto", ns):
            new_concepto = {}
            new_concepto.update(concepto.attrib)

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
                list_info_aduaneras = extract_data_nodo_attr(
                    n_info_aduanera,
                    "NumeroPedimento"
                )
                new_concepto.update({
                    "InformacionAduanera": list_info_aduaneras
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
                    list_info_aduaneras = extract_data_nodo_attr(
                        child.findall("cfdi:InformacionAduanera", ns),
                        "NumeroPedimento"
                    )
                    obj_parte.update({
                        "InformacionAduanera": list_info_aduaneras
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
        #
        # Complemento CHECAR: "QUE MAS HAY EN ESTE NODO"
        n_complemento = root.find("cfdi:Complemento", ns)
        if n_complemento != None:
            complemento = {}

            n_timbre = n_complemento.find("tfd:TimbreFiscalDigital", ns)

            complemento.update({
                'TimbreFiscalDigital': extract_attrs_without(n_timbre.attrib)
            })

            new_cfdi.update({"Complemento": complemento})
        # END Complemento
        cfdis_to_insert.append(new_cfdi)

    # END For xmls
    result = len(db.cfdis.insert_many(cfdis_to_insert).inserted_ids)

    return result


@bp.route('', methods=['POST'])
def insert_many_cfdis():
    body = request.get_json()

    request_id = body['idrequest']
    # obtenemos los datos desde el body de la request
    applicant = db.satInformations.find_one({
        '_id': ObjectId(body['id'])
    })

    # Armamos la data para solicitar datos al sat
    rfc_applicant = applicant["rfc"]

    request_found = db.requestsCfdis.find_one({
        "_id": request_id
    })

    result_update_request = 0
    if not request_found["status"]:
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
        download = DescargaMasiva(fiel)

        check_download = verify_download.verificar_descarga(token,
                                                            rfc_applicant,
                                                            request_id)

        numcfdis = 0
        if check_download['estado_solicitud'] == '3':
            packages_result = check_download['paquetes']
            for package in packages_result:
                binary_file_path = os.getcwd() + '/temp/'
                zip_path = binary_file_path + package + '.zip'
                folder_extract = binary_file_path + package

                result_download = download.descargar_paquete(token,
                                                             rfc_applicant,
                                                             package)

                data = result_download['paquete_b64']

                decoded = base64.b64decode(data)
                with open(zip_path, 'wb') as f:
                    f.write(decoded)

                zf = zipfile.ZipFile(zip_path, 'r')
                zf.extractall(folder_extract)
                zf.close()

                # Después de convertir y extraer al folder temp
                # Debemos de recorrer file por file de la new folder y almacenar en la bd

                list_files = [f for f in os.listdir(folder_extract)]

                numcfdis = numcfdis + insert_cfdis(list_files,
                                                   folder_extract,
                                                   applicant["_id"],
                                                   request_id)
                # END For packages

            # Actualizamos la solicitud
            result_update_request = db.requestsCfdis.update_one(
                {"_id": request_id},
                {"$set": {
                    "status": True,
                    "numcfdis": numcfdis,
                    "datedownload": datetime.now(),
                    "downloads": check_download['paquetes']
                }}
            ).modified_count

        elif check_download['codigo_estado_solicitud'] == '5004':
            result_update_request = db.requestsCfdis.update_one(
                {"_id": request_id},
                {"$set": {
                    "status": True,
                    "numcfdis": 0,
                    "datedownload": datetime.now()
                }}
            ).modified_count

    return jsonify({'status': 'success', 'data': {'modified_count': result_update_request}}), 201


@bp.route('/totalcfdistotype/<id>', methods=['GET'])
def total_cdfis_to_type(id):
    found_request_type = None

    type_user = request.args.get('typeuser')
    applicant = db.satInformations.find_one({"_id": ObjectId(id)})

    last_receptor_cfdi = db.cfdis.find_one(
        {
            'info_id': ObjectId(id),
            'Receptor.Rfc': applicant['rfc']
        }, {
            'Fecha': 1,
            '_id': 0
        },
        sort=list({'Fecha': -1}.items()))

    last_emisor_cfdi = db.cfdis.find_one(
        {
            'info_id': ObjectId(id),
            'Emisor.Rfc': applicant['rfc']
        }, {
            'Fecha': 1,
            '_id': 0
        },
        sort=list({'Fecha': -1}.items()))

    if type_user != 'g':
        found_request_type = db.requestsCfdis.aggregate([
            {
                '$match': {
                    'info_id': ObjectId(id)
                }
            }, {
                '$group': {
                    '_id': '$typerequest',
                    'totalCfdis': {
                        '$sum': '$numcfdis'
                    }
                }
            }
        ])
    else:
        found_request_type = [{"_id": "r", "totalCfdis": None},
                              {"_id": "e", "totalCfdis": None}]

    return jsonify({'status': 'success', 'data': {
        'typesCFDI': json.loads(dumps(found_request_type)),
        'lastReceptorCFDI': json.loads(dumps(last_receptor_cfdi)),
        'lastEmisorCFDI': json.loads(dumps(last_emisor_cfdi)),
    }, }), 200


@bp.route('/<id>', methods=['GET'])
def get_cfdi(id):
   # TODO: Agregar validacion por si no existe
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


@bp.route('/getcfdis/<info_id>', methods=['GET', 'POST'])
def get_cfdis(info_id):
    body = None
    if request.method == 'POST':
        body = request.get_json()

    page_size = int(request.args.get('pagesize'))
    page_num = int(request.args.get('pagenum'))
    type_comprobante = request.args.get('typecomprobante')
    type_request = request.args.get('typerequest')

    cfdis, data_total_monto = get_limit_cfdis(page_size=page_size,
                                              page_num=page_num,
                                              info_id=info_id,
                                              type_comprobante=type_comprobante,
                                              type_request=type_request,
                                              filters=body
                                              )

    return jsonify({'status': 'success', 'data': {
        'dataPagination': json.loads(data_total_monto),
        'cfdis': json.loads(cfdis)
    }}), 200


def get_limit_cfdis(page_size, page_num, info_id, type_comprobante, type_request, filters):
    # Calculate number of documents to skip
    skips = page_size * (page_num - 1)

    applicant = db.satInformations.find_one({'_id': ObjectId(info_id)})

    comprobante_types = type_comprobante.split("-")

    # PARA MATCH
    # Emisor.Rfc or Receptor.Rfc por type_request
    # Receptor.UsoCFDI
    # Emisor.Rfc or Receptor.Rfc por filter
    # Range convertedFecha
    filter_type_request = {
        'TipoDeComprobante': {'$in': comprobante_types}
    }

    project_type_request = {
        'Total': 1,
        'Fecha': 1,
    }

    if type_request == 'r':
        filter_type_request.update({'Receptor.Rfc': applicant["rfc"]})
        project_type_request.update({'Rfc': '$Emisor.Rfc'})
    elif type_request == 'e':
        filter_type_request.update({'Emisor.Rfc': applicant["rfc"]})
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

        if filters['usocfdi'] != '':
            # CHECAR: esta parte la tenemos que hacer con un verdadero catalogo
            switcher = {
                "Adquisición de mercancias": "G01",
                "Devoluciones, descuentos o bonificaciones": "G02",
                "Gastos en general": "G03"
            }
            filter_type_request.update({
                'Receptor.UsoCFDI': switcher[filters['usocfdi']]
            })

    # Requires the PyMongo package. POAG760804RP8
    # https://api.mongodb.com/python/current

    list_cfdis = list(db.cfdis.aggregate([
        {
            '$match': filter_type_request
        }, {
            '$project': project_type_request
        }, {
            '$sort': {
                'Fecha': -1
            }
        }, {
            '$skip': skips
        }, {
            '$limit': page_size
        }
    ]))

    pagination_monto = list(db.cfdis.aggregate([
        {
            '$match': filter_type_request
        }, {
            '$project': project_type_request
        }, {
            '$addFields': {
                'Monto': {'$toDecimal': '$Total'}
            }
        }, {
            '$group': {
                '_id': None,
                'totalMonto': {
                    '$sum': '$Monto'
                },
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
