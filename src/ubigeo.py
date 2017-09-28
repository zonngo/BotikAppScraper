import json

import requests
from bs4 import BeautifulSoup

DATA = []


def getter(url, JSON):
    session = requests.Session()
    session.headers.clear()
    session.headers['Content-Type'] = 'application/json'
    data = session.post(url, data=json.dumps(JSON)).json()['d']
    datos = [{'id': x['Codigo'], 'name': x['Descripcion']} for x in data]
    return datos


def getDistrito(idProv, idDpto):
    return getter('http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/BusquedaGral.aspx/GetDistritos',
                  {'cod_dep': idDpto,
                   'cod_prov': idProv})


def getProvincia(idDpto):
    datos = getter('http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/BusquedaGral.aspx/GetProvincias',
                   {'cod_dep': idDpto})
    data = []
    for da in datos:
        data.append({
            'id': da['id'],
            'name': da['name'],
            'distr': getDistrito(da['id'], idDpto)
        })
    return data


def getDptos():
    html = BeautifulSoup(
        requests.get('http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/BusquedaGral.aspx?' +
                     'grupo=2926*3&total=1*1&con=500*mg&ffs=3&ubigeo=17&cad=PARACETAMOL*500*mg*Tableta*-*Capsula').text,
        'html.parser')
    for da in html.find('select').find_all('option'):
        DATA.append({'id': da['value'], 'name': da.text,
                     'prov': getProvincia(da['value'])})
    return DATA


if __name__ == '__main__':
    with open('data/ubigeo.json', 'w') as f:
        f.write(json.dumps(getDptos(), ensure_ascii=False, indent=2))
