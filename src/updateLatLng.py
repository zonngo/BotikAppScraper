import sys

import requests

from db.MedicineModels import FarmaciaModel
from db.MedicineModels import Ubigeo

URL = "http://serviciosweb.digemid.minsa.gob.pe/DigemidWebApi/apiestablecimiento/BuscarEstablecimientoPorUbigeo?"


def get_data(departamento, provincia, distrito, categoria, estado, nombreRuc):
    query_params = {
        "dpto": departamento,
        "prov": provincia,
        "dist": distrito,
        "categoria": categoria,
        "estado": estado,
        "nombreruc": nombreRuc
    }
    nro_intentos = 0
    while nro_intentos < 5:
        try:
            resp = requests.get(URL, query_params).json()
            return resp
        except Exception as e:
            print(e, file=sys.stderr)
            nro_intentos += 1

    return None


def main():
    # get_data('15', '01', '03', '06', '01', '')
    um = Ubigeo()
    fm = FarmaciaModel()

    farms = {x['id'] for x in fm.where({'estado': '2'})}
    print(len(farms))

    dists = um.where({'tipo': 3})

    rows = []
    for dist in dists:
        codu = str(dist['id'])
        if (len(codu) < 6):
            codu = '0' + codu

        print(codu)
        a = []
        a.extend(get_data(codu[0:2], codu[2:4], codu[4:6], '03', '01', ''))
        a.extend(get_data(codu[0:2], codu[2:4], codu[4:6], '04', '01', ''))
        a.extend(get_data(codu[0:2], codu[2:4], codu[4:6], '06', '01', ''))

        for res in a:
            if not res['ESTDIRLATITUD'] or int(res['ESTNUMEINS']) not in farms:
                continue
            rows.append({
                'id': int(res['ESTNUMEINS']),
                'lat': res['ESTDIRLATITUD'],
                'lng': res['ESTDIRLONGITUD'],
                'estado': '3'
            })

    print(len(rows))
    fm.update(['lat', 'lng', 'estado'], rows)


# Estados: 01-Activo, 02-Cierre temporal, 03-cierre definitivo
# Categorias: 03-Farmacia, 04-Botica, 06-Farmacia de los establecimientos de salud
if __name__ == "__main__":
    main()
    # print(json.dumps(get_data('15', '01', '01', '03', '01', ''), indent=2))
