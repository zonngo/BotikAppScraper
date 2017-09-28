#!/usr/bin/env python3

import json

from db.MedicineModels import ProductoModel

SUG_FILE = 'data/pactivo.json'
PF_FILE = 'data/prod_farm.json'


def main():
    mp = {}
    with open(SUG_FILE) as f:
        for line in f:
            a = json.loads(line)
            key = a['id'] + '@' + a['value'].upper()
            mp[key] = a['pactivo'].upper()

    mp2 = {}
    with open(PF_FILE) as f:
        for line in f:
            a = json.loads(line)
            key = a['idsug'] + '@' + a['cad'].upper()
            if not a['id_p'] in mp2:
                mp2[a['id_p']] = set()

            mp2[a['id_p']].add(mp[key])

    # for k, v in mp2.items():
    #     if len(v) > 1:
    #         print(k, v)

    rows = []
    for k, v in mp2.items():
        rows.append({
            'id': k,
            'pactivo': list(v)[0]
        })

    pm = ProductoModel()
    pm.update(['pactivo'], rows)


if __name__ == "__main__":
    main()
