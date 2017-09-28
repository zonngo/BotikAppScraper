import json

from db.MedicineModels import ProdSugModel
from db.MedicineModels import ProductoModel

mp = {}
stp = set()
rows2 = []
for line in open('data/prod_farm.json'):
    a = json.loads(line)
    if a['id_p'] not in mp:
        mp[a['id_p']] = set()

    key = a['idsug'].upper().strip()
    mp[a['id_p']].add(key)
    if a['id_p'] not in stp:
        stp.add(a['id_p'])
        cad = " ".join([x for x in a['cad'].strip().upper().split() if len(x) > 0])
        rows2.append({
            'id': a['id_p'],
            'sug': cad
        })

rows = []
for k, v in mp.items():
    for idsug in list(v):
        rows.append({
            'idp': k,
            'ids': idsug
        })

psm = ProdSugModel()
psm.save(rows, True)

pm = ProductoModel()
pm.update(['sug'], rows2)
