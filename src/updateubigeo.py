import json

from db.MedicineModels import Ubigeo

PATH = 'data/ubigeo.json'


def main():
    ubm = Ubigeo()

    with open(PATH) as f:
        ubData = json.loads(f.read())

    rows = []
    for dep in ubData:
        rows.append({
            'id': dep['id'],
            'name': dep['name'],
            'tipo': 1,
            'parent': None
        })

        for prov in dep['prov']:
            rows.append({
                'id': dep['id'] + prov['id'],
                'name': prov['name'],
                'tipo': 2,
                'parent': dep['id']
            })

            for dist in prov['distr']:
                rows.append({
                    'id': dist['id'],
                    'name': dist['name'],
                    'tipo': 3,
                    'parent': dep['id'] + prov['id']
                })
    ubm.save(rows)


if __name__ == "__main__":
    main()
