import json
import logging

from db.MedicineModels import ProductoModel
from db.MedicineModels import RegSanModel


def upload(file_in):
    def get_key(item):
        return item["id"]

    def upload_regsan():
        rgm = RegSanModel()
        set_rs = {x['codigo'].upper() for x in rgm.get_all(['codigo'])}

        rows = []
        with open(file_in) as f:
            for line in f:
                pf = json.loads(line)
                cod = pf['Registro'].strip().upper()
                if len(cod) < 10 and cod not in set_rs:
                    rows.append({
                        'id': None,
                        'codigo': cod
                    })
                    set_rs.add(cod)
        rgm.save(rows)

    def get_prods():
        rgm = RegSanModel()
        map_rs = {x['codigo'].upper(): x['id'] for x in rgm.get_all(['id', 'codigo'])}

        ans = []
        st_id = set()

        with open(file_in, "r") as f:
            for line in f:
                pf = json.loads(line)
                if pf["idP"] not in st_id:
                    prod = {
                        "id": int(pf["idP"]),
                        "estado": int(pf["estado"]),
                        "nombre": pf["Medicamento"],
                        "reg_san": pf["Registro"].strip().upper(),
                        "condicionV": pf["CondicionV"],
                        "tipo": pf["Marca"],
                        "fabricante": pf["Fabricante"]
                    }
                    if len(prod['reg_san']) >= 10:
                        prod['reg_san'] = None
                    else:
                        prod['reg_san'] = map_rs[prod['reg_san']]
                    ans.append(prod)
                    st_id.add(pf["idP"])

        ans = sorted(ans, key=get_key)
        return ans

    def save_tb(tps, mmd, camps):
        set_tp = set()
        for tp in mmd.get_all(cols=mmd.primary_key):
            set_tp.add("@".join([str(tp[x]) for x in mmd.primary_key]))
        saves = []
        updates = []
        for tp in tps:
            if "@".join([str(tp[x]) for x in mmd.primary_key]) not in set_tp:
                saves.append(tp)
            else:
                updates.append(tp)
        logging.info("------------------------len(saves) = %d" % len(saves))
        logging.info("------------------------len(updat) = %d" % len(updates))
        mmd.save(saves)
        mmd.update(camps, updates)

    def save_all():
        logging.info('------------------------uploadProds...')
        updateCampsProds = ["estado", "condicionV", "tipo", "fabricante"]
        save_tb(get_prods(), ProductoModel(), updateCampsProds)
        logging.info("------------------------Productos has been uploaded")

    upload_regsan()
    save_all()


if __name__ == '__main__':
    upload('data/prods.json')
