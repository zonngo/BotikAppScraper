import csv
import logging

from db.MedicineModels import LaboratorioModel
from db.MedicineModels import PresentacionModel
from db.MedicineModels import ProductoModel
from db.MedicineModels import RegSanModel
from db.MedicineModels import TipoPresentacionModel


def upload(file_catalog):
    SEPARATOR = ";"

    def prod_json():
        prods = {}
        with open(file_catalog, "r") as f:
            csvrd = csv.DictReader(f, delimiter=SEPARATOR)

            for row in csvrd:
                prod = {
                    "nombre": row["Nom_Prod"].strip(),
                    "concent": row["Concent"].strip(),
                    "tp_pre": row["Nom_Form_Farm"].strip(),
                    "tp_pre_si": row["Nom_Form_Farm_Simplif"].strip(),
                    "presentacion": row["Presentac"].strip(),
                    "fracciones": int(row["Fracciones"]),
                    "reg_san_ven": row["Fec_Vcto_Reg_Sanitario"],
                    "reg_san": row["Num_RegSan"],
                    "laboratorio": row["Nom_Titular"].strip(),
                    "Situacion": row["Situacion"].strip()
                }

                prods[row["Cod_Prod"]] = prod
        return prods

    def get_tp_pre(prods):
        tp_pre = set()
        for k, v in prods.items():
            tp_pre.add(v["tp_pre"].strip())
            tp_pre.add(v["tp_pre_si"].strip())

        tp_pre = sorted(list(tp_pre))
        ans = []
        for tp in tp_pre:
            ans.append({"id": 0, "descripcion": tp})
        return ans

    def get_key(item):
        return item["codigo"]

    def get_key2(item):
        return item["id"]

    def get_reg_san(prods):
        regss = set()
        regs = []
        for k, v in prods.items():
            regn = v["reg_san"].replace(' ', '')
            if not regn in regss:
                regss.add(regn)
                reg_san = {
                    "id": 0,
                    "codigo": regn,
                    "vencimiento": "-".join(reversed(v["reg_san_ven"][0:10].split("/")))
                }
                if len(reg_san["vencimiento"]) != 10:
                    reg_san["vencimiento"] = None
                regs.append(reg_san)
        regs = sorted(regs, key=get_key)
        return regs

    def get_labs(prods):
        labs = set()
        for k, v in prods.items():
            labs.add(v["laboratorio"])

        labs = sorted(list(labs))
        ans = []
        for lb in labs:
            ans.append({"id": 0, "nombre": lb})
        return ans

    def get_presentacion(prods):
        ans = set()
        for k, v in prods.items():
            ans.add(v["presentacion"])

        ps = sorted(list(ans))
        ans = []
        for p in ps:
            ans.append({"id": 0, "descripcion": p})
        return ans

    def get_prods(prods):
        lbm = LaboratorioModel()
        map_lb = {}
        for lb in lbm.get_all():
            map_lb[lb["nombre"]] = lb["id"]

        tpm = TipoPresentacionModel()
        map_tp = {}
        for tp in tpm.get_all():
            map_tp[tp["descripcion"]] = tp["id"]

        rsm = RegSanModel()
        map_rs = {}
        for rs in rsm.get_all():
            map_rs[rs["codigo"]] = rs["id"]

        pm = PresentacionModel()
        map_p = {}
        for p in pm.get_all():
            map_p[p["descripcion"]] = p["id"]

        ans = []
        for k, v in prods.items():
            if (len(v["Situacion"]) != 0 and v["Situacion"][0] == 'A'):
                v["Situacion"] = 'Activo'
            else:
                v["Situacion"] = 'Inactivo'
            pr = {
                "id": int(k),
                "estado": 0,
                "nombre": v["nombre"],
                "concent": v["concent"],
                "tp_pre": map_tp[v["tp_pre"]],
                "tp_pre_si": map_tp[v["tp_pre_si"]],
                "fracciones": v["fracciones"],
                "reg_san": map_rs[v["reg_san"].replace(' ', '')],
                "laboratorio": map_lb[v["laboratorio"]],
                "presentacion": map_p[v["presentacion"]],
                "Situacion": v["Situacion"]
            }

            ans.append(pr)
        ans = sorted(ans, key=get_key2)
        return ans

    def save_tb(tps, columna, mmd, camps):
        set_tp = set()
        for tp in mmd.get_all(cols=[columna]):
            set_tp.add(tp[columna])
        saves = []
        updates = []
        for tp in tps:
            if tp[columna] not in set_tp:
                saves.append(tp)
            else:
                updates.append(tp)
        logging.info("------------------------len(saves) = %d" % len(saves))
        logging.info("------------------------len(updat) = %d" % len(updates))
        mmd.save(saves)
        mmd.update(camps, updates)

    def save_all():
        prs = prod_json()

        updateCampsLab = ['id', 'nombre']
        updateCampsRegSan = ['id', 'codigo', 'vencimiento']
        updateCampsTipoP = ['id', 'descripcion']
        updateCampsPres = ['id', 'descripcion']
        updateCampsProds = ['id', 'nombre', 'concent', 'tp_pre', 'tp_pre_si',
                            'fracciones', 'reg_san', 'laboratorio', 'presentacion', 'Situacion']

        logging.info('Uploadind laboratories.')
        save_tb(get_labs(prs), "nombre", LaboratorioModel(), updateCampsLab)
        logging.info("Laboratorios has been uploaded")

        logging.info("Uploading Registros Sanitarios.")
        save_tb(get_reg_san(prs), "codigo", RegSanModel(), updateCampsRegSan)
        logging.info("Registros Sanitarios has been uploaded")

        logging.info("Tipo de Presentacion has been uploaded")
        save_tb(get_tp_pre(prs), "descripcion", TipoPresentacionModel(), updateCampsTipoP)
        logging.info("Tipo de Presentacion has been uploaded")

        logging.info("Uploading presentacion.")
        save_tb(get_presentacion(prs), "descripcion", PresentacionModel(), updateCampsPres)
        logging.info("Presentacion has been uploaded")

        logging.info('Uploading Producto')
        save_tb(get_prods(prs), "id", ProductoModel(), updateCampsProds)
        logging.info("Producto has been uploaded")

    save_all()


if __name__ == '__main__':
    upload('data/products.csv')
