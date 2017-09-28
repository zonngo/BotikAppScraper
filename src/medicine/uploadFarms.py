import json
import logging
import os

from db.MedicineModels import FarmaciaModel
from db.MedicineModels import PersonalModel


def upload(file_in, tipo):
    def createUniqueFarms():
        logging.info("------------------------------create UniqueProdFarm")
        if (tipo == 0):
            id = 'id_f'
        if (tipo == 1):
            id = 'id'
        if (tipo == 2):
            id = 'txtNroRegistro'
        name = 'data/UniqueFarms.json'
        archW = open(name, 'w')
        S = set()
        with open(file_in, 'r') as archR:
            cantFarms = 0
            for reg in archR:
                obj = json.loads(reg)
                cantFarms += 1
                if (cantFarms % 1000000 == 0):
                    logging.debug("------------------------------Cant. de registros procesados: %d" % cantFarms)
                if obj[id] not in S:
                    S.add(obj[id])
                    archW.write(reg)

        archW.close()
        return name

    def removeUniqueFarms(name):
        logging.info("------------------------------archivo removido ...")
        os.remove(name)

    def get_farms():
        name = createUniqueFarms()
        ans = []
        with open(name, "r") as arch:
            for reg in arch:
                reg = json.loads(reg)
                if (tipo == 0):
                    farm = {
                        "estado": "0",
                        "id": int(reg["id_f"]),
                        "tipo_est": int(reg["t_est"])
                    }
                if (tipo == 1):
                    farm = {
                        "estado": "1",
                        "id": int(reg["id"]),
                        "direccion": reg["direccion"],
                        "provincia": reg["provincia"],
                        "dtecnico": reg["dtecnico"],
                        "horario": reg["horario"],
                        "telefono": reg["telefono"]
                    }
                if (tipo == 2):
                    farm = {
                        "estado": "2",
                        "id": int(reg["txtNroRegistro"]),
                        "direccion": reg["txtDireccion"],
                        "provincia": reg["txtProvincia"],
                        "distrito": reg["txtDistrito"],
                        "departamento": reg["txtDpto"],
                        "nombre": reg["txtNombreComercial"],
                        "ruc": reg["txtNroRuc"],
                        "situacion": reg["txtSituacion"],
                        "categoria": reg["txtCategoria"],
                        "razonSocial": reg["txtRazonSocial"],
                        "horario": reg["txtHorario"],
                        "personal": reg["personal"]
                    }
                ans.append(farm)
        removeUniqueFarms(name)
        logging.info("------------------------len() = %d" % len(ans))
        return ans

    def save_tb(tps, mmd, camps):
        set_tp = set()
        for tp in mmd.get_all(cols=mmd.primary_key):
            set_tp.add("@".join([str(tp[x]) for x in mmd.primary_key]))
        saves = []
        updates = []
        for tp in tps:
            ID = "@".join([str(tp[x]) for x in mmd.primary_key])
            if ID not in set_tp:
                saves.append(tp)
            else:
                updates.append(tp)
        logging.info("------------------------len(saves) = %d" % len(saves))
        logging.info("------------------------len(updat) = %d" % len(updates))
        mmd.save(saves)
        mmd.update(camps, updates)

    def save2(tps, mmd, camps):
        set_tp = set()
        camps2 = mmd.primary_key + ['estado']
        estado = {}
        for tp in mmd.get_all(cols=camps2):
            ID = "@".join([str(tp[x]) for x in mmd.primary_key])
            set_tp.add(ID)
            estado[ID] = tp['estado']
        saves = []
        updates = []
        for tp in tps:
            ID = "@".join([str(tp[x]) for x in mmd.primary_key])
            if ID not in set_tp:
                saves.append(tp)
            else:
                if tp['estado'] < estado[ID]:
                    tp['estado'] = estado[ID]
                updates.append(tp)
        logging.info("------------------------len(saves) = %d" % len(saves))
        logging.info("------------------------len(updat) = %d" % len(updates))
        mmd.save(saves)
        mmd.update(camps, updates)

    def save_all():
        logging.info('------------------------uploadfarms...')
        CampsProdFarms = ['id', 'tipo_est']
        CampsFarms1 = ['id', 'estado', 'direccion', 'provincia', 'dtecnico', 'horario', 'telefono']
        CampsFarms2 = ['id', 'estado', 'direccion', 'provincia', 'distrito', 'departamento', 'nombre', 'ruc',
                       'situacion', 'categoria', 'razonSocial', 'horario']
        CampsPerson = ["idF", "nombre", "cargo", "dni", "horario", "tipo"]
        gfs = get_farms()
        if (tipo == 0):
            updateCamps = CampsProdFarms
        if (tipo == 1):
            updateCamps = CampsFarms1
        if (tipo == 2):
            updateCamps = CampsFarms2
            persFarms = []
            for gf in gfs:
                persFarms.append(gf['personal'])
                gf['personal'] = ""
        save2(gfs, FarmaciaModel(), updateCamps)
        logging.info("------------------------Farmacia has been uploaded")
        if (tipo == 2):
            logging.info("------------------------uploadPersonal...")
            vect = []
            S = set()
            for pf in persFarms:
                for p in pf:
                    ID = p["nombre"] + "@" + str(p["idF"]) + "@" + str(p["tipo"])
                    if ID in S:
                        continue
                    S.add(ID)
                    vect.append(p)
            save_tb(vect, PersonalModel(), CampsPerson)
            logging.info("------------------------Personal has been uploaded")

    save_all()


if __name__ == '__main__':
    logging.info('------------------------**prod_farm.json')
    upload('data/prod_farm.json', 0)
    logging.info('------------------------**farms.json')
    upload('data/farms.json', 1)
    logging.info('------------------------**farms2.json')
    upload('data/farms2.json', 2)
