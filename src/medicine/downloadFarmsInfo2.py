import json
import logging
import os
import shutil
import threading

import requests
from bs4 import BeautifulSoup

URL = "http://observatorio.digemid.minsa.gob.pe/PortalConsultas/Consultas/ConsultaEstablecimientosDetalle.aspx?establecimiento="

dirI = 'data/inputScrap5'
dirO = 'data/outputScrap5'


def complet(num):
    id = str(num)
    while (len(id) != 7):
        id = '0' + id
    return id


class ScraperThread:
    def __init__(self, campos, nro_threads):
        self.campos = campos
        self.nro_threads = nro_threads

    def scrapProds(self, idf):
        reg = None
        path = URL + idf
        try:
            req = requests.get(path)
            statusCode = 200
        except Exception as e:
            statusCode = 0
        if statusCode == 200:
            reg = {}
            bs = BeautifulSoup(req.text, "html.parser")
            for camp in self.campos:
                reg[camp] = bs.find("input", {"name": camp}).get("value")

            reg["personal"] = []

            TD = ["nombre", "cargo", "dni", "horario"]
            row1 = bs.find("table", {"id": "GridView2"})
            row1 = row1.find_all("tr")
            for it1 in range(1, len(row1)):
                row2 = row1[it1].find_all("td")
                if row2[0].text == "":
                    continue
                obj = {
                    "nombre": "",
                    "cargo": "",
                    "dni": "",
                    "id": "0",
                    "idF": int(reg["txtNroRegistro"]),
                    "horario": "",
                    "tipo": "1"
                }
                for it in range(3):
                    try:
                        obj[TD[it]] = row2[it].text
                    except:
                        logging.error("path = " + path)
                reg["personal"].append(obj)

            TD = ["nombre", "cargo", "horario"]
            row1 = bs.find("table", {"id": "GridView1"})
            row1 = row1.find_all("tr")
            for it1 in range(1, len(row1)):
                row2 = row1[it1].find_all("td")
                if row2[0].text == "":
                    continue
                obj = {
                    "nombre": "",
                    "cargo": "",
                    "horario": "",
                    "id": "0",
                    "idF": int(reg["txtNroRegistro"]),
                    "dni": "",
                    "tipo": "2"
                }
                for it in range(3):
                    try:
                        obj[TD[it]] = row2[it].text
                    except:
                        logging.error("path = " + path)
                reg["personal"].append(obj)
        return reg

    def run(self, idT):
        nameR = dirI + ('/archivo%d.json' % idT)
        nameW = dirO + ('/archivo%d.json' % idT)
        archW = open(nameW, 'w')
        with open(nameR, 'r') as arch:
            cntReg = 0
            for idf in arch:
                intento = 0
                regScrap = None
                while intento < 10 and regScrap is None:
                    if intento > 0:
                        logging.error('Thread nro: %d, intento: %d fallo' \
                                      % (idT, intento))
                    regScrap = self.scrapProds(idf)
                    intento += 1
                if intento == 10:
                    logging.critical('Thread nro: %d die' % idT)
                if regScrap is not None:
                    json.dump(regScrap, archW, ensure_ascii=False)
                    archW.write('\n')
                cntReg += 1
                if cntReg % 50 == 0:
                    logging.debug("Scrap Thread%d , porc: %d" % (idT, cntReg))
        logging.info('Finish Thread nro:%d' % (idT))
        archW.close()


def createArchivos(archLectura, nro_threads):
    logging.info('createDirectorios')
    if (not os.path.exists(dirI)):
        os.mkdir(dirI)
    if (not os.path.exists(dirO)):
        os.mkdir(dirO)
    logging.info('createArchivos')
    cnt = total = 0
    with open(archLectura, 'r') as regs:
        for reg in regs:
            total += 1
    limit = total // nro_threads
    # return limit
    with open(archLectura, 'r') as regs:
        for reg in regs:
            if cnt % limit == 0 and cnt // limit < nro_threads:
                name = dirI + ('/archivo%d.json' % (cnt // limit))
                arch = open(name, 'w')
            if cnt % 10000 == 0:
                logging.debug('creando archivo%d : %.2f' % ((cnt // limit), ((cnt % limit) * 100 / limit)))
            reg = json.loads(reg)
            ID = complet(reg['id'])
            arch.write(ID + '\n')
            cnt += 1
            if cnt % limit == 0 and cnt // limit < nro_threads:
                arch.close()
    return limit


def deleteArchivos():
    logging.info('Eliminando directorios')
    shutil.rmtree(dirI)
    shutil.rmtree(dirO)


def download(nro_threads, a, b):
    logging.info('en dow...')
    limit = createArchivos(a, nro_threads)
    logging.info('cantidad de registros por archivo:' + str(limit))
    campos = ["txtNroRegistro", "txtSituacion", "TxtLugarReg", "txtFecha",
              "txtNroRuc", "txtCategoria", "txtNombreComercial",
              "txtRazonSocial", "txtDireccion", "txtDpto", "txtProvincia",
              "txtDistrito", "txtHorario"]
    logging.info('working whit %d threads.' % nro_threads)
    STrun = ScraperThread(campos, nro_threads).run
    sts = []
    for idt in range(nro_threads):
        st = threading.Thread(target=STrun, args=(idt,))
        sts.append(st)
    for st in sts:
        st.start()
    for st in sts:
        st.join()
    logging.info('Scrap Finished')
    logging.info('escribiendo el archivo:%s' % b)
    archW = open(b, 'w')
    for idt in range(nro_threads):
        archR = dirO + ('/archivo%d.json' % idt)
        with open(archR, 'r') as regs:
            for reg in regs:
                archW.write(reg)
    archW.close()
    logging.info('finalizo la escritura del archivo %s' % b)
    deleteArchivos()


if __name__ == '__main__':
    a = '../data/farms.json'
    b = '../data/farms2.json'
    download(20, a, b)
