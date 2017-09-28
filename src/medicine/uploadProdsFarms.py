import csv
import json
import logging
import os
from datetime import datetime

from db import db

name1 = '.ProdFarm.csv'
name2 = '.SortedProdFarm.csv'
name3 = '.saves.csv'
name4 = '.updates.csv'
prod = 10000000


def SortedArch():
    logging.info("Ordenando Archivo ...")
    cmd = 'sort %s -o %s' % (name1, name2)
    os.system(cmd)
    logging.info("Archivo ordenado ...")


def createArchCSV(file_in):
    logging.info("Creando Archivo")
    S = set()
    cantPF = 0
    with open(name1, 'w') as archW:
        archCsv = csv.writer(archW)
        with open(file_in, 'r') as archR:
            for line in archR:
                cantPF += 1
                if (cantPF % 1000000 == 0):
                    logging.debug("Cant. registros procesados: %d" % cantPF)
                obj = json.loads(line)
                ID = int(obj['id_p']) * prod + int(obj['id_f'])
                if (ID not in S):
                    S.add(ID)
                    archCsv.writerow([prod + int(obj['id_p']), obj['id_f'], obj['pr'],
                                      datetime.strptime(obj["f_act"].replace(".", ""), "%d/%m/%Y %H:%M:%S %p")])
    logging.info("Archivo creado")


def removeArch(name):
    os.remove(name)


def siguiente(cursor):
    try:
        reg = cursor.next()
        IDdb = int(reg[0])
    except:
        IDdb = 10 ** 16
    return IDdb


def Clasificar():
    logging.info("Clasificando registros ...")
    query = 'select (idP*10000000 + idF) as ID from PROD_FARM order by ID'
    conn = db.medicine_conn
    cursor = conn.cursor()
    cursor.execute(query)

    archS = open(name3, 'w')
    archS = csv.writer(archS)
    archU = open(name4, 'w')
    archU = csv.writer(archU)

    cantDB = cantRE = 0
    with open(name2, 'r') as archR:
        archR = csv.reader(archR)
        IDdb = siguiente(cursor)
        cantDB += 1
        for re in archR:
            IDre = (int(re[0]) - prod) * prod + int(re[1])
            cantRE += 1
            if (cantRE % 1000000 == 0):
                logging.debug("cantRE: %d" % cantRE)
            while (IDre > IDdb):
                IDdb = siguiente(cursor)
                cantDB += 1
                if (cantDB % 1000000 == 0):
                    logging.debug("cantDB: %d" % cantDB)
            if (IDre == IDdb):
                archU.writerow([int(re[0]) - prod, re[1], re[2], re[3]])
            if (IDre < IDdb):
                archS.writerow([int(re[0]) - prod, re[1], re[2], re[3]])
    conn.close()
    logging.info("Registros Clasificados ...")


def SaveToDB():
    logging.info("save --> DB")
    query = "LOAD DATA LOCAL INFILE '%s' " % name3
    query += 'INTO TABLE PROD_FARM '
    query += "FIELDS TERMINATED BY ',' "
    query += "LINES TERMINATED BY '\n' "
    query += "(idP, idF, precio, fecha_act) "

    conn = db.medicine_conn
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()
    logging.info("save --> DB finish")


def UpdateToDB():
    conn = db.medicine_conn
    cursor = conn.cursor()

    logging.info("update --> DB")
    query = 'drop table if exists PROD_FARM_TMP'
    cursor.execute(query)

    query = 'create table PROD_FARM_TMP ( '
    query += 'idP int, '
    query += 'idF int, '
    query += 'fecha_act datetime, '
    query += 'precio float )'
    cursor.execute(query)

    query = "LOAD DATA LOCAL INFILE '%s' " % name3
    query += 'INTO TABLE PROD_FARM_TMP '
    query += "FIELDS TERMINATED BY ',' "
    query += "LINES TERMINATED BY '\n' "
    query += "(idP, idF, precio, fecha_act)"
    cursor.execute(query)

    query = 'update PROD_FARM as pf1 join PROD_FARM_TMP as pf2 '
    query += 'on pf1.idP = pf2.idP and pf1.idF = pf2.idF set '
    query += 'pf1.idP = pf2.idP, '
    query += 'pf2.idF = pf2.idF, '
    query += 'pf1.fecha_act = pf2.fecha_act, '
    query += 'pf1.precio = pf2.precio'
    cursor.execute(query)

    query = 'drop table PROD_FARM_TMP'
    cursor.execute(query)

    conn.commit()
    conn.close()
    logging.info("update --> DB finish")


def upload(file_in):
    createArchCSV(file_in)  # name1
    SortedArch()  # name2
    Clasificar()  # name2
    SaveToDB()  # name3
    UpdateToDB()  # name4
    removeArch(name1)
    removeArch(name2)
    removeArch(name3)
    removeArch(name4)


if __name__ == '__main__':
    upload('data/prod_farm.json')
