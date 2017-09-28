#!/usr/bin/env python3
import json
import logging
import threading

import requests
from bs4 import BeautifulSoup

FREAD, FWRITE = None, None
lock_read, lock_write = threading.Lock(), threading.Lock()
pg_cnt = 0


def read_sug():
    if not FREAD:
        return None
    ans = None
    lock_read.acquire()
    try:
        line = FREAD.readline()
        ans = json.loads(line)
    except IOError as ie:
        logging.error(str(ie))
    except Exception as e:
        logging.error(str(e))
    lock_read.release()
    return ans


def write_sug(dat):
    global pg_cnt
    lock_write.acquire()
    FWRITE.write(json.dumps(dat, ensure_ascii=False) + '\n')
    pg_cnt += 1
    logging.info(str(pg_cnt) + ' tasks completed.')
    lock_write.release()


class ScraperThread:
    def __init__(self, id_thread):
        self.id_thread = id_thread
        self.session = requests.Session()
        self.session.headers.clear()

    def log(self, message):
        logging.debug("Thread " + str(self.id_thread) + " -> " + message)

    def find(self, sug):
        url = "http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/BusquedaGral.aspx?"
        url += "grupo=" + str(sug['grupo']) + "*3&total=" + str(sug['total_pa']) + \
               "*1&con=" + sug['con'].replace(' ', '*') + \
               "&ffs=" + str(sug['ffs'])
        url += "&ubigeo=15&cad=" + sug['value'].replace(' ', '*')
        PA = BeautifulSoup(self.session.get(url).text, 'html.parser').find(
            'span', {'id': 'ctl00_ContentPlaceHolder1_RelacionPA'}).text
        return PA

    def run(self):
        while True:
            sug = read_sug()
            if sug is None:
                break
            pa = self.find(sug)

            write_sug({
                'id': sug['id'],
                'value': sug['value'],
                'pactivo': pa
            })
            self.log(sug["id"] + " done.")

        self.log("Finish")


def main(file_in, file_out, nro_threads=15):
    global FREAD, FWRITE, pg_cnt
    FREAD = open(file_in)
    FWRITE = open(file_out, "w")
    pg_cnt = 0

    logging.info('Working with ' + str(nro_threads) + ' threads.')

    ths = []
    for i in range(nro_threads):
        st = ScraperThread(i)
        t = threading.Thread(target=st.run)
        ths.append(t)
        t.start()

    for th in ths:
        th.join()

    logging.info('All threads terminated.')

    FREAD.close()
    FWRITE.close()
    FREAD, FWRITE = None, None


if __name__ == "__main__":
    logging.basicConfig(filename='ZonngoCore.log', level=logging.DEBUG)
    main('data/suggestions.json', 'data/pactivo.json', 40)
