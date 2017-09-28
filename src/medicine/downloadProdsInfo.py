import json
import logging
import queue
import threading

import requests
from bs4 import BeautifulSoup

URL = "http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/FichaProducto.aspx?"


def complete(cod):
    while len(cod) < 7:
        cod = '0' + cod
    return cod


def save_products(data, out_file):
    for dat in data:
        dat["estado"] = "1"

    with open(out_file, "w") as f:
        f.write('\n'.join([json.dumps(x, ensure_ascii=False) for x in data]) + '\n')


class PrinterThread(threading.Thread):
    def __init__(self, out_queue, out_array):
        super().__init__()
        self.out_array = out_array
        self.out_queue = out_queue
        self.setDaemon(True)

    def write(self, data):
        if type(data) is not list:
            data = [data]
        self.out_array.extend(data)

    def run(self):
        processed_count = 0
        while True:
            data = self.out_queue.get()
            if data is None:
                break
            self.write(data)
            self.out_queue.task_done()
            processed_count += 1
            logging.debug("Printer Thread -> " + str(processed_count) + " done.")


class ScraperThread:
    def __init__(self, id_thread, rows):
        self.id_thread = id_thread
        self.props = ["Medicamento", "Presentacion", "Pais", "Registro", "CondicionV", "Marca", "Titular", "Fabricante"]
        self.rows = rows

    def log(self, message):
        logging.debug("Thread " + str(self.id_thread) + " -> " + message)

    def scrap_prods(self, id_p, id_f):
        ans = None
        try:
            url_q = URL + "idp=" + id_p + "&ide=" + id_f
            req = requests.get(url_q)
            bs_obj = BeautifulSoup(req.text, "html.parser")

            ans = {"idP": id_p, "idF": id_f}
            for prop in self.props:
                ans[prop] = bs_obj.find("span", {"id": prop}).text
        except Exception as e:
            logging.warning(str(e))
        return ans

    def run(self, begin, out_queue):
        self.log("Starting from " + str(begin))
        for row in self.rows:
            tmp = self.scrap_prods(str(row[0]), str(row[1]))
            try_count = 0
            while tmp is None and try_count < 10:
                try_count += 1
                self.log("Try " + str(try_count) + " failed.")
                tmp = self.scrap_prods(str(row[0]), complete(str(row[1])))
            out_queue.put(tmp)
            self.log(str(row[0]) + ", " + str(row[1]) + " done.")
        self.log("Finish")


def download(nro_threads, in_file, out_file):
    set_prods = set()
    rows = []
    with open(in_file) as f:
        for line in f:
            ob = json.loads(line)
            if ob['id_p'] in set_prods:
                continue
            rows.append([ob['id_p'], ob['id_f']])
            set_prods.add(ob['id_p'])

    logging.debug("Working whit " + str(nro_threads) + " threads.")
    logging.debug("Computing task for each thread...")

    suggestions_pt = max(int(len(rows) / nro_threads), 1)

    ths = []
    out_queue = queue.Queue()

    idt = 0
    for i in range(0, len(rows), suggestions_pt):
        st = ScraperThread(idt, rows[i:i + suggestions_pt])
        idt += 1
        t = threading.Thread(target=st.run, args=(i, out_queue))
        ths.append(t)

    logging.debug("Starting Thread")
    for th in ths:
        th.start()

    data = []
    logging.debug("Starting Printer Thread")
    pt = PrinterThread(out_queue, data)
    pt.start()

    for th in ths:
        th.join()

    logging.debug("Al threads finished.")
    logging.debug("Closing Printer Thread...")
    out_queue.put(None)
    pt.join()
    logging.debug("Scrap finished")

    logging.debug("Starting upload to database.")
    save_products(data, out_file)
    logging.debug("Work finished.")


if __name__ == '__main__':
    print('scrap4 ...')
    download(10, '../data/prod_farm.json', '../data/prods_info.json')
