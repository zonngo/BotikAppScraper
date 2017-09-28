import json
import logging
import queue
import threading

import requests
from bs4 import BeautifulSoup

URL = "http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/FichaProducto.aspx?idp=24018&"


def get_farms(in_file):
    my_set = set()
    with open(in_file) as f:
        for line in f:
            id_f = int(json.loads(line)['id_f'])
            my_set.add(id_f)
    return list(my_set)


def save_farms(data, out_file):
    for dat in data:
        dat["estado"] = "1"
        dat["provincia"] = dat["ubicacion"]
        del dat["ubicacion"]

    with open(out_file, "w") as f:
        f.write('\n'.join([json.dumps(x, ensure_ascii=False) for x in data]) + '\n')


class PrinterThread(threading.Thread):
    def __init__(self, out_queue, array):
        super().__init__()
        self.out_array = array
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
            logging.debug("Printer Thread -> " + str(processed_count) + " procesados.")


class ScraperThread:
    def __init__(self, id_thread, farms):
        self.id_thread = id_thread
        self.props = ["Direccion", "Ubicacion", "Telefono", "Horario", "DTecnico"]
        self.farms = farms

    def log(self, mess):
        logging.debug("Thread " + str(self.id_thread) + " -> " + mess)

    def scrap_farm(self, id_farm):
        ans = None
        try:
            res = requests.get(URL + "ide=" + str(id_farm))
            bs_obj = BeautifulSoup(res.text, "html.parser")

            ans = {"id": id_farm}
            for prop in self.props:
                ans[prop.lower()] = bs_obj.find("span", {"id": prop}).text
        except Exception as e:
            logging.warning(e)
        return ans

    def run(self, begin, out_queue):
        self.log("Starting from " + str(begin))
        for sug in self.farms:
            tmp = self.scrap_farm(sug)
            try_number = 0
            while tmp is None and try_number < 10:
                try_number += 1
                self.log("Try " + str(try_number) + " error.")
                tmp = self.scrap_farm(sug)
            out_queue.put(tmp)
            self.log(str(sug) + " done.")
        self.log("Finish")


def download(nro_threads, in_file, out_file):
    farms = get_farms(in_file)

    logging.debug("Working whit " + str(nro_threads) + " threads.")
    logging.debug("Computing task for each thread...")

    suggestions_pt = max(int(len(farms) / nro_threads), 1)
    ths = []
    out_queue = queue.Queue()

    idt = 0
    for i in range(0, len(farms), suggestions_pt):
        st = ScraperThread(idt, farms[i:i + suggestions_pt])
        idt += 1
        t = threading.Thread(target=st.run, args=(i, out_queue))
        ths.append(t)

    logging.debug("Starting Threads")
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
    save_farms(data, out_file)
    logging.debug("Work finished.")


if __name__ == "__main__":
    print('scrap3 ...')
    download(20, 'data/farms.list', 'data/farms.json')
