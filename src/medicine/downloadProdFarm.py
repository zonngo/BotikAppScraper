import json
import logging
import os
import threading

import requests

URL = "http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Consulta/data.aspx"

FREAD, FWRITE, FCOMP = None, None, None
lock_read, lock_write, lock_comp = threading.Lock(), threading.Lock(), threading.Lock()
completeds = set()


def load_completeds(file_in):
    if os.path.exists(file_in):
        logging.info('Loading tasks completed previously.')
        with open(file_in) as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue
                completeds.add(line)
        logging.info('Done loading tasks completed')


def read_suggestion():
    ans = None
    lock_read.acquire()
    try:
        while True:
            line = FREAD.readline()
            ans = json.loads(line)
            key = ans['id'] + '@' + ans['value']
            if key not in completeds:
                completeds.add(key)
                break
    except IOError as e:
        print(e)
    except Exception as e:
        print(e)
    lock_read.release()
    logging.info(str(len(completeds)) + ' done or running.')
    return ans


def write_to_file(data):
    lock_write.acquire()
    if type(data) != list:
        data = [data]
    if len(data) > 0:
        FWRITE.write("\n".join([json.dumps(x, ensure_ascii=False) for x in data]) + '\n')
    lock_write.release()


def write_completed(line):
    lock_comp.acquire()
    FCOMP.write(line + '\n')
    FCOMP.flush()
    lock_comp.release()


class ScraperThread:
    def __init__(self, id_thread):
        self.id_thread = id_thread
        self.params = {"tipo_busqueda": 3, "relacionado": 0, "ubigeo": "", "_": 1469989862823, "iDisplayStart": 0,
                       "iDisplayLength": 100000, "sSearch_1": "TODOS", "sSearch_4": "", "sSearch_6": ""}

    def log(self, message):
        logging.debug("Thread " + str(self.id_thread) + " -> " + message)

    def find(self, dat):
        self.log("Starting " + dat["id"])
        self.params["grupo"] = dat["grupo"]
        self.params["totalPA"] = dat["total_pa"]
        self.params["con"] = dat["con"]
        self.params["ffs"] = dat["ffs"]
        self.params["cad"] = dat["value"].replace(" ", "*")

        ans = []

        try_number = 0
        json_obj = {"aaData": []}
        while try_number < 5:
            try:
                xd = requests.get(URL, params=self.params)
                json_obj = xd.json()
                break
            except Exception as e:
                try_number += 1
                self.log("Intento: " + str(try_number) + " Error " + str(e))

        self.log("For this suggestion: " + str(len(json_obj["aaData"])))
        for data in json_obj["aaData"]:
            prod = {"id_p": data[0], "t_est": 1 if data[1] == "PRIVADO" else 2, "f_act": data[2], "id_f": data[5],
                    "pr": data[7], 'lab': data[4], 'idsug': dat['id'], 'cad': dat['value']}
            ans.append(prod)

        return ans

    def run(self):
        while True:
            sug = read_suggestion()
            if sug is None:
                break
            write_to_file(self.find(sug))
            write_completed(sug['id'] + '@' + sug['value'])
            self.log(sug["id"] + " done.")

        self.log("Finish")


def download(nro_threads, in_file, out_file):
    global FREAD, FWRITE, FCOMP
    nro_threads = max(1, nro_threads)

    COMP_FILE = out_file + '.done'
    load_completeds(COMP_FILE)
    FCOMP = open(COMP_FILE, 'a')

    logging.info("Opening suggestions file ..")
    FREAD = open(in_file)

    logging.info("Opening output file..")
    FWRITE = open(out_file, "a")
    logging.debug("Working whit " + str(nro_threads) + " threads.")

    ths = []
    for i in range(nro_threads):
        st = ScraperThread(i)
        t = threading.Thread(target=st.run)
        ths.append(t)

    logging.debug("Starting Threads")
    for th in ths:
        th.start()

    for th in ths:
        th.join()

    logging.debug("Al threads finished.")

    logging.debug("Closing input output files...")
    FREAD.close()
    FWRITE.close()
    FCOMP.close()
    FREAD, FWRITE, FCOMP = None, None, None
    os.remove(COMP_FILE)

    logging.info("Work finished.")


if __name__ == "__main__":
    logging.basicConfig(filename='log.txt', level=logging.DEBUG)
    download(10, 'data/suggestions.json', 'data/data2.json')
