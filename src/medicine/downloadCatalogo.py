import csv
import logging
import os

import requests
from bs4 import BeautifulSoup

URL1 = 'http://observatorio.digemid.minsa.gob.pe'
URL2 = "http://observatorio.digemid.minsa.gob.pe/Precios/ProcesoL/Catalogo/CatalogoProductos.aspx"


def download(out_file):
    tmp_file = ".tmp.xls"

    session = requests.Session()
    session.get(URL1)
    session.headers.update({"Content-Type": "application/json; charset=UTF-8"})

    html_doc = BeautifulSoup(requests.get(URL2).text, 'html.parser')
    inputs = html_doc.find_all('input')
    params = {
        "__VIEWSTATE": inputs[0].get('value'),
        "__VIEWSTATEGENERATOR": inputs[1].get('value'),
        "__EVENTVALIDATION": inputs[2].get('value'),
        "ctl00$ContentPlaceHolder1$ctl00.x": 23,
        "ctl00$ContentPlaceHolder1$ctl00.y": 10
    }

    res = requests.post(URL2, params, stream=True)
    if res.status_code == 200:
        logging.debug("Downloading.")
        with open(tmp_file, 'wb') as f:
            for chunk in res:
                if chunk:
                    f.write(chunk)
        logging.debug("Downloaded.")
    else:
        return False

    with open(tmp_file) as f:
        xd = f.read()

    os.remove(tmp_file)
    lines = []
    bg = 0
    for i in range(len(xd)):
        if xd[i:i + 3] == "<tr":
            bg = i
        if xd[i:i + 4] == "</tr":
            lines.append(xd[bg:i + 5])

    data = []
    st = set()
    for line in lines:
        row = []
        id_bg = False
        for i in range(len(line)):
            if line[i:i + 3] == "<td":
                id_bg = True
            if id_bg and line[i] == ">":
                bg = i + 1
            if line[i:i + 4] == "</td":
                row.append(line[bg:i])
                id_bg = False
        if len(row) != 11:
            logging.error("Problem in the row: " + repr(row))
            continue
        if row[0] not in st:
            data.append(row)
            st.add(row[0])

    with open(out_file, "w") as mf:
        cw = csv.writer(mf, delimiter=";")
        for dat in data:
            cw.writerow(dat)

    logging.info('Process done.')
    return True


if __name__ == '__main__':
    logging.basicConfig(filename='log.txt', level=logging.DEBUG)
    download('../data/products.csv')
