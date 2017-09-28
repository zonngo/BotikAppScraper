import json
import logging

import requests

URL1 = 'http://observatorio.digemid.minsa.gob.pe'
URL2 = "http://observatorio.digemid.minsa.gob.pe/Default.aspx/GetMedicamentos"

session = None


def find(term):
    logging.debug("term:" + term)

    body_post = {"term": term}
    xd = session.post(URL2, data=json.dumps(body_post))
    if xd.status_code != 200:
        logging.warning('The server respond with code status different of 200.')
        return []

    json_obj = xd.json()
    ans = []
    for dat in json_obj["d"]:
        id_med = dat["id"].split("@")
        med = {"id": dat["id"], "value": dat["value"], "grupo": int(id_med[0]),
               "total_pa": int(id_med[1]), "con": id_med[2], "ffs": int(id_med[3])}
        ans.append(med)

    logging.debug("{a} in {t}".format(a=len(ans), t=term))
    return ans


def download(out_file):
    global session
    session = requests.Session()
    session.get(URL1)
    session.headers.update({"Content-Type": "application/json; charset=UTF-8"})

    data = []
    for i in range(10):
        data.extend(find(str(i)))

    for i in range(26):
        data.extend(find(chr(65 + i) + " "))
        data.extend(find(chr(65 + i) + "-"))
        for j in range(26):
            data.extend(find(chr(65 + i) + chr(65 + j)))

    with open(out_file, 'w') as arch:
        arch.write("\n".join([json.dumps(x, ensure_ascii=False) for x in data]) + '\n')

    logging.debug("Total: " + str(len(data)))
    return True


if __name__ == "__main__":
    logging.basicConfig(filename='log.txt', level=logging.DEBUG)
    download('../data/data1.json')
