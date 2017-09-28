import json


def run():
    sug = {}
    archi = open('data/pactivo.json', 'r')
    for data in archi:
        data = json.loads(data)
        if data['id'] in sug:
            continue
        sug[data['id']] = data['pactivo']
    archi.close()
    archi = open('data/prod_farm.json', 'r')
    arch = open('data/prod_pactivo.json', 'w')
    for data in archi:
        data = json.loads(data)
        if data['idsug'] in sug:
            arch.write(json.dumps({'id_p': data['id_p'], 'pa': sug[data['idsug']]}, ensure_ascii=False))
            arch.write('\n')
        else:
            arch.write(json.dumps({'id_p': data['id_p'], 'pa': ''}, ensure_ascii=False))
            arch.write('\n')
    arch.close()
    archi.close()


if __name__ == '__main__':
    run()
