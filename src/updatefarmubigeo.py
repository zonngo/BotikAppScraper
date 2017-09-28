from db.MedicineModels import Ubigeo


def f(idu):
    if idu < 100000:
        return '0' + str(idu)
    return str(idu)


def main():
    ubm = Ubigeo()

    q = ''
    for dep in ubm.where({'tipo': 1}):
        for prov in ubm.where({'parent': dep['id']}):
            for dist in ubm.where({'parent': prov['id']}):
                a = "update FARMACIA set ubigeo = '{v0}' where departamento = '{v1}' and provincia = '{v2}' and distrito = '{v3}' and ubigeo is null;\n"
                a = a.format(v0=f(dist['id']), v1=dep['name'], v2=prov['name'], v3=dist['name'])
                q += a

    print('Sending query')
    # print(q)
    ubm.querym(q)
    print('Updated')

    q = """update FARMACIA set ubigeo = '30602' where provincia = 'CHINCHEROS' and distrito like 'ANCO%HUALLO';
update FARMACIA set ubigeo = '120303' where provincia = 'CHANCHAMAYO' and distrito like 'PICHANAQUI';
update FARMACIA set ubigeo = '150132' where provincia = 'LIMA' and distrito like 'LURIGANCHO';
update FARMACIA set ubigeo = '150121' where provincia = 'LIMA' and distrito like 'PUEBLO LIBRE';
update FARMACIA set ubigeo = '230110' where provincia = 'TACNA' and distrito like 'CORONEL GREGORIO ALBARRACIN LA';"""
    ubm.querym(q)


if __name__ == "__main__":
    main()
