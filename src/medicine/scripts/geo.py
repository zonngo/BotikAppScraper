from db.MedicineModels import FarmaciaModel


def save_tb(tps, mmd, camps):
    set_tp = set()
    for tp in mmd.get_all(cols=mmd.primary_key):
        set_tp.add("@".join([str(tp[x]) for x in mmd.primary_key]))
    saves = []
    updates = []
    for tp in tps:
        ID = "@".join([str(tp[x]) for x in mmd.primary_key])
        if ID not in set_tp:
            saves.append(tp)
        else:
            updates.append(tp)
    print("------------------------len(saves) = ", len(saves))
    print("------------------------len(updat) = ", len(updates))
    # mmd.save(saves)
    mmd.update(camps, updates)


ans = []
first = 1
with open('medicine/scripts/geo.txt', 'r') as arch:
    for reg in arch:
        if first:
            first = 0
            continue
        reg = reg.replace('\n', '').split('\t')
        obj = {
            "id": int(reg[0]),
            "estado": 3,
            "lat": float(reg[1]),
            "lng": float(reg[2])
        }
        ans.append(obj)

fm = FarmaciaModel()
save_tb(ans, fm, ["id", "estado", "lat", "lng"])
