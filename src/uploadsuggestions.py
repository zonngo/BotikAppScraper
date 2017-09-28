import json

from db.MedicineModels import Sugestions

PATH = 'data/suggestions.json'


def main():
    sm = Sugestions()

    rows = []
    for line in open(PATH):
        row = json.loads(line)
        rows.append(row)
    sm.save(rows)


if __name__ == "__main__":
    main()
