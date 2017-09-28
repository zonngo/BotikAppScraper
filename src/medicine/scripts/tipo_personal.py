from db.MedicineModels import TipoPersonalModel

tpm = TipoPersonalModel()
tpm.save([{'id': 1, 'descripcion': 'Representante Legal'},
          {'id': 2, 'descripcion': 'Personal del Establecimiento'}])
