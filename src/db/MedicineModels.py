from db import db
from db import model


class LaboratorioModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("LABORATORIO")
        self.set_primary_key("id")
        self.set_attributes(["nombre"])


class ProductoModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("PRODUCTO")
        self.set_primary_key("id")
        self.set_attributes(["nombre", "concent", "tp_pre", "tp_pre_si", "reg_san", "laboratorio", "presentacion",
                             "fracciones", "estado", "condicionV", "tipo", "fabricante", "Situacion", "sug", "pactivo"])


class RegSanModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("REG_SAN")
        self.set_primary_key("id")
        self.set_attributes(["codigo", "vencimiento"])


class TipoPresentacionModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("TIPO_PRESENTACION")
        self.set_primary_key("id")
        self.set_attributes(["descripcion"])


class PresentacionModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("PRESENTACION")
        self.set_primary_key("id")
        self.set_attributes(["descripcion"])


class FarmaciaModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("FARMACIA")
        self.set_primary_key("id")
        self.set_attributes(["estado", "tipo_est", "nombre", "ubigeo", "provincia", "telefono", "horario",
                             "dtecnico", "direccion", "categoria", "razonSocial", "ruc", "situacion",
                             "distrito", "departamento", "lat", "lng"])


class PersonalModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("PERSONAL")
        self.set_primary_key(["nombre", "idF", "tipo"])
        self.set_attributes(["dni", "cargo", "horario"])


class TipoPersonalModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("TIPO_PERSONAL")
        self.set_primary_key("id")
        self.set_attributes(["descripcion"])


class ProdFarmModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("PROD_FARM")
        self.set_primary_key(["idP", "idF"])
        self.set_attributes(["fecha_act", "precio"])


class Ubigeo(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("UBIGEO")
        self.set_primary_key(["id"])
        self.set_attributes(["name", "tipo", "parent"])


class Sugestions(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("SUGGESTIONS")
        self.set_primary_key(["id"])
        self.set_attributes(["value", "type", "grupo", "total_pa", "con", "ffs"])


class ProdSugModel(model.Model):
    def __init__(self, conn=db.medicine_conn):
        super().__init__(conn)
        self.set_table_name("PROD_SUG")
        self.set_primary_key(["idp", "ids"])
        # self.set_attributes([])
