import codecs


class Model:
    def __init__(self, conn):
        self.conn = conn
        self.table_name = ""
        self.primary_key = []
        self.attributes = []
        self.columns = []

        self._table_name = ""
        self._primary_key = []
        self._attributes = []
        self._columns = []
        self.lastrowid = None

    def set_table_name(self, table_name):
        self.table_name = table_name
        self._table_name = "`" + table_name + "`"

    def set_primary_key(self, primary_key):
        if type(primary_key) == str:
            primary_key = [primary_key]
        elif type(primary_key) != list or primary_key is None:
            raise Exception("Argument should be a str or list instance")
        self.primary_key = primary_key
        self._primary_key.clear()
        for pk in primary_key:
            self._primary_key.append("`" + pk + "`")

        self.columns = self.primary_key + self.attributes
        self._columns = self._primary_key + self._attributes

    def set_attributes(self, attributes):
        if type(attributes) == str:
            attributes = [attributes]
        elif type(attributes) != list or attributes is None:
            raise Exception("Argument should be a str or list instance")
        self.attributes = attributes
        self._attributes.clear()
        for at in attributes:
            self._attributes.append("`" + at + "`")
        self.columns = self.primary_key + self.attributes
        self._columns = self._primary_key + self._attributes

    def valid_columns(self, cols):
        if type(cols) != list and type(cols) != dict:
            raise Exception("The argument should be a dict or list instance")
        for col in cols:
            if col not in self.columns:
                return False
        return True

    def extract(self, cursor, cols=None):
        if cols is None:
            cols = self.columns
        rows = []
        for x in cursor:
            row = {}
            ci = 0
            for col in cols:
                if type(x[ci]) == bytearray:
                    row[col] = codecs.decode(x[ci], "utf8")
                else:
                    row[col] = x[ci]
                ci += 1
            rows.append(row)
        cursor.close()
        self.conn.close()
        return rows

    def find(self, cod, cols=None):
        if type(cod) != dict:
            raise Exception("The cod parameter should be a dict instace.")
        if cols is not None and type(cols) != list:
            raise Exception("The cols parameter should be a list instance.")

        if cols is None:
            query = "select " + ",".join(self._columns)
        else:
            if not self.valid_columns(cols):
                raise Exception("Colums aren't valid")
            query = "select " + ",".join(cols)
        query += " from " + self._table_name + " where "

        if len(self.primary_key) == 1:
            query += self._primary_key[0] + " = %(" + self.primary_key[0] + ")s"
        else:
            for i in range(0, len(self.primary_key)):
                if i > 0:
                    query += " and "
                query += self._primary_key[i] + " = %(" + self.primary_key[i] + ")s"

        cursor = self.conn.cursor()
        cursor.execute(query, cod)
        ans = self.extract(cursor, cols)
        if len(ans) == 0:
            ans = None
        else:
            ans = ans[0]
        self.conn.close()
        return ans

    def where(self, vals, cols=None):
        if type(vals) is not dict or len(vals.keys()) == 0:
            raise Exception("Invalid restriccions")
        if cols is not None and (type(cols) != list or not self.valid_columns(cols)):
            raise Exception("Invalid columns")

        if cols is None:
            query = "select " + ", ".join(self._columns) + " from " + self._table_name
        else:
            query = "select " + ", ".join(cols) + " from " + self._table_name

        query += " where "
        ands = False
        for key in vals.keys():
            if ands:
                query += " and "
            ands = True
            query += key + " = %(" + key + ")s"

        cursor = self.conn.cursor()
        cursor.execute(query, vals)
        return self.extract(cursor, cols)

    def get_all(self, cols=None):
        if cols is not None and (type(cols) != list or not self.valid_columns(cols)):
            raise Exception("Invalid cols")

        if cols is None:
            query = "select " + ", ".join(self._columns) + " from " + self._table_name
        else:
            query = "select " + ", ".join(cols) + " from " + self._table_name
        cursor = self.conn.cursor()
        cursor.execute(query)
        return self.extract(cursor, cols)

    def save(self, rows, ignore_errors=False):
        # def save(self, rows):
        if type(rows) == dict:
            rows = [rows]
        elif type(rows) == list:
            for row in rows:
                if type(row) != dict:
                    raise Exception("Row should be a dict")
        else:
            raise Exception("Parameter invalid")

        query = "insert into " + self._table_name + "(" + ", ".join(self._columns) + ") values ("
        query += ", ".join(["%(" + x + ")s" for x in self.columns]) + ")"

        cursor = self.conn.cursor()
        for row in rows:
            if len(row.keys()) < len(self.columns):
                for col in self.columns:
                    if col not in row.keys():
                        row[col] = None
            try:
                cursor.execute(query, row)
            except Exception as e:
                if not ignore_errors:
                    raise e
        self.lastrowid = cursor.lastrowid
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def update(self, cols, rows):
        if type(cols) is not list or not self.valid_columns(cols):
            raise Exception("Invalid Columns")

        query = "update " + self._table_name + " set"
        first = True
        for col in cols:
            if first:
                query += " "
                first = False
            else:
                query += ", "
            query += "`" + col + "` = %(" + col + ")s"
        query += " where "
        for i in range(0, len(self.primary_key)):
            if i > 0:
                query += " and "
            query += self._primary_key[i] + " = %(" + self.primary_key[i] + ")s"

        cursor = self.conn.cursor()
        for row in rows:
            cursor.execute(query, row)
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def delete(self, ids):
        if type(ids) == dict:
            ids = [ids]
        elif type(ids) != list:
            raise Exception('Ids should be dictionary or list of dictionaries')

        query = "delete from " + self._table_name + ' where '
        if len(self.primary_key) == 1:
            query += self._primary_key[0] + " = %(" + self.primary_key[0] + ")s"
        else:
            for i in range(0, len(self.primary_key)):
                if i > 0:
                    query += " and "
                query += self._primary_key[i] + " = %(" + self.primary_key[i] + ")s"

        cursor = self.conn.cursor()
        for cid in ids:
            cursor.execute(query, cid)
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def save2(self, rows, cols=None):
        if type(rows) != list or type(rows[0]) != tuple:
            raise Exception("Rows should be a list of tuples.")
        if cols and type(cols) != list:
            raise Exception("Cols should be a list ol columns name.")
        if cols and not self.valid_columns(cols):
            raise Exception("Columns are not valid")

        cursor = self.conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")

        query = "insert into " + self._table_name
        if cols:
            query += "(" + ", ".join(['`' + x + '`' for x in cols]) + ") values ("
            query += ", ".join(["%s"] * len(cols)) + ")"
        else:
            query += "(" + ", ".join(self._columns) + ") values ("
            query += ", ".join(["%s"] * len(self._columns)) + ")"

        for row in rows:
            try:
                cursor.execute(query, row)
            except Exception as e:
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                cursor.close()
                raise e
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def truncate(self):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM " + self._table_name)
        cursor.close()
        self.conn.commit()
        self.conn.close()

    def querym(self, q):
        cursor = self.conn.cursor()
        for result in cursor.execute(q, multi=True):
            if result.with_rows:
                print("Rows produced by statement '{}':".format(result.statement))
                print(result.fetchall())
            else:
                print("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
        self.conn.commit()
        cursor.close()
        self.conn.close()
