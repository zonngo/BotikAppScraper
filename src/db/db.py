import atexit

import mysql.connector
import pymysql
from mysql.connector import errorcode

from config import ConfigManager


class DbConn:
    def __init__(self, credentials):
        self.credentials = credentials
        self.cnx = None
        atexit.register(self.close)

    def connect(self):
        self.cnx = None
        try:
            # self.credentials['raise_on_warnings'] = True
            self.cnx = mysql.connector.connect(**self.credentials)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def commit(self):
        if self.cnx:
            self.cnx.commit()

    def stop(self):
        if self.cnx:
            self.close()
            self.cnx = None

    def cursor(self):
        if self.cnx is None:
            self.connect()
        return self.cnx.cursor()

    def close(self):
        if self.cnx:
            self.cnx.close()
            self.cnx = None


def get_conn_basicos():
    conf = ConfigManager.get_config('db', "BASICOS")
    conn = pymysql.connect(host=conf['host'],
                           user=conf['user'], passwd=conf['password'], db=conf['database'],
                           charset='utf8')
    return conn


def query(s, array=None):
    try:
        dbConn = get_conn_basicos()
        cursor = dbConn.cursor()
        cursor.execute(s, array)
        res = []
        for x in cursor.fetchall():
            res.append([xx for xx in x])
        dbConn.commit()
        cursor.close()
        return res
    except Exception as e:
        return [[e]]


medicine_conn = DbConn(ConfigManager.get_config("db", "MEDICINE"))
comparator_conn = DbConn(ConfigManager.get_config("db", "COMPARATOR"))
# basicos_conn = DbConn(ConfigManager.get_config('db', "BASICOS"))

if __name__ == '__main__':
    print(query('select 7;'))
