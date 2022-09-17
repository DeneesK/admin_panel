import sqlite3
from sqlite3 import DatabaseError
from contextlib import contextmanager

from logger import logger


class SQLiteExtractor:
    def __init__(self, conn):
        self.conn = conn

    def extract_data(self, table_name, arraysize=100):
        curs = self.conn.cursor()
        query = "SELECT * FROM {0}".format(table_name)
        try:
            curs.execute(query)
            while True:
                data = curs.fetchmany(size=arraysize)
                if not data:
                    break
                yield data
        except DatabaseError as ex:
            logger.error(ex)


@contextmanager
def conn_context(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
