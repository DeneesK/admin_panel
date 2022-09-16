import sqlite3
from sqlite3 import DatabaseError
from contextlib import contextmanager


class SQLiteExtractor:
    def __init__(self, conn):
        self.conn = conn

    def extract_movies(self):
        curs = self.conn.cursor()
        try:
            curs.execute("SELECT * FROM film_work;")
            data = curs.fetchall()
            return data
        except DatabaseError as ex:
            print(ex,
                  """
                  Error occured while trying to get data from film_work table,
                  db.sqlite
                  """)

    def extract_staff(self):
        curs = self.conn.cursor()
        try:
            curs.execute("SELECT * FROM person;")
            data = curs.fetchall()
            return data
        except DatabaseError as ex:
            print(ex,
                  """
                  Error occured while trying to get data from person table,
                  db.sqlite
                  """)

    def extract_genres(self):
        curs = self.conn.cursor()
        try:
            curs.execute("SELECT * FROM genre;")
            data = curs.fetchall()
            return data
        except DatabaseError as ex:
            print(ex,
                  """
                  Error occured while trying to get data from genre table,
                  db.sqlite
                  """)

    def extract_genres_movies(self):
        curs = self.conn.cursor()
        try:
            curs.execute("SELECT * FROM genre_film_work;")
            data = curs.fetchall()
            return data
        except DatabaseError as ex:
            print(ex,
                  """
                  Error occured while trying to get data from genre_film_work
                  table, db.sqlite
                  """)

    def extract_movies_staff(self):
        curs = self.conn.cursor()
        try:
            curs.execute("SELECT * FROM person_film_work;")
            data = curs.fetchall()
            return data
        except DatabaseError as ex:
            print(ex,
                  """
                  Error occured while trying to get data from person_film_work
                  table, db.sqlite
                  """)


@contextmanager
def conn_context(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
