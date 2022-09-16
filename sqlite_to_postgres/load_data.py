import os
import sqlite3
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_extractor import SQLiteExtractor, conn_context
from psql_saver import PostgresSaver

# Loading environment variables
load_dotenv()

db_path = os.environ.get('SQLITE_DB_PATH')
dbname = os.environ.get('DB_NAME')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')
host = os.environ.get('HOST')
port = os.environ.get('DB_PORT')


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    movie_data = sqlite_extractor.extract_movies()
    postgres_saver.save_movies(movie_data)

    person_data = sqlite_extractor.extract_staff()
    postgres_saver.save_staff(person_data)

    genre_data = sqlite_extractor.extract_genres()
    postgres_saver.save_genres(genre_data)

    movies_staff_data = sqlite_extractor.extract_movies_staff()
    postgres_saver.save_movies_staff(movies_staff_data)

    movies_genres_data = sqlite_extractor.extract_genres_movies()
    postgres_saver.save_movies_genres(movies_genres_data)


if __name__ == '__main__':
    dsl = {'dbname': dbname,
           'user': user,
           'password': password,
           'host': host,
           'port': port}
    with conn_context(db_path) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
