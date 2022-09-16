import os
import sqlite3
from dotenv import load_dotenv
from contextlib import contextmanager


# Loading environment variables
load_dotenv()

db_path = os.environ.get('SQLITE_DB_PATH')
dbname = os.environ.get('DB_NAME')
user = os.environ.get('USER_NAME')
password = os.environ.get('PASSWORD')
host = os.environ.get('HOST')
port = os.environ.get('DB_PORT')

dsl = {'dbname': dbname,
       'user': user,
       'password': password,
       'host': host,
       'port': port}


@contextmanager
def conn_context(db_path=db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()
