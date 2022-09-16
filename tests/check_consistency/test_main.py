import pytest
import psycopg2
from psycopg2.extras import RealDictConnection

from config import conn_context, dsl


@pytest.fixture
def number_of_records_from_psql():
    """Получаем список с количеством строк в каждой таблице из Postgres"""
    ammount = []
    with psycopg2.connect(**dsl) as pg_conn:
        curs = pg_conn.cursor()
        curs.execute("SELECT count(*) FROM film_work;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM genre;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM genre_film_work;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM person;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM person_film_work;")
        ammount.append(curs.fetchone()[0])
    return ammount


@pytest.fixture
def number_of_records_from_sqlite():
    """Получаем список с количеством строк в каждой таблице из SQLite"""
    ammount = []
    with conn_context() as sqlite_conn:
        curs = sqlite_conn.cursor()
        curs.execute("SELECT count(*) FROM film_work;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM genre;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM genre_film_work;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM person;")
        ammount.append(curs.fetchone()[0])
        curs.execute("SELECT count(*) FROM person_film_work;")
        ammount.append(curs.fetchone()[0])
    return ammount


@pytest.fixture
def get_query():
    query_dict = {
        'film_work': "SELECT * FROM film_work;",
        'genre': "SELECT * FROM genre;",
        'person': "SELECT * FROM person;",
        'genre_film_work': "SELECT * FROM genre_film_work;",
        'person_film_work': "SELECT * FROM person_film_work;"
    }
    return query_dict


def table_from_sqlite(query):
    """
    Получаем данные из таблиц из SQLite, конкретная таблица передается в query
    """
    with conn_context() as sqlite_conn:
        curs = sqlite_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
    return data


def table_from_psql(query):
    """
    Получаем данные из таблиц из Postgres, конкретная таблица передается в
    query
    """
    with psycopg2.connect(**dsl,
                          connection_factory=RealDictConnection) as pg_conn:
        curs = pg_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
    return data


def test_data_integrity(number_of_records_from_psql,
                        number_of_records_from_sqlite):
    """
    Проверка целостности данных между каждой парой таблиц в SQLite и Postgres.
    Проверяем количество записей в каждой таблице
    """
    records_from_psql = number_of_records_from_psql
    records_from_sqlite = number_of_records_from_sqlite

    assert records_from_psql[0] == records_from_sqlite[0]
    assert records_from_psql[1] == records_from_sqlite[1]
    assert records_from_psql[2] == records_from_sqlite[2]
    assert records_from_psql[3] == records_from_sqlite[3]
    assert records_from_psql[4] == records_from_sqlite[4]


def test_field_values_film_work(get_query):
    """
    Проверка содержимого записей внутри таблицы film_work
    Проверяется, что все записи из PostgreSQL присутствуют с
    такими же значениями полей, как и в SQLite.
    """
    query = get_query['film_work']

    for film_sqlite, film_psql in zip(table_from_sqlite(query),
                                      table_from_psql(query)):
        film_sqlite = dict(film_sqlite)
        assert film_sqlite['id'] == film_psql['id']
        assert film_sqlite['title'] == film_psql['title']
        assert film_sqlite['rating'] == film_psql['rating']
        assert film_sqlite['type'] == film_psql['type']
        assert film_sqlite['creation_date'] == film_psql['creation_date']


def test_field_values_genre(get_query):
    """
    Проверка содержимого записей внутри таблицы genre
    Проверяется, что все записи из PostgreSQL присутствуют с
    такими же значениями полей, как и в SQLite.
    """
    query = get_query['genre']

    for genre_sqlite, genre_psql in zip(table_from_sqlite(query),
                                        table_from_psql(query)):
        genre_sqlite = dict(genre_sqlite)
        assert genre_sqlite['id'] == genre_psql['id']
        assert genre_sqlite['name'] == genre_psql['name']
        assert genre_sqlite['description'] == genre_psql['description']


def test_field_values_person(get_query):
    """
    Проверка содержимого записей внутри таблицы person
    Проверяется, что все записи из PostgreSQL присутствуют с
    такими же значениями полей, как и в SQLite.
    """
    query = get_query['person']

    for person_sqlite, person_psql in zip(table_from_sqlite(query),
                                          table_from_psql(query)):
        person_sqlite = dict(person_sqlite)
        assert person_sqlite['id'] == person_psql['id']
        assert person_sqlite['full_name'] == person_psql['full_name']


def test_field_values_person_film_work(get_query):
    """
    Проверка содержимого записей внутри таблицы person_film_work
    Проверяется, что все записи из PostgreSQL присутствуют с
    такими же значениями полей, как и в SQLite.
    """
    query = get_query['person_film_work']

    for person_film_sqlite, person_film_ps in zip(table_from_sqlite(query),
                                                  table_from_psql(query)):
        person_film_s = dict(person_film_sqlite)
        assert person_film_s['id'] == person_film_ps['id']
        assert person_film_s['film_work_id'] == person_film_ps['film_work_id']
        assert person_film_s['person_id'] == person_film_ps['person_id']
        assert person_film_s['role'] == person_film_ps['role']


def test_field_values_genre_film_work(get_query):
    """
    Проверка содержимого записей внутри таблицы genre_film_work
    Проверяется, что все записи из PostgreSQL присутствуют с
    такими же значениями полей, как и в SQLite.
    """
    query = get_query['genre_film_work']

    for genre_film_sqlite, genre_film_ps in zip(table_from_sqlite(query),
                                                table_from_psql(query)):
        genre_film_s = dict(genre_film_sqlite)
        assert genre_film_s['id'] == genre_film_ps['id']
        assert genre_film_s['genre_id'] == genre_film_ps['genre_id']
        assert genre_film_s['film_work_id'] == genre_film_ps['film_work_id']
