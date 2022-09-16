from to_psql_dataclasses import FilmWork, Person, Genre, PersonFilm, FilmGenre
from psycopg2.errors import DatabaseError


class PostgresSaver:
    def __init__(self, conn):
        self.conn = conn

    def film_work_to_postgres(self, film_work: FilmWork):
        curs = self.conn.cursor()
        curs.execute("""
                    INSERT INTO content.film_work
                    VALUES (%(id)s, %(title)s, %(description)s,
                    %(creation_date)s, %(rating)s, %(type)s, %(created_at)s,
                    %(updated_at)s, %(file_path)s);
                    """, film_work.__dict__)

    def person_to_postgres(self, person: Person):
        curs = self.conn.cursor()
        curs.execute("""
                     INSERT INTO content.person
                     VALUES (%(id)s, %(full_name)s, %(created_at)s,
                     %(updated_at)s)
                     """, person.__dict__)

    def genre_to_postgres(self, genre: Genre):
        curs = self.conn.cursor()
        curs.execute("""
                     INSERT INTO content.genre
                     VALUES (%(id)s, %(name)s, %(description)s, %(created_at)s,
                     %(updated_at)s)
                     """, genre.__dict__)

    def person_film_work_to_postgres(self, person_film_work: PersonFilm):
        curs = self.conn.cursor()
        curs.execute("""
                     INSERT INTO content.person_film_work
                     VALUES (%(id)s, %(film_work_id)s, %(person_id)s,
                     %(role)s, %(created_at)s)
                     """, person_film_work.__dict__)

    def genre_film_work_postgres(self, genre_film_work: FilmGenre):
        curs = self.conn.cursor()
        curs.execute("""
                     INSERT INTO content.genre_film_work
                     VALUES (%(id)s, %(genre_id)s, %(film_work_id)s,
                     %(created_at)s)
                     """, genre_film_work.__dict__)

    def save_movies(self, data):
        for row in data:
            film_work = FilmWork(**row)
            try:
                self.film_work_to_postgres(film_work)
            except DatabaseError as ex:
                print(ex,
                      """
                      Error occured while trying to wright data to film_work
                      table, postgres
                      """)

    def save_staff(self, data):
        for row in data:
            person = Person(**row)
            try:
                self.person_to_postgres(person)
            except DatabaseError as ex:
                print(ex,
                      """
                      Error occured while trying to wright data to person
                      table, postgres
                      """)

    def save_genres(self, data):
        for row in data:
            genre = Genre(**row)
            try:
                self.genre_to_postgres(genre)
            except DatabaseError as ex:
                print(ex,
                      """
                      Error occured while trying to wright data to genre
                      table, postgres
                      """)

    def save_movies_staff(self, data):
        for row in data:
            person_film_work = PersonFilm(**row)
            try:
                self.person_film_work_to_postgres(person_film_work)
            except DatabaseError as ex:
                print(ex,
                      """
                      Error occured while trying to wright data to
                      person_film_work table, postgres
                      """)

    def save_movies_genres(self, data):
        for row in data:
            genre_film_work = FilmGenre(**row)
            try:
                self.genre_film_work_postgres(genre_film_work)
            except DatabaseError as ex:
                print(ex,
                      """
                      Error occured while trying to wright data to
                      genre_film_work table, postgres
                      """)
