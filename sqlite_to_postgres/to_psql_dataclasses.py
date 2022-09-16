import uuid
from dataclasses import dataclass, field


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: str
    file_path: str
    type: str
    created_at: str
    updated_at: str
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created_at: str
    updated_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    description: str
    created_at: str
    updated_at: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilm:
    role: str
    created_at: str
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class FilmGenre:
    created_at: str
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)
