from typing import List
from pydantic import BaseModel

class Actor(BaseModel):
    id: str
    name: str

class Writer(BaseModel):
    id: str
    name: str

class Movie(BaseModel):
    id : str
    imdb_rating: float
    genre: List[str]
    title: str
    description: str
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Actor]
    writers: List[Writer]
