from uuid import UUID
from pydantic_service import Movie, Actor, Writer
from operator import itemgetter
from itertools import groupby


def make_records(raw: list[dict]) -> list[dict]:
    """Merges raw response (list of values for each genre, person) to a list of records - dict coalesced for persons
    and genres"""

    raw.sort(key=itemgetter("fw_id"))
    record = []

    for key, group in groupby(raw, key=itemgetter("fw_id")):
        group = list(group)
        d = {
            "id": str(group[0]["fw_id"]),
            "title": group[0]["title"],
            "description": group[0]["description"],
            "rating": group[0]["rating"],
            "type": group[0]["type"],
            "persons": [],
            "genre": [],
        }
        for item in group:
            person_item = itemgetter("id", "full_name", "role")(item)
            person_dict = {
                "person_id": str(person_item[0]),
                "person_name": person_item[1],
                "person_role": person_item[2],
            }
            if person_dict not in d["persons"]:
                d["persons"].append(person_dict)
            if item["genre"] not in d["genre"]:
                d["genre"].append(item["genre"])
        record.append(d)

    return record


def convert_to_movie(row: dict) -> Movie:
    """Converts and valid coalesced dict to Movie instance"""

    actors = [
        Actor(id=p["person_id"], name=p["person_name"])
        for p in row["persons"]
        if p["person_role"] == "actor"
    ]
    writers = [
        Writer(id=p["person_id"], name=p["person_name"])
        for p in row["persons"]
        if p["person_role"] == "writer"
    ]
    directors = [
        p["person_name"] for p in row["persons"] if p["person_role"] == "director"
    ]
    genres = [item for item in row["genre"]]
    movie_dict = {
        "id": row["id"],
        "imdb_rating": row["rating"] if row["rating"] else 0.0,
        "genre": [genre for genre in genres],
        "title": row["title"],
        "description": row["description"] if row["description"] else "",
        "director": directors,
        "actors_names": [actor.name for actor in actors],
        "writers_names": [writer.name for writer in writers],
        "actors": actors,
        "writers": writers,
    }
    return Movie.parse_obj(movie_dict)


def transform_bulk(rows: list[dict]) -> list[Movie]:
    records = make_records(rows)
    movies = [convert_to_movie(record) for record in records]
    return movies
