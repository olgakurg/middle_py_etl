import psycopg
import logging
from .singleton import SingletonMeta
from .backoff import backoff
from .settings import PG_LOG
from datetime import datetime
from psycopg.rows import dict_row


class PostgresConnection(metaclass=SingletonMeta):
    """Class of static postgres connection"""
    def __init__(self, **kwargs):
        self.conn = psycopg.connect(**kwargs)

    @backoff(log_file=PG_LOG)
    def get_connection(self):
        """establishes a connection with db, uses special naive backoff procedure for recovery"""
        return self.conn


class PostgresExtractor:
    """Class for working with Postgres"""

    def __init__(self, conn_instance, cursor):
        self.connection = conn_instance
        self.cursor = cursor

    def get_throw_refs(self, table_name: str, uploaded_ref_ids: list[str]) -> set():
        """Retrives a set if filmwork ids, which has modified genres or persons"""
        fw_ids = set()
        films = []
        ids_str = ", ".join(f"'{id}'" for id in uploaded_ref_ids)
        fw_by_ref_query = f"""
                SELECT fw.id
                FROM content.film_work fw
                LEFT JOIN content.{table_name}_film_work ifw ON ifw.film_work_id = fw.id
                WHERE ifw.{table_name}_id IN ({ids_str})
                    """
        with self.connection.cursor() as cursor:
            try:
                rows = cursor.execute(fw_by_ref_query)
                films = rows.fetchall()
            except psycopg.Error as err:
                logging.error(err)
            if films:
                for film in films:
                    fw_ids.add(str(film[0]))
            cursor.close()
        return fw_ids

    def query_for_new_ids(self, table_name: str, last_unloaded: str) -> str:
        """Make up a query for modified instances from tables via table name and date of modification params"""
        return f"""
                SELECT id, modified
                FROM content.{table_name}
                WHERE modified > '{last_unloaded}'
                ORDER BY modified;
                    """

    def enrich_by_id(self, ids: list[str]) -> list[dict]:
        """Selects raw data from db via id of modified intance. Raw data is a list of dicts, every dict contains comvination of instance attrs"""
        films = []
        ids_str = ", ".join(f"'{id}'" for id in ids)
        big_query = f"""SELECT
                        fw.id as fw_id,
                        fw.title, 
                        fw.description, 
                        fw.rating, 
                        fw.type,
                        fw.modified, 
                        pfw.role, 
                        p.id, 
                        p.full_name,
                        g.name as genre
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.id IN ({ids_str});
                    """

        with self.connection.cursor(row_factory=dict_row) as cursor:
            try:
                rows = cursor.execute(big_query)
                films = rows.fetchall()
            except psycopg.Error as err:
                print(err)
        return films

    def extract(self, table_name: str, ids: list) -> list[dict]:
        """Extract raw data via id of modified intance. Raw data is a list of dicts, every dict contains comvination of instance attrs"""
        ids = [str(id["id"]) for id in ids]
        if table_name in ("genre", "person"):
            fw_ids = self.get_throw_refs(table_name, ids)
            return self.enrich_by_id(fw_ids)

        return self.enrich_by_id(ids)
