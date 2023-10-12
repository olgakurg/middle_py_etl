from .backoff import backoff
from .singleton import SingletonMeta
from elasticsearch import Elasticsearch, helpers
import logging
from .settings import E_LOG, index_body


class ElasticsearchConnection(metaclass=SingletonMeta):
    """Class of static elasticservice connection"""

    def __init__(self, url : str):
        self.es = Elasticsearch(url)

    @backoff(log_file=E_LOG)
    def get_connection(self):
        """establish a connection with elastic instance, uses special naive backoff procedure for recovery"""
        return self.es


class ElasticsearchLoader:
    """Class for woking with elasticsearch engine via elasticsearch8 module"""

    def __init__(self, conn):
        self.connection = conn

    def create_index(self):
        """Creates index if not exists"""
        if not self.connection.indices.exists(index="movies"):
            self.connection.indices.create(
                index="movies", body=index_body, ignore=[400, 404, 500]
            )

    def load_bulk(self, movies: list) -> str:
        """Loads bulk, return error message or success message for logging in main"""
        actions = [
            {"_index": "movies", "_id": movie.id, "_source": movie.dict()}
            for movie in movies
        ]

        logging.basicConfig(
            level=logging.ERROR,
            filename=E_LOG,
            filemode="w",
            format="%(asctime)s %(levelname)s %(message)s",
        )

        success = 0
        try:
            success, error = helpers.bulk(self.connection, actions)
        except helpers.BulkIndexError as error:
            logging.error(f"For bulk {len(movies)} some errors occurred {error}")
            return f"For bulk {len(movies)} some errors occurred {error}, was loaded {success}"
        else:
            return f"For bulk {len(movies)} was loaded {success}"
