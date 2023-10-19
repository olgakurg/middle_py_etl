import logging
import time

import psycopg
from psycopg.rows import dict_row

from utils.elastic_service import ElasticsearchConnection, ElasticsearchLoader
from utils.json_state import JsonState
from utils.pg_service import PostgresConnection, PostgresExtractor
from utils.settings import (
    BULK_SIZE,
    TIME_OUT,
    main_state,
    DSL,
    EDSL,
    initial_date,
    all_sources,
    initial_source,
)
from utils.transform_service import transform_bulk

def bulk_etl(
    cursor: psycopg.Cursor,
    extractor: PostgresExtractor,
    loader: ElasticsearchLoader,
    data_source: list,
):

    """Procedure of iterative extacting, transforming and loading modified data. Data divides into some bulks via fetchmany select"""

    state = JsonState(main_state)
    for table in data_source:
        current_state = state.retrieve_state()
        last_uploaded = current_state[table]
        query = extractor.query_for_new_ids(table, last_uploaded)
        try:
            tmp = cursor.execute(query)
        except psycopg.Error as err:
            logging.error(err)
        while (new_ids := cursor.fetchmany(BULK_SIZE)):
            last_id = new_ids[-1]
            current_state[table] = last_id["modified"].strftime("%Y-%m-%d %H:%M:%S.%f%z")
            bulk = extractor.extract(table, new_ids)
            record = transform_bulk(bulk)
            msg = loader.load_bulk(record)
            logging.info(msg)
            state.save_state(current_state)


def main():
    """Starts etl proccess in two modes - initial if it wasn't started before and have no state, or cyclic if we have some state"""
    logging.basicConfig(
        level=logging.INFO,
        filename="main_log.log",
        filemode="w",
        format="%(asctime)s %(levelname)s %(message)s",
    )
    postgres_conn = PostgresConnection(**DSL)
    elastic_conn = ElasticsearchConnection(EDSL)
    state = JsonState(main_state)

    with postgres_conn.get_connection() as pg_conn, elastic_conn.get_connection() as es_conn, pg_conn.cursor(
        row_factory=dict_row
    ) as cursor:
        extractor = PostgresExtractor(pg_conn, cursor)
        loader = ElasticsearchLoader(es_conn)
        current_state = state.retrieve_state()
        if current_state:
            while True:
                print("CYCLE LOADING STARTS")
                bulk_etl(cursor, extractor, loader, all_sources)
                time.sleep(TIME_OUT)

        else:
            current_state = dict.fromkeys(
                ["genre", "person", "film_work"], initial_date
            )
            state.save_state(current_state)
            loader.create_index()
            bulk_etl(cursor, extractor, loader, initial_source)
    postgres_conn.close_connection()

if __name__ == "__main__":
    main()
