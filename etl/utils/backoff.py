import time
from functools import wraps
import logging


def backoff(
    start_sleep_time=0.1, factor=2, border_sleep_time=10, log_file="backoff.log"
):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            logging.basicConfig(
                level=logging.ERROR,
                filename=log_file,
                filemode="w",
                format="%(asctime)s %(levelname)s %(message)s",
            )
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs) 
                except Exception as e:
                    logging.error(f"Error: {e}, retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * factor, border_sleep_time)

        return inner

    return func_wrapper