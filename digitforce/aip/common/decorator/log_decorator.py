import logging
from functools import wraps


def log_func(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"begin run {func.__name__} {args} {kwargs}")
        res = func(*args, **kwargs)
        return res

    return wrapper
