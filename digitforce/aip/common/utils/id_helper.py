# coding: utf-8
from snowflake.server import Generator

generator = Generator(dc=1, worker=1)


def gen_uniq_id():
    return generator.get_next_id()
