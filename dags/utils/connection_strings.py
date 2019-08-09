"""
This module contains functions for creating database connection strings
"""

import os
from configparser import ConfigParser


def get_postgres_connection_string():
    """
    Reads from config.ini and returns the SQLalchemy connection string for the Postgres instance which will
    serve as our metadata database

    :return: Str
    """
    # get path to config file relative to this file
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, '../../config.ini')
    # read config.ini file
    config = ConfigParser()
    config.read(filepath)

    conn_string = 'postgresql+psycopg2://{}:{}@{}/{}'.format(
        config['postgres_metadata_db']['username'],
        config['postgres_metadata_db']['password'],
        config['postgres_metadata_db']['host'],
        config['postgres_metadata_db']['dbname'])

    return conn_string


if __name__ == '__main__':
    print(get_postgres_connection_string())
