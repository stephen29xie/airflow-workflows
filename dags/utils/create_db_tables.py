__doc__ = """
This file contains functions to create database tables. This will only be run manually once in the case we have a new
database and need new tables.
"""

from sqlalchemy import create_engine, exc
from configparser import ConfigParser
import os


def get_connection_string():
    """
    This function reads from the config.ini file and returns the connection string required for the SQLalchemy engine
    :return: connection string
    """
    # get path to config file relative to this file
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, '../../config.ini')

    # read config.ini file
    config = ConfigParser()
    config.read(filepath)

    CONNECTION_URI = 'postgresql+psycopg2://{}:{}@/{}?host={}'.format(
        config['Postgres']['username'],
        config['Postgres']['password'],
        config['Postgres']['dbname'],
        config['Postgres']['host'])

    return CONNECTION_URI


def create_reddit_summary_table(table_name):
    """
    Creates an empty table for the reddit and reddit summary data if one does not already exist.
    The db connection will be open and closed within this function.
    :return: None
    """

    SQLALCHEMY_DATABASE_URI = get_connection_string()

    CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS {} (\
                          fullname VARCHAR PRIMARY KEY,\
                          subreddit VARCHAR(50) NOT NULL,\
                          post_title VARCHAR(500) NOT NULL,\
                          url VARCHAR(200) NOT NULL,\
                          url_domain VARCHAR(200) NOT NULL,\
                          thumbnail_url VARCHAR,\
                          upvotes INT NOT NULL,\
                          downvotes INT NOT NULL,\
                          score INT NOT NULL,\
                          num_comments INT NOT NULL,\
                          post_datetime TIMESTAMP NOT NULL,\
                          post_datetime_uct TIMESTAMP NOT NULL,\
                          smmry_character_count INT NOT NULL,\
                          smmry_content_reduced FLOAT NOT NULL,\
                          smmry_title VARCHAR NOT NULL,\
                          smmry_content TEXT NOT NULL)".format(table_name)

    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
        connection = engine.connect()
        connection.execute(CREATE_TABLE_QUERY)
    except exc.SQLAlchemyError as err:
        print(err)
    finally:
        connection.close()

if __name__ == '__main__':
    create_reddit_summary_table('posts_prod')
