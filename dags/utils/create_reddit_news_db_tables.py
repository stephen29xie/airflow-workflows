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
        config['reddit_news_db']['username'],
        config['reddit_news_db']['password'],
        config['reddit_news_db']['dbname'],
        config['reddit_news_db']['host'])

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
                          score INT NOT NULL,\
                          num_comments INT NOT NULL,\
                          post_datetime_utc TIMESTAMP NOT NULL,\
                          news_title VARCHAR NOT NULL,\
                          news_description TEXT,\
                          news_author VARCHAR,\
                          news_domain VARCHAR,\
                          news_date_publish TIMESTAMP,\
                          news_image_url VARCHAR,\
                          news_text TEXT NOT NULL,\
                          news_word_count INT NOT NULL)".format(table_name)

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
