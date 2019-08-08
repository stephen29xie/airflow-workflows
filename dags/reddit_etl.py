"""
This DAG pulls news posts from Reddit, summarizes the news article, and inserts the post data, summarized content, and
other metadata into a Postgres db.
"""

from configparser import ConfigParser
import os
from datetime import datetime, timedelta
from praw.exceptions import APIException, PRAWException, ClientException
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG

from utils.reddit_data import get_reddit_posts, validate_reddit_post
from utils.news_extraction import extract_news, validate_extracted_news

# get path to config file relative to this file
current_dir = os.path.dirname(__file__)
filepath = os.path.join(current_dir, '../config.ini')
# read config.ini file
config = ConfigParser()
config.read(filepath)

default_args = {
    'owner': 'Stephen Xie',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 20),
    'email': ['stephen29xie@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='reddit_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)


def format_postgres_string(string):
    """
    PostgreSQL syntax has some intricacies that we must follow when formatting query strings.
    "%", and "'" must be escaped or they will be interpreted as some other functionality that is not a
    string literal.

    This function takes in a string that is meant to be formatted into a PostgreSQL query and returns the modified
    string safe to format into a query

    :param string: the string meant to be formatted into the PostgreSQL query
    :return: str or None
    """

    if string is None:
        return None
    else:
        return string.replace("'", "''").replace('%', '%%')


def reddit_etl_callable(subreddit, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id=config['Airflow']['postgres_conn_id'],
                                 schema=config['Postgres']['dbname'])

    postgres_hook_conn = postgres_hook.get_conn()
    postgres_hook_cur = postgres_hook_conn.cursor()

    try:
        reddit_posts = get_reddit_posts(subreddit)
        logging.info('Succesfully downloaded posts from Reddit')
    except APIException:
        logging.error('Encountered Praw APIException. Error occured trying to fetch posts from Reddit')
        reddit_posts = []
    except PRAWException:
        logging.error('Encountered Praw PRAWException. Error occured trying to fetch posts from Reddit')
        reddit_posts = []
    except ClientException:
        logging.error('Encountered Praw ClientException. Error occured trying to fetch posts from Reddit')
        reddit_posts = []

    insertions = 0
    invalid_posts = 0

    # If we fetch a post that we have already fetched before, we will just update the score, upvotes, downvotes, and.
    # number of parent comments. All the other information is the same
    for post in reddit_posts:

        logging.info('Attempting to process post ' + post['fullname'] + '...')

        # validate the reddit post to see if it has all the required information we want. If not we wont use it
        if not validate_reddit_post(post):
            logging.info('Reddit post not valid')
            invalid_posts += 1
            continue

        # extract news from the URL linked in the reddit post
        try:
            news = extract_news(post['url'])
        except:
            logging.info('Unable to extract news from webpage')
            invalid_posts += 1
            continue

        # validate news
        if not validate_extracted_news(news):
            logging.info('Extracted news not valid, will not be stored')
            invalid_posts += 1
            continue

        # If it exists already, update the votes and number of comments.
        # Replace % with %% and ' with '' to escape these special characters in the query string
        QUERY = "INSERT INTO {table}\
                    VALUES (\'{fullname}\', \'{subreddit}\', \'{title}\', \'{url}\', \'{url_domain}\',\
                            \'{thumbnail_url}\', {score}, {num_comments},\
                            \'{post_datetime_utc}\', \'{news_title}\', \'{news_description}\', \'{news_authors}\',\
                            \'{news_source_domain}\', \'{news_date_publish}\', \'{news_image_url}\', \'{news_text}\',\
                            {news_word_count})\
                    ON CONFLICT (fullname) \
                    DO UPDATE \
                    SET score = {score},\
                        num_comments = {num_comments}".format(table=config['Postgres']['table'],
                                                              fullname=format_postgres_string(post['fullname']),
                                                              subreddit=format_postgres_string(subreddit),
                                                              title=format_postgres_string(post['title']),
                                                              url=format_postgres_string(post['url']),
                                                              url_domain=format_postgres_string(post['url_domain']),
                                                              thumbnail_url=format_postgres_string(post['thumbnail_url']),
                                                              score=post['score'],
                                                              num_comments=post['num_comments'],
                                                              post_datetime_utc=post['post_datetime_utc'],
                                                              news_title=format_postgres_string(news['title']),
                                                              news_description=format_postgres_string(news['description']),
                                                              news_authors=format_postgres_string(news['authors']),
                                                              news_source_domain=format_postgres_string(news['source_domain']),
                                                              news_date_publish=news['date_publish'],
                                                              news_image_url=format_postgres_string(news['image_url']),
                                                              news_text=format_postgres_string(news['text']),
                                                              news_word_count=news['word_count'])

        postgres_hook_cur.execute(QUERY)
        postgres_hook_conn.commit()
        logging.info('Successfully processed and inserted post ' + post['fullname'])
        insertions += 1

    postgres_hook_cur.close()
    postgres_hook_conn.close()
    logging.info('Successfully processed and inserted {} posts to the database. Unable to insert {} posts'.format(insertions,
                                                                                                                  invalid_posts))


start_task = DummyOperator(task_id='start_dummy',
                           dag=dag)

end_task = DummyOperator(task_id='end_dummy',
                         dag=dag)

etl_technews = PythonOperator(task_id='etl_technews',
                              python_callable=reddit_etl_callable,
                              provide_context=True,
                              op_kwargs={'subreddit': 'technews'},
                              dag=dag)

start_task >> etl_technews >> end_task
