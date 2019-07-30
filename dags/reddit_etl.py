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

from utils.summary import summarize, validate_summary
from utils.reddit_data import get_reddit_posts, validate_reddit_post

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


def reddit_etl_callable(subreddit, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id=config['Airflow']['postgres_conn_id'],
                                 schema=config['Postgres']['dbname'])

    postgres_hook_conn = postgres_hook.get_conn()
    postgres_hook_cur = postgres_hook_conn.cursor()

    try:
        reddit_posts = get_reddit_posts(subreddit)
        logging.info('Succesfully downloaded posts from Reddit')
    except:
        logging.error('Error occured trying to get posts from Reddit')
        reddit_posts = []

    insertions = 0

    # If we fetch a post that we have already fetched before, we will just update the score, upvotes, downvotes, and.
    # number of parent comments. All the other information is the same
    for post in reddit_posts:

        logging.info('Attempting to process post ' + post['fullname'] + '...')

        # validate the reddit post to see if it has all the required information we want. If not we wont use it
        if not validate_reddit_post(post):
            logging.info('Reddit post not valid')
            continue

        # Summarize the news in the URL linked in the reddit post
        try:
            summary = summarize(post['url'])
            logging.info('Sucessfully summarized URL')
        except APIException as e:
            logging.info('Unable to summarize URL, encountered APIException')
            continue
        except ClientException as e:
            logging.info('Unable to summarize URL, encountered ClientException')
            continue
        except PRAWException as e:
            logging.info('Unable to summarize URL, encountered PrawException')
            continue

        # validate the reddit post to see if it has all the required information we want. If not we wont use it
        if not validate_summary(summary):
            logging.info('Summary not valid')
            continue

        # If it exists already, update the votes and number of comments.
        # Replace % with %% and ' with '' to escape these special characters in the query string
        QUERY = "INSERT INTO {} VALUES (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \
                                            {}, {}, {}, {}, \'{}\', \'{}\', {}, \'{}\', \'{}\', \'{}\')\
                     ON CONFLICT (fullname) \
                     DO UPDATE \
                     SET score = {}, upvotes = {}, downvotes = {}, num_comments = {}".format(
                        config['Postgres']['table'],
                        post['fullname'].replace("'", "''").replace('%', '%%'),
                        subreddit,
                        post['title'].replace("'", "''").replace('%', '%%'),
                        post['url'].replace("'", "''").replace('%', '%%'),
                        post['url_domain'].replace("'", "''").replace('%', '%%'),
                        post['thumbnail_url'].replace("'", "''").replace('%', '%%'),
                        post['upvotes'],
                        post['downvotes'],
                        post['score'],
                        post['num_comments'],
                        post['post_datetime'],
                        post['post_datetime_utc'],
                        int(summary['sm_api_character_count']),
                        float(summary['sm_api_content_reduced'].replace('%', '')) / 100,
                        summary['sm_api_title'].replace("'", "''").replace('%', '%%'),
                        summary['sm_api_content'].replace("'", "''").replace('%', '%%'),
                        post['score'],
                        post['upvotes'],
                        post['downvotes'],
                        post['num_comments'])

        postgres_hook_cur.execute(QUERY)
        postgres_hook_conn.commit()
        logging.info('Successfully processed and inserted post ' + post['fullname'])
        insertions += 1

    postgres_hook_cur.close()
    postgres_hook_conn.close()
    logging.info('Succesfully processed and inserted {} posts to the database'.format(insertions))


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
