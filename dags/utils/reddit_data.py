import os
import praw
from configparser import ConfigParser
import datetime


def get_reddit_posts(subreddit):
    """
    Makes requests to the Reddit API (PRAW) to query for the top 10 hottest posts in the specified subreddit

    Any single reddit listing will display at most 1000 items. This is true for all listings including subreddit
    submission listings, user submission listings, and user comment listings. Reddit allows requests of up to 100 items
    at once. So if you request <= 100 items PRAW can serve your request in a single API call, but for larger requests
    PRAW will break it into multiple API calls of 100 items each separated by a small 2 second delay to follow the
    api guidelines. So requesting 250 items will require 3 api calls and take at least 2x2=4 seconds due to API delay.
    PRAW does the API calls lazily, i.e. it will not send the next api call until you actually need the data.
    Meaning the runtime is max(api_delay, code execution time)


    :param subreddit: Str - the subreddit we want to get data from
    :return: List[Dict] - list of Dictionaries, each one representing a Reddit post. Each dict contains an id, title,
             url, score, thumbnail url, url domain, num comments, and posting time.
    """

    # get path to config file relative to this file
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, '../../config.ini')

    # read config.ini file
    config = ConfigParser()
    config.read(filepath)

    reddit = praw.Reddit(client_id=config['Reddit']['client_id'],
                         client_secret=config['Reddit']['client_secret'],
                         user_agent=config['Reddit']['user_agent'])

    # get hottest posts from all subreddits
    hot_posts = reddit.subreddit(subreddit).top(limit=4)

    posts = []
    for post in hot_posts:
        # Create a dictionary object for each post.
        json = {}
        json['fullname'] = post.fullname
        json['title'] = post.title
        json['url'] = post.url
        json['upvotes'] = post.ups
        json['downvotes'] = post.downs
        json['score'] = post.score
        json['thumbnail_url'] = post.thumbnail
        json['url_domain'] = post.domain
        json['num_comments'] = post.num_comments
        # cast epoch times to datetimes
        json['post_datetime'] = datetime.datetime.fromtimestamp(post.created).strftime('%Y-%m-%d %H:%M:%S')
        json['post_datetime_utc'] = datetime.datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        posts.append(json)

    return posts


def validate_reddit_post(post):
    """
    If a reddit post does not provide an external link, or does not provide a link at all, return False.
    Else return True.

    :param post:
    :return: Bool
    """

    # validate that posts actually have a URL, and any other absolutely required info.
    # if required info is missing, skip this post
    if post['url'] == '' or post['url'] is None or post['url'].startswith("https://www.reddit.com"):
        return False
    else:
        return True


if __name__ == '__main__':
    print(get_reddit_posts('technews'))
