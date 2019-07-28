import requests
import os
from configparser import ConfigParser


# get path to config file relative to this file
current_dir = os.path.dirname(__file__)
filepath = os.path.join(current_dir, '../../config.ini')

# read config.ini file
config = ConfigParser()
config.read(filepath)


def summarize(url):
    """
    A wrapper for the SMRRY API using the requests library.
    Makes requests to the SMMRY API (https://smmry.com) to get summarized news article data and metadata.

    Free: A limit of 100 free API requests can be made daily, and each request must be at least 10 seconds apart.

    :param url: Str - URL of the news article to summarize
    :return: Dict - contains character count, content reduced %, title, summarized content, and api limitation info
             of the news article
    """

    key = config['SMMRY']['api_key']

    SMMRY_url_params = {}
    SMMRY_url_params['SM_API_KEY'] = key
    SMMRY_url_params['SM_LENGTH'] = 7
    SMMRY_url_params['SM_URL'] = url

    response = requests.get('https://api.smmry.com', params=SMMRY_url_params)

    summary = response.json()

    return summary


def validate_summary(summary):
    """
    If the summary returned by SMMRY has an error, or is missing the title or content, return False. Else return True.

    :param summary: Dict - data returned from the summarize() function.
    :return: Bool
    """

    if 'sm_api_error' in summary:
        return False
    elif summary['sm_api_content'] == '' or summary['sm_api_content'] is None:
        return False
    elif summary['sm_api_character_count'] == 0:
        return False
    elif summary['sm_api_title'] == '' or summary['sm_api_title'] is None:
        return False
    elif summary['sm_api_content_reduced'] == '0%':
        return False
    else:
        return True


if __name__ == '__main__':
    print(summarize('https://www.cnet.com/news/iphone-11-11r-and-11-max-what-to-expect-from-apple-in-september-new/'))
