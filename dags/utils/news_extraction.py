"""
This module contains functions to extract news articles from websites, making use of the news-please library
(https://github.com/fhamborg/news-please)
"""

from newsplease import NewsPlease


def extract_news(url):
    """
    Extracts the title, description, authors, domain, image url, language, and content from a news article on a website.

    :param url: str - URL of the webpage
    :return: dict
    """

    news = {}
    article = NewsPlease.from_url(url)

    news['title'] = article.title
    news['description'] = article.description
    news['authors'] = article.authors
    news['source_domain'] = article.source_domain
    news['date_publish'] = article.date_publish
    news['image_url'] = article.image_url
    news['language'] = article.language
    news['text'] = article.text

    return news


def validate_extracted_news(news):
    """
    Validates the extracted news from a website by checking if it successfully extracted a title and text, and if is
    in english

    :param news: dict - extracted news data
    :return: bool
    """

    # if there is no title or text, return False
    if news['title'] is None:
        return False
    elif news['text'] is None:
        return False
    elif news['language'] is not None and news['language'] != 'en':
        return False
    else:
        return True


if __name__ == '__main__':
    news = extract_news('https://www.cnet.com/news/iphone-11-11r-and-11-max-what-to-expect-from-apple-in-september-new/')
    print(news)
    print(validate_extracted_news(news))
