"""
This file should be run before starting the webserver to set up user-password authentication

"When password auth is enabled, an initial user credential will need to be created before anyone can login.
Creating a new user has to be done via a Python REPL on the same machine Airflow is installed."

More details at https://airflow.apache.org/security.html#web-authentication
"""

import os
from configparser import ConfigParser
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


def create_superuser():
    """
    Creates a superuser (admin level) for accessing the web UI for Airflow

    :return: None
    """

    # get path to config file relative to this file
    current_dir = os.path.dirname(__file__)
    filepath = os.path.join(current_dir, 'config.ini')
    # read config.ini file
    config = ConfigParser()
    config.read(filepath)

    # create User object
    user = PasswordUser(models.User())
    user.username = config['web_authentication']['username']
    user.email = config['web_authentication']['email']
    user.password = config['web_authentication']['password']
    user.superuser = True

    # Add user to Airflow session
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()


if __name__ == '__main__':
    create_superuser()
