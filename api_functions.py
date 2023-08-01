from sqlalchemy import create_engine
import pandas as pd
import requests
import urllib
import gspread
import json
import os
import concurrent.futures
from notification import send_email

header = None
pool = None
pool.autocommit = True


def connect_to_api():
    """Connect to the API."""
    global header
    username = os.environ.get('PUIG_API_UNAME')
    password = os.environ.get('PUIG_API_PASS')
    # Authenticate to the API
    param = {'username': username, 'password': password}
    header = {'Content-type': 'application/x-www-form-urlencoded'}
    url = 'https://api.puig.tv/es/login'
    try:
        response = requests.post(url, params=param, headers=header)
        if response.status_code == 200:
            token = response.json()['data']['token']
            header = {'Api-Token': token}
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Authentication to PUIG API failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e


def connect_to_db():
    """Connect to the database."""
    global pool
    db_host = os.environ.get('INSTANCE_HOST')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_name = os.environ.get('DB_PUIG')
    db_port = os.environ.get('DB_PORT')
    connect_string = f"postgresql+psycopg2://{db_user}:{urllib.parse.quote_plus(db_pass)}@{db_host}:{db_port}/{db_name}"
    try:
        pool = create_engine(connect_string)
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Authentication to database failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e
