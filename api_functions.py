from sqlalchemy import create_engine
import pandas as pd
import requests
import urllib
import gspread
import json
import concurrent.futures
from notification import send_email

header = None


def connect_to_api():
    global header
    # Authenticate to the API
    param = {'username': 'C6993', 'password': 'DTXZDX'}
    header = {'Content-type': 'application/x-www-form-urlencoded'}
    url = 'https://api.puig.tv/es/login'
    response = requests.post(url, params=param, headers=header)
    if response.status_code == 200:
        token = response.json()['data']['token']
        header = {'Api-Token': token}
    else:
        send_email("PUIG API script failed.",
                   "Authentication to PUIG API failed.")
    send_email("PUIG API script failed.",
               "Authentication to PUIG API failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.")
    raise Exception("Failed to connect to API")
