import pandas as pd
import requests
import urllib
import json
import os
import concurrent.futures
from notification import send_email
from api_data_read_write import *

header = None
# -------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO PUIG API
# -------------------------------------------------------------------------------------------------------------------------------


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

# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET BIKES
# -------------------------------------------------------------------------------------------------------------------------------


def get_bikes():
    # API request to retreive list of bikes
    response_bikes = requests.get(
        'https://api.puig.tv/en/bikes', headers=header)
    # Convert list of bikes into dataframe
    bikes_df = pd.DataFrame(response_bikes.json()['data'])
    bikes_df['puig_final_name'] = bikes_df['brand'].astype(
        str)+" "+bikes_df['model'].astype(str)+" "+bikes_df['year'].astype(str)

    bikes_df['puig_final_name'] = bikes_df['puig_final_name'].str.replace(
        '  ', ' ')
    # db_display(bikes_df, "bikes")
    sh_write(bikes_df, "PUIG", "bikes")
    print(bikes_df)
