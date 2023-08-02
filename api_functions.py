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
    try:
        # API request to retreive list of bikes
        response = requests.get(
            'https://api.puig.tv/en/bikes', headers=header)
        if response.status_code == 200:
            # Convert list of bikes into dataframe
            bikes_df = pd.DataFrame(response.json()['data'])
            bikes_df['puig_final_name'] = bikes_df['brand'].astype(
                str)+" "+bikes_df['model'].astype(str)+" "+bikes_df['year'].astype(str)

            bikes_df['puig_final_name'] = bikes_df['puig_final_name'].str.replace(
                '  ', ' ')
            db_write(bikes_df, "bikes")
            sh_write(bikes_df, "PUIG", "bikes")
            print("bikes")
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_bikes() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET CATEGORIES
# -------------------------------------------------------------------------------------------------------------------------------


def get_categories():
    try:
        response = requests.get(
            'https://api.puig.tv/en/categories', headers=header)
        if response.status_code == 200:
            # Convert list of bikes into dataframe
            categories_df = pd.DataFrame(response.json()['data'])
            db_write(categories_df, "categories")
            sh_write(categories_df, "PUIG", "categories")
            print("categories")
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_categories() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET REFERENCES(all REFERENCE sku's)
# -------------------------------------------------------------------------------------------------------------------------------


def get_references():
    try:
        # API request to retreive list of references
        response = requests.get(
            'https://api.puig.tv/en/references', headers=header)
        if response.status_code == 200:
            # Convert list of references into dataframe
            references_df = pd.DataFrame(response.json()['data'])
            references_df.rename(columns={0: 'ref_sku'}, inplace=True)
            # Droping known sku with error
            references_df.drop(
                references_df[references_df.ref_sku == "5020N/G"].index, inplace=True)
            references_df.drop_duplicates(
                subset=['ref_sku'], keep="first", inplace=True)
            db_write(references_df, "references")
            sh_write(references_df, "PUIG", "references")
            print("references")
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_references() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e
