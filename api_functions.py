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


def bikes_process_endpoint(endpoint):
    try:
        # API request to retreive list of product details
        response_details = requests.get(endpoint, headers=header)
        data = json.loads(response_details.text)
        df = pd.DataFrame()
        df.insert(0, 'ref_sku', 'null')
        df.insert(1, 'variations', 'null')
        df.insert(2, 'category', 'null')
        df.insert(3, 'description', 'null')
        df.insert(4, 'bikes', 'null')
        df.insert(5, 'manual', 'null')
        df.insert(6, 'aerotest', 'null')
        df.insert(7, 'comparative', 'null')
        df.at[0, 'ref_sku'] = str(data['data']['code'])
        df.at[0, 'variations'] = str(data['data']['variations'])
        df.at[0, 'category'] = str(data['data']['product'])
        df.at[0, 'description'] = str(data['data']['groups'])
        df.at[0, 'bikes'] = str(data['data']['bikes'])
        df.at[0, 'manual'] = str(data['data']['instructions'])
        df.at[0, 'aerotest'] = str(data['data']['aerotest'])
        df.at[0, 'comparative'] = str(data['data']['comparative'])
        return df
    except:
        pass


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
            # Get a list of all the id
            ids = bikes_df['id'].tolist()
            # Append the ids to the string
            endpoint = ['https://api.puig.tv/es/bikes/' +
                        str(id) for id in ids]

            print(endpoint)
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
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET PRODUCTS
# -------------------------------------------------------------------------------------------------------------------------------
# function to be executed in parallel threads


def products__process_endpoint(endpoint):
    # API request to retreive list of product details
    response_details = requests.get(endpoint, headers=header)
    data = json.loads(response_details.text)
    df = pd.DataFrame()
    df.insert(0, 'id', 'null')
    df.insert(1, 'title', 'null')
    df.insert(2, 'ref_sku', 'null')
    df.insert(3, 'description', 'null')
    df.at[0, 'id'] = str(data['data']['id'])
    df.at[0, 'title'] = str(data['data']['title'])
    df.at[0, 'ref_sku'] = str(data['data']['references'])
    df.at[0, 'description'] = str(
        data['data']['description']) + ' | ' + str(data['data']['technical'])
    return df


def get_products():
    # Backing current data from the database
    sql_query = 'select * from "products";'
    products_backup_df = db_read(sql_query)
    # API request to retreive list of products
    response_products = requests.get(
        'https://api.puig.tv/en/products', headers=header)
    # Convert list of products into dataframe
    sku_df = pd.DataFrame(response_products.json()['data'])
    # empty list for url's
    endpoints = []
    # appending all url's to endpoints list
    for i, row in sku_df.iterrows():
        endpoints.append('https://api.puig.tv/en/products/' + str(row['id']))
    # Create an empty DataFrame to store the results
    products_df = pd.DataFrame()
    # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit each API endpoint to the executor
        futures = [executor.submit(
            products__process_endpoint, endpoint) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            products_df = pd.concat([products_df, df], axis=0)

    products_df = products_df.replace(
        {"\[": '', "\]": '', "'": "", "\r": '', "\n": ''}, regex=True)
    products_df['ref_sku'] = products_df['ref_sku'].str.replace(' ', '')
    # Concatinating the current data with the new data
    products_df = pd.concat([products_backup_df, products_df], axis=0)
    # Droping duplicated products by id
    # This is done as occationally api calls fail and the products drops completly from the database
    products_df.drop_duplicates(subset=['id'], keep="last", inplace=True)
    db_write(products_df, "products")
    sh_write(products_df, "PUIG", "products")
    print("products")
