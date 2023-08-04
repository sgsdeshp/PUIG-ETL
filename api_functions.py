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
                   f"Authentication to PUIG API failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
        raise e

# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET BIKES
# -------------------------------------------------------------------------------------------------------------------------------


def bikes_process_endpoint(endpoint):
    try:
        # API request to retreive list of product details
        response_details = requests.get(endpoint, headers=header)
        if response_details.status_code == 200:
            # data = json.loads(response_details.text)
            # df = pd.DataFrame(data['data'])
            df = pd.DataFrame(response_details.json()['data'])
            # Split the references column into multiple rows
            df = df.explode('references')
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
            # Get a list of all the id
            ids = bikes_df['id'].tolist()
            # Append the ids to the string
            endpoints = ['https://api.puig.tv/en/bikes/' +
                         str(id) for id in ids]
            # endpoints = ['https://api.puig.tv/en/bikes/8499']
            # Create an empty DataFrame to store the results
            bikes_df = pd.DataFrame()
            # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit each API endpoint to the executor
                futures = [executor.submit(
                    bikes_process_endpoint, endpoint) for endpoint in endpoints]
                # Iterate over each completed future and append the result to the products_df DataFrame
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    bikes_df = pd.concat([bikes_df, df], axis=0)
            bikes_df['puig_final_name'] = bikes_df['brand'].astype(
                str)+" "+bikes_df['model'].astype(str)+" "+bikes_df['year'].astype(str)

            bikes_df['puig_final_name'] = bikes_df['puig_final_name'].str.replace(
                '  ', ' ')
            db_write(bikes_df, "bikes")
            bikes_df = bikes_df.drop(
                ['id', 'brand', 'model', 'year', 'references'], axis=1)
            sh_write(bikes_df, "PUIG", "bikes")
            print(bikes_df)
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_bikes() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
        raise e
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET CATEGORIES
# -------------------------------------------------------------------------------------------------------------------------------


def categories_process_endpoint(endpoint):
    try:
        # API request to retreive list of product details
        response_details = requests.get(endpoint, headers=header)
        if response_details.status_code == 200:
            df = pd.DataFrame(response_details.json()['data'])
            return df
    except:
        pass


def get_categories():
    try:
        response = requests.get(
            'https://api.puig.tv/en/categories', headers=header)
        if response.status_code == 200:
            # Convert list of bikes into dataframe
            categories_df = pd.DataFrame(response.json()['data'])
            db_write(categories_df, "categories")
            sh_write(categories_df, "PUIG", "categories")

            # Get a list of all the id
            ids = categories_df['id'].tolist()
            # Append the ids to the api request url as a list of strings
            endpoints = ['https://api.puig.tv/en/categories/' +
                         str(id) for id in ids]
            # endpoints = ['https://api.puig.tv/en/categories/200498']
            # Create an empty DataFrame to store the results
            subcategories_df = pd.DataFrame()
            # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit each API endpoint to the executor
                futures = [executor.submit(
                    categories_process_endpoint, endpoint) for endpoint in endpoints]
                # Iterate over each completed future and append the result to the products_df DataFrame
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    subcategories_df = pd.concat(
                        [subcategories_df, df], axis=0)
            db_write(subcategories_df, "subcategories")
            sh_write(subcategories_df, "PUIG", "subcategories")
            print(subcategories_df)
    except Exception as e:
        # send_email("PUIG API script failed.",   f"Function get_categories() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
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
                   f"Function get_references() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
        raise e
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET PRODUCTS
# -------------------------------------------------------------------------------------------------------------------------------
# function to be executed in parallel threads


def products_process_endpoint(endpoint):
    try:
        # API request to retreive list of product details
        response_details = requests.get(endpoint, headers=header)
        df = pd.DataFrame(columns=['id', 'title', 'description', 'technical', 'homologation',
                          'references', 'bikes', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''])
        if response_details.status_code == 200:
            data = json.loads(response_details.text)['data']
            # data = data['data']
            # Convert the dictionary to a list of key-value pairs
            records = list(data.items())
            df = pd.DataFrame.from_records(records, columns=['key', 'value'])
            # Set the key column as the index
            df.set_index('key', inplace=True)
            print(df)
            # Iterate over the keys in the JSON object
            for key, value in data.items():
                # Create a new row in the DataFrame
                # row = {'key': key, 'value': value}
                # row = {key: value}
                # print(key, value)
                # Append the row to the DataFrame
                # df = pd.DataFrame.from_dict(row)
                # df = pd.DataFrame(response_details.json()['data'])
                # return df
                # print(df)
                ...
    except Exception as e:
        raise e


def get_products():
    # Backing current data from the database
    # sql_query = 'select * from "products";'
    # products_backup_df = db_read(sql_query)
    # API request to retreive list of products
    response_products = requests.get(
        'https://api.puig.tv/en/products', headers=header)
    # Convert list of products into dataframe
    products_df = pd.DataFrame(response_products.json()['data'])
    # Get a list of all the id
    ids = products_df['id'].tolist()
    # Append the ids to the api request url as a list of strings
    # endpoints = ['https://api.puig.tv/en/products/' + id for id in ids]
    endpoints = ['https://api.puig.tv/en/products/1101132']
    # Create an empty DataFrame to store the results
    product_details_df = pd.DataFrame()
    # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit each API endpoint to the executor
        futures = [executor.submit(
            products_process_endpoint, endpoint) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            product_details_df = pd.concat(
                [product_details_df, df], axis=0)
    # sh_write(product_details_df, "PUIG", "products")
    """
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
            products_process_endpoint, endpoint) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            products_df = pd.concat([products_df, df], axis=0)
    
    # products_df = products_df.replace({"\[": '', "\]": '', "'": "", "\r": '', "\n": ''}, regex=True)
    # products_df['ref_sku'] = products_df['ref_sku'].str.replace(' ', '')
    # Concatinating the current data with the new data
    # products_df = pd.concat([products_backup_df, products_df], axis=0)
    # Droping duplicated products by id
    # This is done as occationally api calls fail and the products drops completly from the database
    products_df.drop_duplicates(subset=['id'], keep="last", inplace=True)
    db_write(products_df, "products")
    sh_write(products_df, "PUIG", "products")
    print("products")
    """
