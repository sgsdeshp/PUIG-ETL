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


def bikes_process_endpoint(endpoint, session):
    try:
        # API request to retreive list of product details
        response_details = session.get(endpoint, headers=header)
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
        session = requests.Session()
        # API request to retreive list of bikes
        response = session.get(
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
                    bikes_process_endpoint, endpoint, session) for endpoint in endpoints]
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
            bikes_df.drop_duplicates(
                subset=['puig_final_name'], keep="first", inplace=True)
            sh_write(bikes_df, "PUIG", "bikes")
            print(bikes_df)
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_bikes() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
        raise e
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET CATEGORIES
# -------------------------------------------------------------------------------------------------------------------------------


def categories_process_endpoint(endpoint, session):
    try:
        # API request to retreive list of product details
        response_details = session.get(endpoint, headers=header)
        # response_details = requests.get(endpoint, headers=header)
        if response_details.status_code == 200:
            df = pd.DataFrame(response_details.json()['data'])
            return df
    except:
        pass


def get_categories():
    try:
        session = requests.Session()
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
                    categories_process_endpoint, endpoint, session) for endpoint in endpoints]
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

# API REQUESTS TO GET PRODUCTS
# -------------------------------------------------------------------------------------------------------------------------------
# function to be executed in parallel threads


def products_process_endpoint(endpoint, session):
    try:
        # API request to retreive list of product details
        response_details = session.get(endpoint, headers=header)
        df = pd.DataFrame(
            columns=['id', 'title', 'description', 'homologation', 'references', 'bikes'])
        if response_details.status_code == 200:
            data = json.loads(response_details.text)['data']
            df.at[0, 'id'] = str(data['id'])
            df.at[0, 'title'] = str(data['title'])
            df.at[0, 'description'] = str(data['title'])
            df.at[0, 'homologation'] = str(data['homologation'])
            df.at[0, 'references'] = str(data['references'])
            df.at[0, 'bikes'] = str(data['bikes'])
            df.at[0, 'technical'] = json.dumps(data['technical'])
            df.at[0, 'multimedia'] = json.dumps(data['multimedia'])
            return df
    except Exception as e:
        pass


def get_products():
    # Backing current data from the database
    # sql_query = 'select * from "products";'
    # products_backup_df = db_read(sql_query)
    # API request to retreive list of products
    session = requests.Session()
    response_products = requests.get(
        'https://api.puig.tv/en/products', headers=header)
    # Convert list of products into dataframe
    products_df = pd.DataFrame(response_products.json()['data'])
    db_write(products_df, "products")
    sh_write(products_df, "PUIG", "products")
    # Get a list of all the id
    ids = products_df['id'].tolist()
    # Append the ids to the api request url as a list of strings
    endpoints = ['https://api.puig.tv/en/products/' + str(id) for id in ids]
    # endpoints = ['https://api.puig.tv/en/products/1100775']
    # Create an empty DataFrame to store the results
    product_details_df = pd.DataFrame()
    # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit each API endpoint to the executor
        futures = [executor.submit(
            products_process_endpoint, endpoint, session) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            product_details_df = pd.concat(
                [product_details_df, df], axis=0)

    # product_details_df = product_details_df.replace(
    #    {"\[": '', "\]": '', "'": "", "\r": '', "\n": ''}, regex=True)
    product_details_df['references'] = product_details_df['references'].replace({
        " ": '', "\[": '', "\]": '', "'": ''}, regex=True)
    product_details_df['bikes'] = product_details_df['bikes'].replace({
        " ": '', "\[": '', "\]": '', "'": ''}, regex=True)
    print(product_details_df)
    db_write(product_details_df, "product_details")
    # sh_write(product_details_df, "PUIG", "product_details")

# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET REFERENCE VARIANTS
# -------------------------------------------------------------------------------------------------------------------------------
# function to be executed in parallel threads


def get_references():
    try:
        # API request to retreive list of references
        response = requests.get(
            'https://api.puig.tv/en/references', headers=header)
        if response.status_code == 200:
            # Convert list of references into dataframe
            references_df = pd.DataFrame(response.json()['data'])
            references_df.rename(columns={0: 'references'}, inplace=True)
            # Droping known sku with error
            references_df.drop(
                references_df[references_df.references == "5020N/G"].index, inplace=True)
            references_df.drop_duplicates(
                subset=['references'], keep="first", inplace=True)
            db_write(references_df, "references")
            sh_write(references_df, "PUIG", "references")
            return references_df
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Function get_references() failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\nERROR:{e}")
        raise e


def variants_process_endpoint(endpoint, session):
    try:
        # API request to retreive list of product details
        response_details = session.get(endpoint, headers=header)
        df = pd.DataFrame(
            columns=['reference', 'product', 'variations', 'groups', 'bikes', 'aerotest', 'comparative', 'instructions'])
        if response_details.status_code == 200:
            data = json.loads(response_details.text)['data']
            df.at[0, 'reference'] = str(data['reference'])
            df.at[0, 'product'] = str(data['product'])
            df.at[0, 'variations'] = str(data['variations'])
            df.at[0, 'groups'] = str(data['groups'])
            df.at[0, 'bikes'] = str(data['bikes'])
            df.at[0, 'aerotest'] = str(data['aerotest'])
            df.at[0, 'aerotest'] = str(data['data'])
            df.at[0, 'comparative'] = str(data['comparative'])
            df.at[0, 'instructions'] = str(data['instructions'])
            print(df)
            return df
    except:
        pass


def get_variants():
    session = requests.Session()
    ref_df = get_references()
    # Get a list of all the id
    refs = ref_df['references'].tolist()
    endpoints = ['https://api.puig.tv/en/references/' +
                 str(ref) for ref in refs]
    # Create an empty DataFrame to store the results
    variants_df = pd.DataFrame()
    # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit each API endpoint to the executor
        futures = [executor.submit(
            variants_process_endpoint, endpoint, session) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            variants_df = pd.concat([variants_df, df], axis=0)

    # variants_df['bikes'] = variants_df['bikes'].str.replace(' ', '')

    print(variants_df)
    db_write(variants_df, "ref_variants")
    # sh_write(product_details_df, "PUIG", "product_details")
