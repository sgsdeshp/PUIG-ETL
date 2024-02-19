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
            # db_write(references_df, "references")
            # sh_write(references_df, "PUIG", "references")
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
            columns=['reference', 'product', 'variations', 'title', 'description', 'bikes', 'aerotest', 'comparative', 'instructions'])
        if response_details.status_code == 200:
            data = json.loads(response_details.text)['data']
            df.at[0, 'reference'] = str(data['code'])
            df.at[0, 'product'] = data['product']
            df.at[0, 'variations'] = data['variations']
            if data['groups'] == None:
                df.at[0, 'title'] = None
                df.at[0, 'description'] = None
            else:
                df.at[0, 'title'] = str(data['groups'][0]['title'])
                df.at[0, 'description'] = str(data['groups'][0]['description'])
            df.at[0, 'bikes'] = str(data['bikes'])
            df.at[0, 'aerotest'] = data['aerotest']
            df.at[0, 'comparative'] = data['comparative']
            df.at[0, 'instructions'] = data['instructions']
            df = df.explode('variations')
            return df
    except Exception as e:
        print(str(data['code']), str(data['groups']), type(data['groups']))
        raise e


def get_variants():
    session = requests.Session()
    ref_df = get_references()
    # Get a list of all the id
    refs = ref_df['references'].tolist()
    endpoints = ['https://api.puig.tv/en/references/' +
                 str(ref) for ref in refs]
    # endpoints = ['https://api.puig.tv/en/references/0013']
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
    variants_df['bikes'] = variants_df['bikes'].str.replace(' ', '')
    variants_df = variants_df.replace(
        {"\[": '', "\]": '', "\{": '', "\}": '', "'": "", "\r": '', "\n": ''}, regex=True)
    variants_df.insert(
        0, 'sku', variants_df['reference']+variants_df['variations'])
    # print(variants_df)
    db_write(variants_df, "variants")
    # sh_write(product_details_df, "PUIG", "product_details")
# -------------------------------------------------------------------------------------------------------------------------------

# API REQUESTS TO GET VARIANT SPECIFICATIONS
# -------------------------------------------------------------------------------------------------------------------------------
# function to be executed in parallel threads


def variantdetails_process_endpoint(endpoint, session):
    try:
        # API request to retreive list of product details
        response_details = requests.get(endpoint, headers=header)
        if response_details.status_code == 200:
            data = json.loads(response_details.text)['data']
            # print(data)
            # Creating empty dataframe
            df = pd.DataFrame(columns=['reference', 'colour', 'stock', 'stock_prevision', 'outdated', 'weight', 'height',
                              'width', 'depth', 'barcode', 'alternative', 'pvp', 'pvp_recomended', 'multimedia', 'origin', 'hs_code'])
            # Inserting appropriate data to dataframe
            df.at[0, 'reference'] = str(data['code'])
            df.at[0, 'colour'] = str(data['colour'])
            df.at[0, 'stock'] = str(data['stock'])
            df.at[0, 'stock_prevision'] = str(data['stock_prevision'])
            df.at[0, 'outdated'] = str(data['outdated'])
            df.at[0, 'weight'] = str(data['weight'])
            df.at[0, 'height'] = str(data['height'])
            df.at[0, 'width'] = str(data['width'])
            df.at[0, 'depth'] = str(data['depth'])
            df.at[0, 'barcode'] = str(data['barcode'])
            df.at[0, 'alternative'] = str(data['alternative'])
            df.at[0, 'pvp'] = str(data['pvp'])
            df.at[0, 'pvp_recomended'] = str(data['pvp_recomended'])
            df.at[0, 'multimedia'] = str(data['multimedia'])
            df.at[0, 'origin'] = str(data['origin'])
            df.at[0, 'hs_code'] = str(data['hs_code'])
            # df.at[0, 'images'] = str(data['multimedia']['images'])
            # df.at[0, 'videos'] = str(data['multimedia']['videos'])
            # df.at[0, 'onbike'] = str(data['multimedia']['onbike'])[1:-1]
            return df
    except Exception as e:
        """
        if response_details.status_code != 200:
            print(
                f"The API returned an error code: {response_details.status_code}.\nReconnecting to API...")
            connect_to_api()
            print("Reconnected to API.")
            return variantdetails_process_endpoint(endpoint, session)
        else:"""
        # print(str(data['code']))
        print(endpoint, e)
        pass


def get_variant_details():
    session = requests.Session()
    # Backing current data from the database
    # sql_query = 'select * from "variants";'
    # variantspecs_backup_df = db_read(sql_query)

    sql_query = "select sku from variants;"
    variants_df = db_read(sql_query)

    refs = variants_df['sku'].tolist()
    endpoints = ['https://api.puig.tv/en/references/' +
                 str(ref[:-1]) + '/' + str(ref[-1]) for ref in refs]
    # endpoints = ['https://api.puig.tv/en/references/3755/N', 'https://api.puig.tv/en/references/3755/H', 'https://api.puig.tv/en/references/3755/W']
    # Create an empty DataFrame to store the results
    variant_details_df = pd.DataFrame()
    # Use a ThreadPoolExecutor to execute the process_endpoint function in parallel threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit each API endpoint to the executor
        futures = [executor.submit(
            variantdetails_process_endpoint, endpoint, session) for endpoint in endpoints]
        # Iterate over each completed future and append the result to the products_df DataFrame
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            variant_details_df = pd.concat([variant_details_df, df], axis=0)
    # Inserting sku column, combining ref sku & colour
    variant_details_df.insert(
        0, 'sku', variant_details_df['reference']+variant_details_df['colour'])
    # Converting datatype of column (string to float)
    variant_details_df["pvp_recomended"] = variant_details_df["pvp_recomended"].astype(
        float)
    # Converting datatype of column (string to float)
    variant_details_df["pvp"] = variant_details_df["pvp"].astype(
        float)
    # Calculating Cost
    variant_details_df["cost"] = variant_details_df['pvp']*0.495
    # Calculating RRP
    variant_details_df['rrp'] = round(
        variant_details_df['pvp']*1.21*0.86)-0.01
    # print(variant_details_df)
    # variant_details_df['onbike'] = variant_details_df['onbike'].to_json()
    # variant_details_df['onbike'] = json.dumps(variant_details_df['onbike'])
    variant_details_df = variant_details_df.replace(
        {"'": '"', "None": "null"}, regex=True)
    print("variant_details_complete")
    connect_to_db()
    db_write(variant_details_df, "variant_details")
    variant_details_df.info(memory_usage="deep")
    """variantspecs_df = variantspecs_df.fillna('0')
    variantspecs_df = variantspecs_df.replace(
        {"\[": '', "\]": '', "'": "", "\r": '', "\n": ''}, regex=True)
    variantspecs_df['ref_sku'] = variantspecs_df.sku.str[:-1]
    variantspecs_df["pvp"] = variantspecs_df["pvp"].astype(float)
    variantspecs_df['rrp'] = round(variantspecs_df['pvp']*1.21*0.88)-0.01
    # Concatinating the current data with the new data
    variantspecs_df = pd.concat(
        [variantspecs_backup_df, variantspecs_df], axis=0)
    # Droping duplicated products by id
    # This is done as occationally api calls fail and the products drops completly from the database
    variantspecs_df.drop_duplicates(subset=['sku'], keep="last", inplace=True)
    print("variantspecs")
    print("finished")"""
# ALTER TABLE variant_details
# ALTER COLUMN multimedia TYPE JSONB USING multimedia::jsonb;
