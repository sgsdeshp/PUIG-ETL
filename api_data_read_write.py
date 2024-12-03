from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import pandas as pd
import requests
import urllib
import gspread
import shutil
import json
import ssl
import os
import concurrent.futures
from notification import send_email

pool = None
sa = None
drive_service = None


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
        # pool.autocommit = True
    except Exception as e:
        send_email("PUIG API script failed.",
                   f"Authentication to database failed.\nNo immediate action necessary.\nThe script will auto-retry after a delay.\nDo ensure the script has executed successfully after a while.\n{e}")
        raise e


def connect_to_serv_acc():
    """Connect to the google service account and Google drive API."""
    global sa
    global drive_service
    service_acc = os.environ.get('PMGCPKEY')
    # Authenticate and build the Google Sheets API client using a service account
    sa = gspread.service_account(filename=service_acc)
    # *****IMPORTANT*****
    # uncomment the line below and comment the line above before cloud deployment
    # sa = gspread.service_account_from_dict(json.loads(os.environ.get('PMGCPKEY')))

    # Authenticate and build the Google Drive API client using a service account
    credentials = service_account.Credentials.from_service_account_file(
        service_acc)
    # *****IMPORTANT*****
    # uncomment the line below and comment the line above before cloud deployment
    # credentials = service_account.Credentials.from_service_account_file(json.loads(os.environ.get('PMGCPKEY')))
    drive_service = build('drive', 'v3', credentials=credentials)
# -------------------------------------------------------------------------------------------------------------------------------

# GSHEETS READ & WRITE
# -------------------------------------------------------------------------------------------------------------------------------


def sh_read(wbook, wsheet):
    """reading data

    Args:
        wbook (string): gsheet workbook to read from
        wsheet (string): gsheet sheet to read from



    Returns:
        dataframe: data read from gsheet

    """
    # select workbook
    sh = sa.open(wbook)
    # select worksheet
    wks = sh.worksheet(wsheet)
    # get all records as pandas data frame
    df = pd.DataFrame(wks.get_all_records())
    return df


def sh_write(df, wbook, wsheet, x=True):
    """writing to gsheet

    Args:
        df (dataframe): dataframe data to write
        wbook (string): gsheet workbook to write to
        wsheet (string): gsheet sheet to write to
        x (boolean): optional argument. Set to False if script writes formulas to sheets
    """
    # establish connection
    sh = sa.open(wbook)
    wks = sh.worksheet(wsheet)
    wks.clear()
    # writing to sheet
    wks.update([df.columns.values.tolist()] + df.values.tolist(), raw=x)


def set_formula(wbook, wsheet, cell, formula):
    """writing formula to gsheets

    Args:
        wbook (string): gsheet workbook to write to
        wsheet (string): gsheet sheet to write to
        cell (string): gsheet cell to write to
        formula (string): formula to write
    """
    # establish connection
    sh = sa.open(wbook)
    wks = sh.worksheet(wsheet)
    # writing to sheet
    wks.update(cell, formula, raw=False)
# -------------------------------------------------------------------------------------------------------------------------------

# DATABASE READ AND WRITE
# -------------------------------------------------------------------------------------------------------------------------------


def db_write(df, table):
    """writing to database table

    Args:
        df (dataframe): dataframe data to write
        table (string): table name to write to
    """
    # writing data to db
    with pool.connect() as connection:
        try:
            df.to_sql(table, con=connection, if_exists='replace', index=False)
        finally:
            connection.close()


def db_read(sql_query):
    """read data from database

    Args:
        sql_query (string): sql query

    Returns:
        dataframe: output of the query
    """
    # fetching data from db
    with pool.connect() as connection:
        df = pd.read_sql(sql=sql_query, con=connection)
        connection.close()
    return df


def db_query(sql_query):
    """execute query on database

    Args:
        sql_query (string): query to exeute
    """
    # executing queries on database
    pool.execute(sql_query)
# -------------------------------------------------------------------------------------------------------------------------------

# FTP:picbox READ
# -------------------------------------------------------------------------------------------------------------------------------


def download_file(url, filename):
    """download files

    Args:
        url (string): url of the file to be downloaded
        filename (string): file name to be downloaded
    """
    # Disable certificate verification
    context = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=context) as response:
        with open(filename, 'wb') as f:
            shutil.copyfileobj(response, f)


def ftp_read(url, filename):
    """download files

    Args:
        url (_type_): (string): url of the file to be downloaded
        filename (string): file name to be downloaded

    Returns:
        dataframe: data from downloaded file
    """
    # read files from ftp
    download_file(url, filename)
    df = pd.read_excel(filename)
    df = df.fillna('')
    return df
# -------------------------------------------------------------------------------------------------------------------------------

# READ GOOGLE DRIVE FILES
# -------------------------------------------------------------------------------------------------------------------------------


def read_gdrive(folder_id, file_type):

    # Initialize variables for pagination
    page_token = None
    items = []

    # Retrieve all the files in the folder using pagination and search parameters
    while True:
        query = f"trashed = false and parents in '{folder_id}' and mimeType='{file_type}'"
        results = drive_service.files().list(q=query, fields="nextPageToken, files(id, name, webViewLink)",
                                             pageToken=page_token, pageSize=1000).execute()
        items += results.get('files', [])
        page_token = results.get('nextPageToken', None)
        if not page_token:
            break

    # Create a pandas DataFrame with the names and URLs of all the files
    if not items:
        print('No files found in the specified folder.')
    else:
        file_names = [item['name'] for item in items]
        file_ids = [item['id'] for item in items]
        file_urls = [
            f'https://drive.google.com/uc?id={item["id"]}' for item in items]
        df = pd.DataFrame(
            {'Name': file_names, 'ID': file_ids, 'URL': file_urls})
        df.insert(0, 'SKU', '')
        if file_type == 'image/jpeg':
            df['SKU'] = df['Name'].str.split('-').str.get(0)
        elif file_type == 'application/pdf':
            df['SKU'] = df['Name'].apply(lambda x: x[:-4])
        return df
