from api_functions import *
from api_data_read_write import *
import time
import asyncio
import threading


if __name__ == '__main__':
    t0 = time.time()
    # connect_to_db()
    # connect_to_serv_acc()
    connect_to_api()

    # get_bikes()
    # get_categories()
    get_products()
    """
    all_threads = []
    # Create a thread for each function
    for func in [get_categories, get_references]:
        t = threading.Thread(target=func)
        all_threads.append(t)
    # Start all the threads
    for t in all_threads:
        t.start()
    # Wait for all threads to finish
    for t in all_threads:
        t.join()
    """

    t1 = time.time()
    print(f"Execution Time: {(t1-t0)/60} minutes")
